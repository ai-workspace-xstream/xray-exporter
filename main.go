// Xray Exporter - Unified data-plane and telemetry runtime.
// Supports both embedded Xray-core with zero-gRPC in-memory stats collection
// and legacy gRPC polling against an external Xray instance, plus optional
// access log parsing and billing snapshot delivery to Vector.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/xtls/xray-core/common/cmdarg"
	"github.com/xtls/xray-core/core"

	"xray-exporter/internal/geoip"
	"xray-exporter/internal/snapshot"
	"xray-exporter/internal/xrayembed"
)

// Build information injected during compilation.
var (
	buildVersion = "dev"
	buildCommit  = "none"
	buildDate    = "unknown"
)

type cliOptions struct {
	Listen                 string `short:"l" long:"listen" description:"Listen address (empty or 'disabled' to disable HTTP)" value-name:"[ADDR]:PORT" default:":9550"`
	MetricsPath            string `short:"m" long:"metrics-path" description:"Metrics path" value-name:"PATH" default:"/scrape"`
	XRayEndpoint           string `short:"e" long:"xray-endpoint" description:"Xray API endpoint (legacy gRPC mode)" value-name:"HOST:PORT" default:"127.0.0.1:8080"`
	XRayConfig             string `short:"c" long:"xray-config" description:"Path to Xray config file for embedded in-process mode (or XRAY_CONFIG_PATH)" value-name:"PATH"`
	ScrapeTimeoutInSeconds int64  `short:"t" long:"scrape-timeout" description:"The timeout in seconds for every individual scrape" value-name:"N" default:"5"`
	UserTrafficMetrics     bool   `short:"u" long:"user-traffic-metrics" description:"Export per-user traffic byte counters (high cardinality: one series per user)"`
	LogPath                string `short:"p" long:"log-path" description:"Path to Xray access log file (empty or 'disabled' to disable user metrics)" value-name:"PATH" default:"/var/log/xray/access.log"`
	LogTimeWindowMinutes   int    `short:"w" long:"log-time-window" description:"Time window in minutes for user metrics" value-name:"N"`
	GeoIPDir               string `short:"g" long:"geoip-dir" description:"Directory for GeoLite2 databases" value-name:"PATH" default:"."`
	DisableGeoIPDownload   bool   `long:"disable-geoip-download" description:"Skip GeoLite2 database download on startup"`
	Version                bool   `long:"version" description:"Display the version and exit"`
	LogLevel               string `long:"log-level" description:"Log level: error, warn, info, debug (env: LOG_LEVEL) (default: warn)" value-name:"LEVEL"`
	Instance               string `long:"instance" description:"Optional instance label for logs (or XRAY_EXPORTER_INSTANCE)"`
	NodeID                 string `long:"node-id" description:"Billing node identifier (or EXPORTER_NODE_ID)"`
	Environment            string `long:"environment" description:"Billing environment (or EXPORTER_ENV)"`
	AccountsBaseURL        string `long:"accounts-base-url" description:"Accounts base URL (or ACCOUNTS_BASE_URL)"`
	InternalServiceToken   string `long:"internal-service-token" description:"Snapshot API token (or INTERNAL_SERVICE_TOKEN)"`
	SnapshotStorePath      string `long:"snapshot-store-path" description:"Snapshot JSON store (or SNAPSHOT_STORE_PATH)"`
	SnapshotRetention      string `long:"snapshot-retention" description:"Snapshot retention (or SNAPSHOT_RETENTION)"`
	SnapshotInterval       string `long:"snapshot-interval" description:"Snapshot collection interval (or SNAPSHOT_INTERVAL)"`
	VectorSnapshotURL      string `long:"vector-snapshot-url" description:"Vector snapshot input URL (or VECTOR_SNAPSHOT_URL)"`
}

type cliAction int

const (
	cliActionRun cliAction = iota
	cliActionVersion
	cliActionHelp
	cliActionRunTest
	cliActionDump
)

type runtimeConfig struct {
	Action               cliAction
	RunSubcommand        bool
	Embedded             bool
	XRayConfigPath       string
	XRayConfigFiles      []string
	XRayConfigDir        string
	XRayConfigFormat     string
	XRayEndpoint         string
	ExplicitXRayEndpoint bool
	Listen               string
	MetricsPath          string
	ScrapeTimeout        time.Duration
	UserTrafficMetrics   bool
	LogPath              string
	LogTimeWindow        time.Duration
	GeoIPDir             string
	DisableGeoIPDownload bool
	LogLevel             string
	Instance             string
	NodeID               string
	Environment          string
	AccountsBaseURL      string
	InternalServiceToken string
	SnapshotStorePath    string
	SnapshotRetention    time.Duration
	SnapshotInterval     time.Duration
	VectorSnapshotURL    string
}

type stringListFlag []string

func (s *stringListFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringListFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func exporterBanner() string {
	return fmt.Sprintf("Xray Exporter %v-%v (built %v)", buildVersion, buildCommit, buildDate)
}

func printVersions(w io.Writer) {
	for _, line := range xrayembed.VersionStatement() {
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w, exporterBanner())
}

// scrapeHandler creates an HTTP handler for the Prometheus scrape endpoint.
func scrapeHandler(exporter *Exporter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		promhttp.HandlerFor(
			exporter.registry, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError},
		).ServeHTTP(w, r)
	}
}

// healthHandler is a simple HTTP health check endpoint.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// configureLogging sets the global logrus level. Precedence: explicit flag,
// then XRAY_EXPORTER_LOG_LEVEL / LOG_LEVEL env var, then "warn".
func configureLogging(flagLevel string, getenv func(string) (string, bool)) {
	levelStr := strings.TrimSpace(flagLevel)
	if levelStr == "" {
		levelStr = firstLookupEnv(getenv, "XRAY_EXPORTER_LOG_LEVEL", "LOG_LEVEL")
	}
	if levelStr == "" {
		levelStr = "warn"
	}

	var level logrus.Level
	switch strings.ToLower(levelStr) {
	case "error":
		level = logrus.ErrorLevel
	case "warn":
		level = logrus.WarnLevel
	case "info":
		level = logrus.InfoLevel
	case "debug":
		level = logrus.DebugLevel
	default:
		logrus.Warnf("invalid log level %q, defaulting to warn", levelStr)
		level = logrus.WarnLevel
	}
	logrus.SetLevel(level)
}

func firstLookupEnv(getenv func(string) (string, bool), keys ...string) string {
	for _, key := range keys {
		if val, ok := getenv(key); ok {
			return strings.TrimSpace(val)
		}
	}
	return ""
}

func lookupAnyEnv(getenv func(string) (string, bool), keys ...string) (string, bool) {
	for _, key := range keys {
		if val, ok := getenv(key); ok {
			return strings.TrimSpace(val), true
		}
	}
	return "", false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseDuration(value string, fallback time.Duration) time.Duration {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		logrus.WithError(err).Warnf("invalid duration %q; using %s", value, fallback)
		return fallback
	}
	return parsed
}

func parseBoolEnv(getenv func(string) (string, bool), keys ...string) (bool, bool) {
	raw, ok := lookupAnyEnv(getenv, keys...)
	if !ok || raw == "" {
		return false, false
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		logrus.Warnf("invalid boolean env value %q; ignoring", raw)
		return false, false
	}
	return parsed, true
}

func isDisabledValue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "disabled", "off", "none":
		return true
	default:
		return false
	}
}

func normalizeListenAddr(v string) string {
	if isDisabledValue(v) {
		return ""
	}
	return strings.TrimSpace(v)
}

func parseCLI(args []string, getenv func(string) (string, bool), stdout, stderr io.Writer) (*runtimeConfig, error) {
	if len(args) == 0 {
		return nil, errors.New("missing executable argument")
	}

	if len(args) >= 2 {
		switch args[1] {
		case "version", "-version", "--version":
			return &runtimeConfig{Action: cliActionVersion}, nil
		case "help":
			printGeneralHelp(stdout)
			return &runtimeConfig{Action: cliActionHelp}, nil
		case "run":
			return parseRunSubcommand(args[2:], getenv, stderr)
		}
	}

	return parseExporterFlags(args[1:], getenv, stdout)
}

func printGeneralHelp(w io.Writer) {
	fmt.Fprintln(w, "Xray Exporter (unified Xray-core + telemetry binary)")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  xray-exporter [OPTIONS]")
	fmt.Fprintln(w, "  xray-exporter --xray-config <config.json> [OPTIONS]")
	fmt.Fprintln(w, "  xray-exporter run [-test] -c <config.json>")
	fmt.Fprintln(w, "  xray-exporter version")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Run 'xray-exporter --help' for all exporter flags or 'xray-exporter run -h' for Xray run flags.")
}

func parseRunSubcommand(args []string, getenv func(string) (string, bool), stderr io.Writer) (*runtimeConfig, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var configFiles stringListFlag
	fs.Var(&configFiles, "config", "Config path for Xray.")
	fs.Var(&configFiles, "c", "Short alias of -config.")
	configDir := fs.String("confdir", "", "A dir with multiple json configs.")
	format := fs.String("format", "auto", "Format of input file.")
	testOnly := fs.Bool("test", false, "Test config file only, without launching server.")
	dumpOnly := fs.Bool("dump", false, "Dump merged config only, without launching server.")

	// Optional exporter flags in `run` mode (env vars apply when omitted).
	listen := fs.String("listen", "", "Metrics/snapshot listen address (disabled by default in run mode unless set)")
	metricsPath := fs.String("metrics-path", "", "Prometheus metrics path")
	scrapeTimeoutSec := fs.Int64("scrape-timeout", 0, "Scrape timeout in seconds")
	userTrafficFlag := fs.Bool("user-traffic-metrics", false, "Export per-user traffic byte counters")
	logPath := fs.String("log-path", "", "Access log path (disabled by default in run mode unless set)")
	logWindowMin := fs.Int("log-time-window", 0, "Log time window in minutes")
	geoipDir := fs.String("geoip-dir", "", "GeoIP database directory")
	disableGeoIP := fs.Bool("disable-geoip-download", false, "Disable GeoLite2 download")
	logLevel := fs.String("log-level", "", "Log level: error, warn, info, debug")
	instance := fs.String("instance", "", "Instance label")
	nodeID := fs.String("node-id", "", "Billing node identifier")
	environment := fs.String("environment", "", "Billing environment")
	accountsURL := fs.String("accounts-base-url", "", "Accounts base URL")
	serviceToken := fs.String("internal-service-token", "", "Snapshot API token")
	snapshotStorePath := fs.String("snapshot-store-path", "", "Snapshot JSON store path")
	snapshotRetention := fs.String("snapshot-retention", "", "Snapshot retention duration")
	snapshotInterval := fs.String("snapshot-interval", "", "Snapshot collection interval")
	vectorURL := fs.String("vector-snapshot-url", "", "Vector snapshot input URL")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return &runtimeConfig{Action: cliActionHelp, RunSubcommand: true}, nil
		}
		return nil, err
	}

	explicitFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		explicitFlags[f.Name] = true
	})

	if len(configFiles) == 0 && strings.TrimSpace(*configDir) == "" {
		if envCfg := firstLookupEnv(getenv, "XRAY_CONFIG_PATH", "XRAY_EXPORTER_CONFIG_PATH", "XRAY_LOCATION_CONFIG"); envCfg != "" {
			configFiles = append(configFiles, envCfg)
		} else if envDir := firstLookupEnv(getenv, "XRAY_LOCATION_CONFDIR"); envDir != "" {
			*configDir = envDir
		} else if wd, err := os.Getwd(); err == nil {
			for _, suffix := range []string{".json", ".jsonc", ".toml", ".yaml", ".yml"} {
				candidate := filepath.Join(wd, "config"+suffix)
				if info, statErr := os.Stat(candidate); statErr == nil && !info.IsDir() {
					configFiles = append(configFiles, candidate)
					break
				}
			}
		}
	}

	var singleConfigPath string
	if len(configFiles) == 1 && strings.TrimSpace(*configDir) == "" {
		singleConfigPath = configFiles[0]
	}

	cfg := &runtimeConfig{
		Action:           cliActionRun,
		RunSubcommand:    true,
		Embedded:         true,
		XRayConfigPath:   singleConfigPath,
		XRayConfigFiles:  []string(configFiles),
		XRayConfigDir:    strings.TrimSpace(*configDir),
		XRayConfigFormat: strings.TrimSpace(*format),
		LogLevel:         *logLevel,
	}

	if *dumpOnly {
		cfg.Action = cliActionDump
		return cfg, nil
	}
	if *testOnly {
		cfg.Action = cliActionRunTest
		return cfg, nil
	}

	// In `run` mode, HTTP listener and access-log GeoIP parsing are opt-in so
	// multiple independent `xray run -config ...` services (gateway, xhttp, tcp)
	// never collide on :9550 or touch snapshots/GeoIP unless explicitly enabled.
	if explicitFlags["listen"] {
		cfg.Listen = normalizeListenAddr(*listen)
	} else if envListen, ok := lookupAnyEnv(getenv, "XRAY_EXPORTER_LISTEN", "EXPORTER_LISTEN"); ok {
		cfg.Listen = normalizeListenAddr(envListen)
	}

	cfg.MetricsPath = firstNonEmpty(*metricsPath, firstLookupEnv(getenv, "XRAY_EXPORTER_METRICS_PATH", "EXPORTER_METRICS_PATH"), "/scrape")

	scrapeSec := *scrapeTimeoutSec
	if !explicitFlags["scrape-timeout"] {
		if raw := firstLookupEnv(getenv, "XRAY_EXPORTER_SCRAPE_TIMEOUT", "EXPORTER_SCRAPE_TIMEOUT"); raw != "" {
			if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
				scrapeSec = parsed
			}
		}
	}
	if scrapeSec <= 0 {
		scrapeSec = 5
	}
	cfg.ScrapeTimeout = time.Duration(scrapeSec) * time.Second

	if explicitFlags["log-path"] {
		cfg.LogPath = strings.TrimSpace(*logPath)
	} else if envLogPath, ok := lookupAnyEnv(getenv, "XRAY_EXPORTER_LOG_PATH", "EXPORTER_LOG_PATH"); ok {
		cfg.LogPath = envLogPath
	}

	winMin := *logWindowMin
	if !explicitFlags["log-time-window"] {
		if raw := firstLookupEnv(getenv, "XRAY_EXPORTER_LOG_TIME_WINDOW", "EXPORTER_LOG_TIME_WINDOW"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				winMin = parsed
			}
		}
	}
	if winMin <= 0 {
		winMin = DefaultLogTimeWindowMinutes
	}
	cfg.LogTimeWindow = time.Duration(winMin) * time.Minute

	cfg.GeoIPDir = firstNonEmpty(*geoipDir, firstLookupEnv(getenv, "XRAY_EXPORTER_GEOIP_DIR", "EXPORTER_GEOIP_DIR"), ".")
	if explicitFlags["disable-geoip-download"] {
		cfg.DisableGeoIPDownload = *disableGeoIP
	} else if v, ok := parseBoolEnv(getenv, "XRAY_EXPORTER_DISABLE_GEOIP_DOWNLOAD", "EXPORTER_DISABLE_GEOIP_DOWNLOAD"); ok {
		cfg.DisableGeoIPDownload = v
	}

	populateSnapshotAndIdentityConfig(cfg, *instance, *nodeID, *environment, *accountsURL, *serviceToken, *snapshotStorePath, *snapshotRetention, *snapshotInterval, *vectorURL, getenv)

	if explicitFlags["user-traffic-metrics"] {
		cfg.UserTrafficMetrics = *userTrafficFlag
	} else if v, ok := parseBoolEnv(getenv, "XRAY_EXPORTER_USER_TRAFFIC_METRICS", "EXPORTER_USER_TRAFFIC_METRICS"); ok {
		cfg.UserTrafficMetrics = v
	} else if cfg.snapshotsEnabled() {
		cfg.UserTrafficMetrics = true
	}

	return cfg, nil
}

func parseExporterFlags(args []string, getenv func(string) (string, bool), stdout io.Writer) (*runtimeConfig, error) {
	var opts cliOptions
	parser := flags.NewParser(&opts, flags.Default)
	parser.Usage = "[OPTIONS]"
	if _, err := parser.ParseArgs(args); err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type == flags.ErrHelp {
			return &runtimeConfig{Action: cliActionHelp}, nil
		}
		return nil, err
	}

	if opts.Version {
		return &runtimeConfig{Action: cliActionVersion}, nil
	}

	isSet := func(longName string) bool {
		opt := parser.FindOptionByLongName(longName)
		return opt != nil && opt.IsSet()
	}

	configPath := strings.TrimSpace(opts.XRayConfig)
	if !isSet("xray-config") {
		configPath = firstLookupEnv(getenv, "XRAY_CONFIG_PATH", "XRAY_EXPORTER_CONFIG_PATH")
	}

	listen := opts.Listen
	if isSet("listen") {
		listen = normalizeListenAddr(opts.Listen)
	} else if envListen, ok := lookupAnyEnv(getenv, "XRAY_EXPORTER_LISTEN", "EXPORTER_LISTEN"); ok {
		listen = normalizeListenAddr(envListen)
	} else {
		listen = normalizeListenAddr(opts.Listen)
	}

	metricsPath := opts.MetricsPath
	if !isSet("metrics-path") {
		metricsPath = firstNonEmpty(firstLookupEnv(getenv, "XRAY_EXPORTER_METRICS_PATH", "EXPORTER_METRICS_PATH"), opts.MetricsPath)
	}

	xrayEndpoint := opts.XRayEndpoint
	if !isSet("xray-endpoint") {
		xrayEndpoint = firstNonEmpty(firstLookupEnv(getenv, "XRAY_EXPORTER_XRAY_ENDPOINT", "EXPORTER_XRAY_ENDPOINT"), opts.XRayEndpoint)
	}

	scrapeSec := opts.ScrapeTimeoutInSeconds
	if !isSet("scrape-timeout") {
		if raw := firstLookupEnv(getenv, "XRAY_EXPORTER_SCRAPE_TIMEOUT", "EXPORTER_SCRAPE_TIMEOUT"); raw != "" {
			if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
				scrapeSec = parsed
			}
		}
	}
	if scrapeSec <= 0 {
		scrapeSec = 5
	}

	logPath := opts.LogPath
	if !isSet("log-path") {
		if envLogPath, ok := lookupAnyEnv(getenv, "XRAY_EXPORTER_LOG_PATH", "EXPORTER_LOG_PATH"); ok {
			logPath = envLogPath
		}
	}

	winMin := opts.LogTimeWindowMinutes
	if !isSet("log-time-window") {
		if raw := firstLookupEnv(getenv, "XRAY_EXPORTER_LOG_TIME_WINDOW", "EXPORTER_LOG_TIME_WINDOW"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				winMin = parsed
			}
		}
	}
	if winMin <= 0 {
		winMin = DefaultLogTimeWindowMinutes
	}

	geoipDir := opts.GeoIPDir
	if !isSet("geoip-dir") {
		geoipDir = firstNonEmpty(firstLookupEnv(getenv, "XRAY_EXPORTER_GEOIP_DIR", "EXPORTER_GEOIP_DIR"), opts.GeoIPDir)
	}

	disableGeoIP := opts.DisableGeoIPDownload
	if !isSet("disable-geoip-download") {
		if v, ok := parseBoolEnv(getenv, "XRAY_EXPORTER_DISABLE_GEOIP_DOWNLOAD", "EXPORTER_DISABLE_GEOIP_DOWNLOAD"); ok {
			disableGeoIP = v
		}
	}

	cfg := &runtimeConfig{
		Action:               cliActionRun,
		Embedded:             configPath != "",
		XRayConfigPath:       configPath,
		XRayEndpoint:         xrayEndpoint,
		ExplicitXRayEndpoint: isSet("xray-endpoint"),
		Listen:               listen,
		MetricsPath:          metricsPath,
		ScrapeTimeout:        time.Duration(scrapeSec) * time.Second,
		LogPath:              strings.TrimSpace(logPath),
		LogTimeWindow:        time.Duration(winMin) * time.Minute,
		GeoIPDir:             geoipDir,
		DisableGeoIPDownload: disableGeoIP,
		LogLevel:             opts.LogLevel,
	}
	if cfg.XRayConfigPath != "" {
		cfg.XRayConfigFiles = []string{cfg.XRayConfigPath}
	}

	populateSnapshotAndIdentityConfig(
		cfg,
		opts.Instance,
		opts.NodeID,
		opts.Environment,
		opts.AccountsBaseURL,
		opts.InternalServiceToken,
		opts.SnapshotStorePath,
		opts.SnapshotRetention,
		opts.SnapshotInterval,
		opts.VectorSnapshotURL,
		getenv,
	)

	if isSet("user-traffic-metrics") {
		cfg.UserTrafficMetrics = opts.UserTrafficMetrics
	} else if v, ok := parseBoolEnv(getenv, "XRAY_EXPORTER_USER_TRAFFIC_METRICS", "EXPORTER_USER_TRAFFIC_METRICS"); ok {
		cfg.UserTrafficMetrics = v
	} else if cfg.snapshotsEnabled() {
		cfg.UserTrafficMetrics = true
	}

	_ = stdout
	return cfg, nil
}

func populateSnapshotAndIdentityConfig(
	cfg *runtimeConfig,
	instanceFlag, nodeIDFlag, envFlag, accountsURLFlag, tokenFlag, storePathFlag, retentionFlag, intervalFlag, vectorURLFlag string,
	getenv func(string) (string, bool),
) {
	cfg.Instance = firstNonEmpty(instanceFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_INSTANCE", "EXPORTER_INSTANCE"))
	cfg.NodeID = firstNonEmpty(nodeIDFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_NODE_ID", "EXPORTER_NODE_ID"))
	cfg.Environment = firstNonEmpty(envFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_ENV", "EXPORTER_ENV"), "uat")
	cfg.AccountsBaseURL = firstNonEmpty(accountsURLFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_ACCOUNTS_BASE_URL", "ACCOUNTS_BASE_URL"))
	cfg.InternalServiceToken = firstNonEmpty(tokenFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_INTERNAL_SERVICE_TOKEN", "INTERNAL_SERVICE_TOKEN"))
	cfg.SnapshotStorePath = firstNonEmpty(storePathFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_SNAPSHOT_STORE_PATH", "SNAPSHOT_STORE_PATH"), "/var/lib/xray-exporter/snapshots.json")
	cfg.SnapshotRetention = parseDuration(firstNonEmpty(retentionFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_SNAPSHOT_RETENTION", "SNAPSHOT_RETENTION")), 72*time.Hour)
	cfg.SnapshotInterval = parseDuration(firstNonEmpty(intervalFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_SNAPSHOT_INTERVAL", "SNAPSHOT_INTERVAL")), time.Minute)
	cfg.VectorSnapshotURL = firstNonEmpty(vectorURLFlag, firstLookupEnv(getenv, "XRAY_EXPORTER_VECTOR_SNAPSHOT_URL", "VECTOR_SNAPSHOT_URL"))
}

func (c *runtimeConfig) snapshotsEnabled() bool {
	return c.NodeID != "" && c.InternalServiceToken != "" && c.AccountsBaseURL != ""
}

func main() {
	os.Exit(runMain(os.Args, os.LookupEnv, os.Stdout, os.Stderr))
}

func runMain(args []string, getenv func(string) (string, bool), stdout, stderr io.Writer) int {
	cfg, err := parseCLI(args, getenv, stdout, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "Failed to parse arguments: %v\n", err)
		return 2
	}

	switch cfg.Action {
	case cliActionHelp:
		return 0
	case cliActionVersion:
		printVersions(stdout)
		return 0
	case cliActionDump:
		files := cmdarg.Arg(cfg.XRayConfigFiles)
		merged, err := core.GetMergedConfig(files)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 23
		}
		fmt.Fprint(stdout, merged)
		return 0
	case cliActionRunTest:
		printVersions(stdout)
		if err := xrayembed.ValidateOptions(xrayembed.Options{
			ConfigPath:  cfg.XRayConfigPath,
			ConfigFiles: cfg.XRayConfigFiles,
			ConfigDir:   cfg.XRayConfigDir,
			Format:      cfg.XRayConfigFormat,
		}); err != nil {
			fmt.Fprintln(stdout, "Failed to start:", err)
			return 23
		}
		fmt.Fprintln(stdout, "Configuration OK.")
		return 0
	}

	configureLogging(cfg.LogLevel, getenv)

	// Print identity banner directly so it is never hidden by log level.
	fmt.Fprintln(stdout, exporterBanner())

	stopTelemetry := xrayembed.InitTelemetry()
	defer stopTelemetry()

	var (
		engine      *xrayembed.Engine
		statsReader StatsReader
	)
	if cfg.Embedded {
		if cfg.ExplicitXRayEndpoint {
			logrus.Warn("Embedded --xray-config mode active; ignoring --xray-endpoint and using in-memory stats.Manager")
		}
		engine, err = xrayembed.NewEngineWithOptions(xrayembed.Options{
			ConfigPath:  cfg.XRayConfigPath,
			ConfigFiles: cfg.XRayConfigFiles,
			ConfigDir:   cfg.XRayConfigDir,
			Format:      cfg.XRayConfigFormat,
		})
		if err != nil {
			fmt.Fprintf(stderr, "Failed to initialize embedded Xray config: %v\n", err)
			return 23
		}
		if err := engine.Start(); err != nil {
			_ = engine.Close()
			fmt.Fprintf(stderr, "Failed to start embedded Xray: %v\n", err)
			if cfg.RunSubcommand {
				return 23
			}
			return 1
		}
		if !engine.HasStatsManager() && (cfg.Listen != "" || cfg.snapshotsEnabled()) {
			logrus.Warn("Embedded Xray config does not enable stats ({}); traffic metrics and billing snapshots will be empty")
		}
		logrus.WithFields(logrus.Fields{
			"xray_version": xrayembed.Version(),
			"config":       firstNonEmpty(cfg.XRayConfigPath, strings.Join(cfg.XRayConfigFiles, ",")),
			"instance":     cfg.Instance,
		}).Info("Embedded Xray-core started with in-memory telemetry")
		statsReader = newInprocStatsReader(engine, true)
	} else {
		statsReader, err = newGRPCStatsReader(cfg.XRayEndpoint)
		if err != nil {
			logrus.WithError(err).Error("Failed to initialize Xray gRPC stats reader")
			return 1
		}
	}

	// Configure GeoIP directory unconditionally when access-log parsing is enabled,
	// even if automatic GeoIP download is disabled (using pre-staged .mmdb files).
	if cfg.LogPath != "" && cfg.LogPath != "disabled" {
		geoip.Dir = cfg.GeoIPDir
		if !cfg.DisableGeoIPDownload {
			shouldDownloadGeoIP := !cfg.Embedded
			if cfg.Embedded {
				if _, statErr := os.Stat(cfg.LogPath); statErr == nil {
					shouldDownloadGeoIP = true
				}
			}
			if shouldDownloadGeoIP {
				if err := geoip.DownloadDB(); err != nil {
					logrus.WithError(err).Warn("Failed to initialize GeoIP database, GeoIP metrics will be unavailable")
				}
			}
		}
	}

	exporter, err := NewExporterWithStatsReader(statsReader, cfg.ScrapeTimeout, cfg.UserTrafficMetrics, cfg.LogPath, cfg.LogTimeWindow)
	if err != nil {
		_ = statsReader.Close()
		logrus.WithError(err).Error("Failed to create exporter")
		return 1
	}
	defer exporter.Close()

	var (
		snapshotStore   *snapshot.Store
		snapshotService *snapshot.Service
		snapshotPusher  *snapshot.Pusher
		stopSnapshots   func(flush bool)
	)
	if cfg.snapshotsEnabled() {
		snapshotStore, err = snapshot.NewStore(cfg.SnapshotStorePath, cfg.SnapshotRetention)
		if err != nil {
			logrus.WithError(err).Error("Failed to create snapshot store")
			return 1
		}
		snapshotService = snapshot.NewService(
			cfg.NodeID,
			cfg.Environment,
			exporter,
			snapshot.NewAccountsClient(cfg.AccountsBaseURL, cfg.InternalServiceToken),
			snapshotStore,
			cfg.ScrapeTimeout,
		)
		if cfg.VectorSnapshotURL != "" {
			snapshotPusher = snapshot.NewPusher(cfg.VectorSnapshotURL, cfg.InternalServiceToken, cfg.ScrapeTimeout)
		}
		stopSnapshots = startSnapshotLoop(snapshotService, cfg.SnapshotInterval, snapshotPusher, cfg.ScrapeTimeout)
	} else if !cfg.RunSubcommand || cfg.Listen != "" || cfg.VectorSnapshotURL != "" {
		logrus.Warn("Billing snapshots disabled: EXPORTER_NODE_ID, ACCOUNTS_BASE_URL and INTERNAL_SERVICE_TOKEN are required")
	}

	var (
		server    *http.Server
		serverErr = make(chan error, 1)
	)
	if cfg.Listen != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc(cfg.MetricsPath, scrapeHandler(exporter))
		mux.HandleFunc("/health", healthHandler)
		if snapshotStore != nil {
			mux.Handle("/v1/snapshots/", snapshot.Handler(snapshotStore, cfg.InternalServiceToken))
		}
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprintf(w, `<html>
<head><title>Xray Exporter</title></head>
<body>
<h1>Xray Exporter %s</h1>
<p><a href='/metrics'>Exporter Metrics</a></p>
<p><a href='%s'>Scrape Xray Metrics</a></p>
<p><a href='/health'>Health Check</a></p>
</body>
</html>
`, buildVersion, cfg.MetricsPath)
		})

		server = &http.Server{
			Addr:         cfg.Listen,
			Handler:      mux,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  120 * time.Second,
		}

		go func() {
			logrus.WithField("instance", cfg.Instance).Infof("Server starting on %s", cfg.Listen)
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				serverErr <- err
			}
		}()
	} else {
		logrus.WithField("instance", cfg.Instance).Info("Exporter HTTP server disabled")
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigCh)

	for {
		select {
		case err := <-serverErr:
			logrus.WithError(err).Error("Server failed to start")
			if stopSnapshots != nil {
				stopSnapshots(false)
			}
			return 1
		case sig := <-sigCh:
			if sig == syscall.SIGHUP {
				if engine == nil {
					logrus.Info("Ignoring SIGHUP in gRPC mode")
					continue
				}
				logrus.WithField("instance", cfg.Instance).Info("Received SIGHUP, reloading embedded Xray configuration")
				if err := engine.Reload(); err != nil {
					logrus.WithError(err).Error("Failed to reload embedded Xray configuration; keeping previous instance active")
					continue
				}
				logrus.WithField("instance", cfg.Instance).Info("Embedded Xray configuration reloaded successfully")
				if !engine.HasStatsManager() {
					logrus.Warn("Reloaded Xray config does not enable stats ({}); traffic metrics and billing snapshots will be empty")
				}
				continue
			}

			logrus.WithField("signal", sig.String()).Info("Shutting down...")
			if stopSnapshots != nil {
				stopSnapshots(true)
			}
			if server != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := server.Shutdown(ctx); err != nil {
					logrus.WithError(err).Error("Server forced to shutdown")
				}
				cancel()
			}
			logrus.Info("Server exited")
			return 0
		}
	}
}

func startSnapshotLoop(service *snapshot.Service, interval time.Duration, pusher *snapshot.Pusher, pushTimeout time.Duration) func(flush bool) {
	if interval <= 0 {
		interval = time.Minute
	}
	if pushTimeout <= 0 || pushTimeout > 5*time.Second {
		pushTimeout = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	collect := func() {
		value, err := service.Collect()
		if err != nil {
			logrus.WithError(err).Warn("Billing snapshot collection failed")
			return
		}
		if pusher != nil {
			pushCtx, pushCancel := context.WithTimeout(context.Background(), pushTimeout)
			defer pushCancel()
			if err := pusher.Push(pushCtx, value); err != nil {
				logrus.WithError(err).Warn("Vector snapshot push failed")
			}
		}
	}

	go func() {
		defer close(done)
		collect()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collect()
			}
		}
	}()

	var once sync.Once
	return func(flush bool) {
		once.Do(func() {
			cancel()
			<-done
			if flush {
				collect()
			}
		})
	}
}

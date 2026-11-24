package xrayembed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	applog "github.com/xtls/xray-core/app/log"
	"github.com/xtls/xray-core/app/stats/command"
	"github.com/xtls/xray-core/common/cmdarg"
	clog "github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/stats"
	_ "github.com/xtls/xray-core/main/distro/all"
)

var (
	// ErrNotRunning is returned when an operation requires a running embedded Xray instance.
	ErrNotRunning = errors.New("embedded xray instance is not running")
	// ErrAlreadyRunning is returned when Start is called on an already running Engine.
	ErrAlreadyRunning = errors.New("embedded xray instance is already running")
)

// Options configures an embedded Xray-core Engine.
type Options struct {
	ConfigPath  string
	ConfigFiles []string
	ConfigDir   string
	Format      string
	OnReload    func()
}

// Engine manages an in-process *core.Instance lifecycle and exposes direct
// in-memory access to Xray's stats.Manager with monotonic counter preservation
// across SIGHUP config reloads.
type Engine struct {
	opts       Options
	onReloads  []func()
	opMu       sync.Mutex // serializes Start / Reload / Close
	mu         sync.RWMutex
	instance   *core.Instance
	statsMgr   stats.Manager
	startTime  time.Time
	closed     bool
	prevBytes  []byte
	prevFormat string

	// Cumulative counter baselines folded from previous instances on Reload
	// so Prometheus counters and Billing snapshots remain strictly monotonic.
	baseOffsets map[string]int64
}

// NewEngine creates a new embedded Xray engine for a single config path.
func NewEngine(configPath string, onReload ...func()) (*Engine, error) {
	return NewEngineWithOptions(Options{
		ConfigPath: configPath,
		OnReload: func() {
			for _, cb := range onReload {
				if cb != nil {
					cb()
				}
			}
		},
	})
}

// NewEngineWithOptions creates a new embedded Xray engine from explicit options.
func NewEngineWithOptions(opts Options) (*Engine, error) {
	if _, err := resolveConfigFiles(opts); err != nil {
		return nil, err
	}
	var callbacks []func()
	if opts.OnReload != nil {
		callbacks = append(callbacks, opts.OnReload)
	}
	return &Engine{
		opts:        opts,
		onReloads:   callbacks,
		baseOffsets: make(map[string]int64),
	}, nil
}

// ValidateConfig validates a single Xray configuration file without starting
// listeners, HTTP endpoints, or background loops.
func ValidateConfig(configPath string) error {
	return ValidateOptions(Options{ConfigPath: configPath})
}

// ValidateOptions validates Xray configuration files/directories without
// starting listeners.
func ValidateOptions(opts Options) error {
	files, err := resolveConfigFiles(opts)
	if err != nil {
		return err
	}
	inst, _, _, _, err := buildInstance(files, opts.Format, nil)
	if err != nil {
		return err
	}
	_ = inst.Close()
	return nil
}

// Version returns the embedded Xray-core semantic version string.
func Version() string {
	return core.Version()
}

// VersionStatement returns Xray-core's full version banner lines.
func VersionStatement() []string {
	return core.VersionStatement()
}

// Start loads the configured Xray config, constructs the core.Instance, and
// starts all listeners and features.
func (e *Engine) Start() error {
	e.opMu.Lock()
	defer e.opMu.Unlock()

	if e.closed {
		return errors.New("embedded xray engine is closed")
	}
	e.mu.RLock()
	alreadyRunning := e.instance != nil
	e.mu.RUnlock()
	if alreadyRunning {
		return ErrAlreadyRunning
	}

	files, err := resolveConfigFiles(e.opts)
	if err != nil {
		return err
	}
	inst, sm, rawBytes, rawFormat, err := buildInstance(files, e.opts.Format, nil)
	if err != nil {
		return err
	}

	if err := inst.Start(); err != nil {
		_ = inst.Close()
		return fmt.Errorf("start embedded xray instance: %w", err)
	}

	e.mu.Lock()
	e.instance = inst
	e.statsMgr = sm
	if e.startTime.IsZero() {
		e.startTime = time.Now()
	}
	e.prevBytes = rawBytes
	e.prevFormat = rawFormat
	e.mu.Unlock()

	runtime.GC()
	debug.FreeOSMemory()
	return nil
}

// Reload validates the updated configuration before interrupting traffic, folds
// existing stats counters into the monotonic baseline, swaps the running
// core.Instance atomically, and rolls back to the previous config if the new
// listeners fail to bind.
func (e *Engine) Reload() error {
	e.opMu.Lock()
	if e.closed {
		e.opMu.Unlock()
		return errors.New("embedded xray engine is closed")
	}

	e.mu.RLock()
	oldInst := e.instance
	oldSM := e.statsMgr
	prevBytes := append([]byte(nil), e.prevBytes...)
	prevFormat := e.prevFormat
	e.mu.RUnlock()

	if oldInst == nil {
		e.opMu.Unlock()
		return ErrNotRunning
	}

	files, err := resolveConfigFiles(e.opts)
	if err != nil {
		e.opMu.Unlock()
		return err
	}

	var oldLogger clog.Handler
	if f := oldInst.GetFeature((*applog.Instance)(nil)); f != nil {
		if h, ok := f.(clog.Handler); ok {
			oldLogger = h
		}
	}

	newInst, newSM, rawBytes, rawFormat, err := buildInstance(files, e.opts.Format, oldLogger)
	if err != nil {
		e.opMu.Unlock()
		return fmt.Errorf("validate xray config for reload: %w", err)
	}

	// Close the previous instance first so TCP ports and Unix domain sockets
	// (including Xray's FileLocker *.lock) are released before Start binds them.
	// Counter values in oldSM remain readable in memory after Close().
	_ = oldInst.Close()

	e.mu.Lock()
	if oldSM != nil {
		oldSM.VisitCounters(func(name string, c stats.Counter) bool {
			e.baseOffsets[name] += c.Value()
			return true
		})
	}
	e.instance = newInst
	e.statsMgr = newSM
	e.mu.Unlock()

	if err := newInst.Start(); err != nil {
		_ = newInst.Close()
		e.mu.Lock()
		e.instance = nil
		e.statsMgr = nil
		e.mu.Unlock()

		if len(prevBytes) > 0 && prevFormat != "" {
			if rbInst, rbSM, rbErr := buildInstanceFromBytes(prevBytes, prevFormat, oldLogger); rbErr == nil {
				if rbStartErr := rbInst.Start(); rbStartErr == nil {
					e.mu.Lock()
					e.instance = rbInst
					e.statsMgr = rbSM
					e.mu.Unlock()
					e.opMu.Unlock()
					return fmt.Errorf("start reloaded xray instance (rolled back to previous config): %w", err)
				} else {
					_ = rbInst.Close()
				}
			}
		}
		e.opMu.Unlock()
		return fmt.Errorf("start reloaded xray instance: %w", err)
	}

	e.mu.Lock()
	e.prevBytes = rawBytes
	e.prevFormat = rawFormat
	e.mu.Unlock()

	runtime.GC()
	debug.FreeOSMemory()

	callbacks := slices.Clone(e.onReloads)
	e.opMu.Unlock()

	for _, cb := range callbacks {
		if cb != nil {
			cb()
		}
	}
	return nil
}

// Close shuts down the running embedded Xray instance. Safe to call multiple times.
func (e *Engine) Close() error {
	e.opMu.Lock()
	defer e.opMu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	e.mu.Lock()
	inst := e.instance
	e.instance = nil
	e.statsMgr = nil
	e.mu.Unlock()

	if inst == nil {
		return nil
	}
	return inst.Close()
}

// IsRunning reports whether an embedded core.Instance is currently active.
func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.instance != nil && e.instance.IsRunning()
}

// HasStatsManager reports whether the active Xray config enabled a real
// stats.Manager rather than Xray's NoopManager fallback.
func (e *Engine) HasStatsManager() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.statsMgr == nil {
		return false
	}
	_, isNoop := e.statsMgr.(stats.NoopManager)
	return !isNoop
}

// QueryStats reads counters directly from the in-memory stats.Manager merged
// with monotonic counter baselines from prior SIGHUP reloads.
func (e *Engine) QueryStats(ctx context.Context, pattern string, reset bool) ([]*command.Stat, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	if reset {
		e.mu.Lock()
		defer e.mu.Unlock()
	} else {
		e.mu.RLock()
		defer e.mu.RUnlock()
	}

	if e.instance == nil {
		return nil, ErrNotRunning
	}

	merged := make(map[string]int64, len(e.baseOffsets))
	for name, val := range e.baseOffsets {
		if pattern == "" || strings.Contains(name, pattern) {
			merged[name] = val
			if reset {
				e.baseOffsets[name] = 0
			}
		}
	}

	if e.statsMgr != nil {
		e.statsMgr.VisitCounters(func(name string, c stats.Counter) bool {
			if pattern == "" || strings.Contains(name, pattern) {
				var val int64
				if reset {
					val = c.Set(0)
				} else {
					val = c.Value()
				}
				merged[name] += val
			}
			return true
		})
	}

	result := make([]*command.Stat, 0, len(merged))
	for name, val := range merged {
		result = append(result, &command.Stat{
			Name:  name,
			Value: val,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result, nil
}

// GetSysStats returns process runtime and memory statistics matching Xray's
// StatsService.GetSysStats response without gRPC overhead.
func (e *Engine) GetSysStats(ctx context.Context) (*command.SysStatsResponse, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	e.mu.RLock()
	inst := e.instance
	startTime := e.startTime
	e.mu.RUnlock()

	if inst == nil {
		return nil, ErrNotRunning
	}

	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)

	var uptime time.Duration
	if !startTime.IsZero() {
		uptime = time.Since(startTime)
	}

	return &command.SysStatsResponse{
		Uptime:       uint32(uptime.Seconds()),
		NumGoroutine: uint32(runtime.NumGoroutine()),
		Alloc:        rtm.Alloc,
		TotalAlloc:   rtm.TotalAlloc,
		Sys:          rtm.Sys,
		Mallocs:      rtm.Mallocs,
		Frees:        rtm.Frees,
		LiveObjects:  rtm.Mallocs - rtm.Frees,
		NumGC:        rtm.NumGC,
		PauseTotalNs: rtm.PauseTotalNs,
	}, nil
}

func buildInstance(files []string, format string, restoreLogger clog.Handler) (*core.Instance, stats.Manager, []byte, string, error) {
	loadFmt := resolveLoadFormat(format)
	pbCfg, err := core.LoadConfig(loadFmt, cmdarg.Arg(files))
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("load xray config %v: %w", files, err)
	}

	inst, err := core.New(pbCfg)
	if err != nil {
		if restoreLogger != nil {
			clog.RegisterHandler(restoreLogger)
		}
		return nil, nil, nil, "", fmt.Errorf("create xray instance: %w", err)
	}

	sm, _ := inst.GetFeature(stats.ManagerType()).(stats.Manager)

	var rawBytes []byte
	var rawFormat string
	if len(files) == 1 && !strings.HasPrefix(files[0], "http://") && !strings.HasPrefix(files[0], "https://") && files[0] != "stdin:" {
		if data, readErr := os.ReadFile(files[0]); readErr == nil {
			rawBytes = data
			rawFormat = resolveSingleFileFormat(files[0], loadFmt)
		}
	}
	return inst, sm, rawBytes, rawFormat, nil
}

func buildInstanceFromBytes(raw []byte, format string, restoreLogger clog.Handler) (*core.Instance, stats.Manager, error) {
	pbCfg, err := core.LoadConfig(format, bytes.NewReader(raw))
	if err != nil {
		return nil, nil, err
	}
	inst, err := core.New(pbCfg)
	if err != nil {
		if restoreLogger != nil {
			clog.RegisterHandler(restoreLogger)
		}
		return nil, nil, err
	}
	sm, _ := inst.GetFeature(stats.ManagerType()).(stats.Manager)
	return inst, sm, nil
}

func resolveLoadFormat(format string) string {
	trimmed := strings.TrimSpace(format)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return "auto"
	}
	if mapped := core.GetFormatByExtension(trimmed); mapped != "" {
		return mapped
	}
	return strings.ToLower(trimmed)
}

func resolveSingleFileFormat(path, loadFmt string) string {
	if loadFmt != "" && loadFmt != "auto" {
		return loadFmt
	}
	if detected := core.GetFormat(path); detected != "" {
		return detected
	}
	return "json"
}

func resolveConfigFiles(opts Options) ([]string, error) {
	var files []string
	if strings.TrimSpace(opts.ConfigDir) != "" {
		dirEntries, err := readConfDir(opts.ConfigDir, opts.Format)
		if err != nil {
			return nil, err
		}
		files = append(files, dirEntries...)
	}
	if p := strings.TrimSpace(opts.ConfigPath); p != "" {
		files = append(files, p)
	}
	for _, f := range opts.ConfigFiles {
		if trimmed := strings.TrimSpace(f); trimmed != "" {
			files = append(files, trimmed)
		}
	}
	if len(files) == 0 {
		return nil, errors.New("xray config path is required")
	}
	return files, nil
}

func readConfDir(dirPath, format string) ([]string, error) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("read config dir %s: %w", dirPath, err)
	}
	pattern := `^.+\.(json|jsonc|toml|yaml|yml)$`
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		pattern = `^.+\.(json|jsonc)$`
	case "toml":
		pattern = `^.+\.toml$`
	case "yaml", "yml":
		pattern = `^.+\.(yaml|yml)$`
	}
	re := regexp.MustCompile(pattern)
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if re.MatchString(entry.Name()) {
			files = append(files, filepath.Join(dirPath, entry.Name()))
		}
	}
	return files, nil
}

package snapshot

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type traceParentContextKey struct{}

// WithTraceParent attaches a W3C traceparent string to a context so outbound
// HTTP clients can inject the traceparent header.
func WithTraceParent(ctx context.Context, traceParent string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, traceParentContextKey{}, strings.TrimSpace(traceParent))
}

// TraceParentFromContext extracts the W3C traceparent string from a context.
func TraceParentFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(traceParentContextKey{}).(string)
	return v
}

// GenerateSnapshotRef computes a deterministic 16-char hex SHA-256 reference of
// env + "|" + node_id + "|" + collected_at.UTC().Format(time.RFC3339Nano).
func GenerateSnapshotRef(env, nodeID string, collectedAt time.Time) string {
	raw := strings.TrimSpace(env) + "|" + strings.TrimSpace(nodeID) + "|" + collectedAt.UTC().Format(time.RFC3339Nano)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:8])
}

// GenerateTraceParent produces a valid W3C traceparent ("00-<32-hex-trace-id>-<16-hex-span-id>-01").
func GenerateTraceParent() string {
	var traceID [16]byte
	var spanID [8]byte
	if _, err := rand.Read(traceID[:]); err != nil || isAllZero(traceID[:]) {
		binarySeed(traceID[:], time.Now().UnixNano())
	}
	if _, err := rand.Read(spanID[:]); err != nil || isAllZero(spanID[:]) {
		binarySeed(spanID[:], time.Now().UnixNano()^0x5a5a5a5a5a5a5a5a)
	}
	return "00-" + hex.EncodeToString(traceID[:]) + "-" + hex.EncodeToString(spanID[:]) + "-01"
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func binarySeed(dst []byte, seed int64) {
	u := uint64(seed)
	if u == 0 {
		u = 1
	}
	for i := range dst {
		u = u*6364136223846793005 + 1442695040888963407
		dst[i] = byte(u >> 32)
	}
	if isAllZero(dst) {
		dst[len(dst)-1] = 1
	}
}

// DeriveTransport extracts a low-cardinality transport label ("xhttp", "tcp", or "unknown")
// from nodeID or env without exposing raw hostnames or dynamic identifiers.
func DeriveTransport(nodeID, env string) string {
	for _, candidate := range []string{nodeID, env} {
		lower := strings.ToLower(strings.TrimSpace(candidate))
		switch {
		case strings.Contains(lower, "xhttp"):
			return "xhttp"
		case strings.Contains(lower, "tcp"):
			return "tcp"
		case strings.Contains(lower, "grpc"):
			return "grpc"
		case strings.Contains(lower, "ws"):
			return "ws"
		}
	}
	return "unknown"
}

func sanitizeEnv(env string) string {
	trimmed := strings.ToLower(strings.TrimSpace(env))
	if trimmed == "" {
		return "unknown"
	}
	return trimmed
}

func sanitizeTransport(transport, nodeID, env string) string {
	trimmed := strings.ToLower(strings.TrimSpace(transport))
	if trimmed != "" {
		return trimmed
	}
	return DeriveTransport(nodeID, env)
}

// Metrics holds low-cardinality Prometheus collectors for the snapshot pipeline
// and outbound Accounts/Vector HTTP dependencies.
type Metrics struct {
	collectTotal          *prometheus.CounterVec
	collectDuration       *prometheus.HistogramVec
	lastCollectedTime     *prometheus.GaugeVec
	samplesCount          *prometheus.GaugeVec
	pushTotal             *prometheus.CounterVec
	pushEnabled           *prometheus.GaugeVec
	httpPhaseDuration     *prometheus.HistogramVec
}

var (
	defaultMetricsOnce sync.Once
	defaultMetricsInst *Metrics
)

// DefaultMetrics returns the process-wide snapshot metrics registered on
// prometheus.DefaultRegisterer.
func DefaultMetrics() *Metrics {
	defaultMetricsOnce.Do(func() {
		defaultMetricsInst = NewMetrics(prometheus.DefaultRegisterer)
	})
	return defaultMetricsInst
}

// NewMetrics creates and registers low-cardinality snapshot telemetry collectors
// on the provided prometheus.Registerer (if non-nil).
func NewMetrics(reg prometheus.Registerer) *Metrics {
	durationBuckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}
	m := &Metrics{
		collectTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "xray",
			Subsystem: "snapshot",
			Name:      "collect_total",
			Help:      "Total number of snapshot collection attempts by outcome.",
		}, []string{"env", "transport", "outcome"}),
		collectDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "xray",
			Subsystem: "snapshot",
			Name:      "collect_duration_seconds",
			Help:      "Duration of snapshot collection and push stages in seconds.",
			Buckets:   durationBuckets,
		}, []string{"env", "transport", "stage"}),
		lastCollectedTime: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "xray",
			Subsystem: "snapshot",
			Name:      "last_collected_timestamp_seconds",
			Help:      "Unix timestamp in seconds of the latest successful snapshot collection.",
		}, []string{"env", "transport"}),
		samplesCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "xray",
			Subsystem: "snapshot",
			Name:      "samples_count",
			Help:      "Number of normalized account samples in the latest collected snapshot.",
		}, []string{"env", "transport"}),
		pushTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "xray",
			Subsystem: "snapshot",
			Name:      "push_total",
			Help:      "Total number of Vector snapshot push attempts by status class.",
		}, []string{"env", "transport", "status_class"}),
		pushEnabled: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "xray",
			Subsystem: "snapshot",
			Name:      "push_enabled",
			Help:      "Whether Vector snapshot push is enabled (1) or disabled (0).",
		}, []string{"env", "transport"}),
		httpPhaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "xray",
			Subsystem: "snapshot",
			Name:      "http_phase_duration_seconds",
			Help:      "Duration of outbound HTTP client phases (dns, tcp, tls, ttfb, total) in seconds.",
			Buckets:   durationBuckets,
		}, []string{"env", "transport", "target", "phase"}),
	}

	if reg != nil {
		for _, c := range m.Collectors() {
			if err := reg.Register(c); err != nil {
				var are prometheus.AlreadyRegisteredError
				if errors.As(err, &are) {
					continue
				}
			}
		}
	}
	return m
}

// Collectors returns the underlying prometheus.Collector instances so callers
// can register them on custom registries (such as the Exporter's /scrape registry).
func (m *Metrics) Collectors() []prometheus.Collector {
	if m == nil {
		return nil
	}
	return []prometheus.Collector{
		m.collectTotal,
		m.collectDuration,
		m.lastCollectedTime,
		m.samplesCount,
		m.pushTotal,
		m.pushEnabled,
		m.httpPhaseDuration,
	}
}

func (m *Metrics) ObserveCollectStage(env, transport, stage string, d time.Duration) {
	if m == nil {
		return
	}
	m.collectDuration.WithLabelValues(sanitizeEnv(env), sanitizeTransport(transport, "", env), stage).Observe(d.Seconds())
}

func (m *Metrics) IncCollectOutcome(env, transport, outcome string) {
	if m == nil {
		return
	}
	m.collectTotal.WithLabelValues(sanitizeEnv(env), sanitizeTransport(transport, "", env), outcome).Inc()
}

func (m *Metrics) RecordCollectSuccess(env, transport string, collectedAt time.Time, samples int) {
	if m == nil {
		return
	}
	e := sanitizeEnv(env)
	t := sanitizeTransport(transport, "", env)
	m.lastCollectedTime.WithLabelValues(e, t).Set(float64(collectedAt.UTC().Unix()))
	m.samplesCount.WithLabelValues(e, t).Set(float64(samples))
}

func (m *Metrics) SetPushEnabled(env, transport string, enabled bool) {
	if m == nil {
		return
	}
	val := 0.0
	if enabled {
		val = 1.0
	}
	m.pushEnabled.WithLabelValues(sanitizeEnv(env), sanitizeTransport(transport, "", env)).Set(val)
}

func (m *Metrics) IncPushStatus(env, transport, statusClass string) {
	if m == nil {
		return
	}
	m.pushTotal.WithLabelValues(sanitizeEnv(env), sanitizeTransport(transport, "", env), statusClass).Inc()
}

func (m *Metrics) ObserveHTTPPhase(env, transport, target, phase string, d time.Duration) {
	if m == nil {
		return
	}
	m.httpPhaseDuration.WithLabelValues(sanitizeEnv(env), sanitizeTransport(transport, "", env), target, phase).Observe(d.Seconds())
}

type httpPhaseTracker struct {
	mu      sync.Mutex
	start   time.Time
	dnsStart time.Time
	dnsDone  time.Duration
	hasDNS   bool

	tcpStart time.Time
	tcpDone  time.Duration
	hasTCP   bool

	tlsStart time.Time
	tlsDone  time.Duration
	hasTLS   bool

	ttfb    time.Duration
	hasTTFB bool
}

func attachHTTPTrace(req *http.Request) (*http.Request, *httpPhaseTracker) {
	tracker := &httpPhaseTracker{
		start: time.Now(),
	}
	trace := &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			tracker.mu.Lock()
			tracker.dnsStart = time.Now()
			tracker.mu.Unlock()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			tracker.mu.Lock()
			if !tracker.dnsStart.IsZero() {
				tracker.dnsDone = time.Since(tracker.dnsStart)
				tracker.hasDNS = true
			}
			tracker.mu.Unlock()
		},
		ConnectStart: func(_, _ string) {
			tracker.mu.Lock()
			tracker.tcpStart = time.Now()
			tracker.mu.Unlock()
		},
		ConnectDone: func(_, _ string, _ error) {
			tracker.mu.Lock()
			if !tracker.tcpStart.IsZero() {
				tracker.tcpDone = time.Since(tracker.tcpStart)
				tracker.hasTCP = true
			}
			tracker.mu.Unlock()
		},
		TLSHandshakeStart: func() {
			tracker.mu.Lock()
			tracker.tlsStart = time.Now()
			tracker.mu.Unlock()
		},
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			tracker.mu.Lock()
			if !tracker.tlsStart.IsZero() {
				tracker.tlsDone = time.Since(tracker.tlsStart)
				tracker.hasTLS = true
			}
			tracker.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			tracker.mu.Lock()
			if !tracker.start.IsZero() && !tracker.hasTTFB {
				tracker.ttfb = time.Since(tracker.start)
				tracker.hasTTFB = true
			}
			tracker.mu.Unlock()
		},
	}
	ctx := httptrace.WithClientTrace(req.Context(), trace)
	return req.WithContext(ctx), tracker
}

func (t *httpPhaseTracker) record(m *Metrics, env, transport, target string) {
	if t == nil || m == nil {
		return
	}
	total := time.Since(t.start)
	t.mu.Lock()
	hasDNS, dnsDone := t.hasDNS, t.dnsDone
	hasTCP, tcpDone := t.hasTCP, t.tcpDone
	hasTLS, tlsDone := t.hasTLS, t.tlsDone
	hasTTFB, ttfb := t.hasTTFB, t.ttfb
	t.mu.Unlock()

	if hasDNS {
		m.ObserveHTTPPhase(env, transport, target, "dns", dnsDone)
	}
	if hasTCP {
		m.ObserveHTTPPhase(env, transport, target, "tcp", tcpDone)
	}
	if hasTLS {
		m.ObserveHTTPPhase(env, transport, target, "tls", tlsDone)
	}
	if hasTTFB {
		m.ObserveHTTPPhase(env, transport, target, "ttfb", ttfb)
	}
	m.ObserveHTTPPhase(env, transport, target, "total", total)
}

func classifyHTTPStatus(code int) string {
	switch {
	case code >= 200 && code < 300:
		return "2xx"
	case code >= 400 && code < 500:
		return "4xx"
	case code >= 500 && code < 600:
		return "5xx"
	default:
		return "network_error"
	}
}

func classifyHTTPError(err error) string {
	if err == nil {
		return "2xx"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	if strings.Contains(strings.ToLower(err.Error()), "timeout") || strings.Contains(strings.ToLower(err.Error()), "deadline exceeded") {
		return "timeout"
	}
	return "network_error"
}

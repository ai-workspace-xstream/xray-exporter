package snapshot

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestAccountsClientIndexesProxyAccountAndEmail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/internal/network/identities" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("X-Service-Token") != "secret" {
			t.Fatalf("missing service token")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"identities":[{"uuid":"proxy-1","email":"User@Example.com","accountUuid":"account-1"}]}`))
	}))
	defer server.Close()

	identities, err := NewAccountsClient(server.URL, "secret").FetchIdentities(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"proxy-1", "account-1", "user@example.com"} {
		identity, ok := identities[key]
		if !ok {
			t.Fatalf("missing lookup key %q", key)
		}
		if identity.AccountUUID != "account-1" || identity.Email != "user@example.com" {
			t.Fatalf("unexpected identity %#v", identity)
		}
	}
}

func TestHTTPTracePhaseTrackingFreshVsReusedAndTLS(t *testing.T) {
	// 1. Plain HTTP server: test fresh connection vs reused connection (Keep-Alive)
	plainServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"identities":[{"uuid":"proxy-1","email":"user@example.com","accountUuid":"account-1"}]}`))
	}))
	defer plainServer.Close()

	regPlain := prometheus.NewRegistry()
	metricsPlain := NewMetrics(regPlain)
	// Use localhost URL so DNS resolution is triggered on fresh dial
	localhostURL := strings.Replace(plainServer.URL, "127.0.0.1", "localhost", 1)
	client := NewAccountsClient(localhostURL, "secret").WithTelemetry("uat", "xhttp", metricsPlain)

	tp1 := GenerateTraceParent()
	if _, err := client.FetchIdentitiesWithContext(WithTraceParent(context.Background(), tp1), time.Second); err != nil {
		t.Fatalf("first FetchIdentities failed: %v", err)
	}

	tcpAfterFirst := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "tcp",
	})
	tlsAfterFirst := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "tls",
	})
	ttfbAfterFirst := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "ttfb",
	})
	totalAfterFirst := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "total",
	})

	if tcpAfterFirst != 1 {
		t.Fatalf("expected 1 tcp phase observation on fresh connection, got %d", tcpAfterFirst)
	}
	if tlsAfterFirst != 0 {
		t.Fatalf("expected 0 tls phase observations on plain HTTP connection, got %d", tlsAfterFirst)
	}
	if ttfbAfterFirst != 1 || totalAfterFirst != 1 {
		t.Fatalf("expected ttfb=1 and total=1 on first request, got ttfb=%d total=%d", ttfbAfterFirst, totalAfterFirst)
	}

	// Second call reuses the keep-alive connection: tcp (and dns/tls) must NOT record a 0 observation!
	tp2 := GenerateTraceParent()
	if _, err := client.FetchIdentitiesWithContext(WithTraceParent(context.Background(), tp2), time.Second); err != nil {
		t.Fatalf("second FetchIdentities failed: %v", err)
	}

	tcpAfterSecond := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "tcp",
	})
	tlsAfterSecond := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "tls",
	})
	ttfbAfterSecond := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "ttfb",
	})
	totalAfterSecond := histogramCount(t, regPlain, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "total",
	})

	if tcpAfterSecond != 1 {
		t.Fatalf("expected tcp phase count to remain 1 on reused keep-alive connection, got %d", tcpAfterSecond)
	}
	if tlsAfterSecond != 0 {
		t.Fatalf("expected tls phase count to remain 0 on plain HTTP connection, got %d", tlsAfterSecond)
	}
	if ttfbAfterSecond != 2 || totalAfterSecond != 2 {
		t.Fatalf("expected ttfb=2 and total=2 after second request, got ttfb=%d total=%d", ttfbAfterSecond, totalAfterSecond)
	}

	// 2. TLS server: verify tls phase is recorded on fresh HTTPS connection and not re-recorded on reused TLS connection
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"identities":[]}`))
	}))
	defer tlsServer.Close()

	regTLS := prometheus.NewRegistry()
	metricsTLS := NewMetrics(regTLS)
	tlsClient := NewAccountsClient(tlsServer.URL, "secret").WithTelemetry("uat", "xhttp", metricsTLS)
	tlsClient.client = tlsServer.Client()

	if _, err := tlsClient.FetchIdentitiesWithContext(context.Background(), time.Second); err != nil {
		t.Fatalf("first TLS FetchIdentities failed: %v", err)
	}
	if got := histogramCount(t, regTLS, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "tls",
	}); got != 1 {
		t.Fatalf("expected 1 tls phase observation on fresh HTTPS connection, got %d", got)
	}

	if _, err := tlsClient.FetchIdentitiesWithContext(context.Background(), time.Second); err != nil {
		t.Fatalf("second TLS FetchIdentities failed: %v", err)
	}
	if got := histogramCount(t, regTLS, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "tls",
	}); got != 1 {
		t.Fatalf("expected tls phase count to remain 1 on reused HTTPS connection, got %d", got)
	}
}

func TestHTTPTracePhaseFailureMatrixDNSUntrustedTLSConnectionRefusedAndCancel(t *testing.T) {
	// 1. Untrusted TLS certificate: TCP and TLS handshake attempt occur, but TTFB must NOT be recorded as 0.
	untrustedTLS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer untrustedTLS.Close()

	regUntrusted := prometheus.NewRegistry()
	metricsUntrusted := NewMetrics(regUntrusted)
	// Use default client (does not trust untrustedTLS self-signed cert)
	untrustedClient := NewAccountsClient(untrustedTLS.URL, "secret").WithTelemetry("uat", "xhttp", metricsUntrusted)
	if _, err := untrustedClient.FetchIdentitiesWithContext(context.Background(), time.Second); err == nil {
		t.Fatal("expected untrusted TLS error")
	}
	if got := histogramCount(t, regUntrusted, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "ttfb",
	}); got != 0 {
		t.Fatalf("expected 0 ttfb observations on TLS failure, got %d", got)
	}

	// 2. TCP connection refused: TLS and TTFB must NOT be recorded as 0.
	regRefused := prometheus.NewRegistry()
	metricsRefused := NewMetrics(regRefused)
	refusedClient := NewAccountsClient("http://127.0.0.1:1", "secret").WithTelemetry("uat", "xhttp", metricsRefused)
	if _, err := refusedClient.FetchIdentitiesWithContext(context.Background(), 500*time.Millisecond); err == nil {
		t.Fatal("expected connection refused error")
	}
	for _, phase := range []string{"tls", "ttfb"} {
		if got := histogramCount(t, regRefused, "xray_snapshot_http_phase_duration_seconds", map[string]string{
			"env": "uat", "transport": "xhttp", "target": "accounts", "phase": phase,
		}); got != 0 {
			t.Fatalf("expected 0 %s observations on connection refused, got %d", phase, got)
		}
	}

	// 3. Context cancellation / client abort mid-request: TTFB must NOT be recorded as 0.
	blockCh := make(chan struct{})
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blockCh
	}))
	defer func() {
		close(blockCh)
		slowServer.Close()
	}()

	regCancel := prometheus.NewRegistry()
	metricsCancel := NewMetrics(regCancel)
	cancelClient := NewAccountsClient(slowServer.URL, "secret").WithTelemetry("uat", "xhttp", metricsCancel)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := cancelClient.FetchIdentitiesWithContext(ctx, time.Second); err == nil {
		t.Fatal("expected canceled/deadline error")
	}
	if got := histogramCount(t, regCancel, "xray_snapshot_http_phase_duration_seconds", map[string]string{
		"env": "uat", "transport": "xhttp", "target": "accounts", "phase": "ttfb",
	}); got != 0 {
		t.Fatalf("expected 0 ttfb observations on canceled request before response headers, got %d", got)
	}
}

func histogramCount(t *testing.T, reg *prometheus.Registry, metricName string, labels map[string]string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchLabels(m.GetLabel(), labels) {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}


package snapshot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestPusherPostsAuthorizedSnapshot(t *testing.T) {
	var receivedTraceParent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("unexpected request: %s %s auth=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
		receivedTraceParent = r.Header.Get("traceparent")
		var got Snapshot
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode snapshot: %v", err)
		}
		if got.NodeID != "node-1" || len(got.Samples) != 1 {
			t.Fatalf("unexpected snapshot: %#v", got)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	expectedTP := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	pusher := NewPusher(server.URL, "token", time.Second)
	err := pusher.Push(context.Background(), Snapshot{
		CollectedAt: time.Now().UTC(),
		NodeID:      "node-1",
		Env:         "uat",
		SnapshotRef: "0123456789abcdef",
		TraceParent: expectedTP,
		Samples:     []Sample{{UUID: "00000000-0000-0000-0000-000000000001"}},
	})
	if err != nil {
		t.Fatalf("push snapshot: %v", err)
	}
	if receivedTraceParent != expectedTP {
		t.Fatalf("expected traceparent header %q, got %q", expectedTP, receivedTraceParent)
	}
}

func TestPusherTreatsNon2xxAsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := NewPusher(server.URL, "token", time.Second).Push(context.Background(), Snapshot{})
	if err == nil {
		t.Fatal("expected non-2xx push error")
	}
}

func TestPusherRecordsStatusClassesAndPushEnabledGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)

	// 1. Disabled pusher sets push_enabled = 0 and records status_class = "disabled"
	disabledPusher := NewPusher("", "", time.Second).WithTelemetry("uat", "xhttp", metrics)
	if disabledPusher.Enabled() {
		t.Fatal("expected disabled pusher")
	}
	if got := gaugeValue(t, reg, "xray_snapshot_push_enabled", map[string]string{"env": "uat", "transport": "xhttp"}); got != 0 {
		t.Fatalf("expected push_enabled=0 for disabled pusher, got %v", got)
	}
	if err := disabledPusher.Push(context.Background(), Snapshot{Env: "uat", NodeID: "uat-xhttp"}); err != nil {
		t.Fatalf("disabled push should not return error: %v", err)
	}
	if got := counterValue(t, reg, "xray_snapshot_push_total", map[string]string{"env": "uat", "transport": "xhttp", "status_class": "disabled"}); got != 1 {
		t.Fatalf("expected status_class=disabled count=1, got %v", got)
	}

	// 2. Enabled pusher sets push_enabled = 1 and records 2xx, 4xx, 5xx, timeout
	var responseStatus int
	var responseDelay time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !traceParentRegex.MatchString(r.Header.Get("traceparent")) {
			t.Errorf("invalid or missing traceparent header: %q", r.Header.Get("traceparent"))
		}
		if responseDelay > 0 {
			time.Sleep(responseDelay)
		}
		w.WriteHeader(responseStatus)
	}))
	defer server.Close()

	enabledPusher := NewPusher(server.URL, "token", 40*time.Millisecond).WithTelemetry("uat", "xhttp", metrics)
	if !enabledPusher.Enabled() {
		t.Fatal("expected enabled pusher")
	}
	if got := gaugeValue(t, reg, "xray_snapshot_push_enabled", map[string]string{"env": "uat", "transport": "xhttp"}); got != 1 {
		t.Fatalf("expected push_enabled=1 for enabled pusher, got %v", got)
	}

	snap := Snapshot{
		CollectedAt: time.Now().UTC(),
		NodeID:      "uat-xhttp",
		Env:         "uat",
		TraceParent: GenerateTraceParent(),
	}

	// 2xx
	responseStatus = http.StatusAccepted
	responseDelay = 0
	if err := enabledPusher.Push(context.Background(), snap); err != nil {
		t.Fatalf("expected 2xx success, got %v", err)
	}

	// 4xx
	responseStatus = http.StatusUnauthorized
	if err := enabledPusher.Push(context.Background(), snap); err == nil {
		t.Fatal("expected 4xx error")
	}

	// 5xx
	responseStatus = http.StatusBadGateway
	if err := enabledPusher.Push(context.Background(), snap); err == nil {
		t.Fatal("expected 5xx error")
	}

	// timeout
	responseStatus = http.StatusOK
	responseDelay = 120 * time.Millisecond
	if err := enabledPusher.Push(context.Background(), snap); err == nil {
		t.Fatal("expected timeout error")
	}

	for _, sc := range []string{"2xx", "4xx", "5xx", "timeout"} {
		if got := counterValue(t, reg, "xray_snapshot_push_total", map[string]string{"env": "uat", "transport": "xhttp", "status_class": sc}); got != 1 {
			t.Fatalf("expected push_total status_class=%q count=1, got %v", sc, got)
		}
	}
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, metricName string, labels map[string]string) float64 {
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
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("gauge %s with labels %v not found", metricName, labels)
	return 0
}

func counterValue(t *testing.T, reg *prometheus.Registry, metricName string, labels map[string]string) float64 {
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
				return m.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("counter %s with labels %v not found", metricName, labels)
	return 0
}

func matchLabels(pairs []*dto.LabelPair, expected map[string]string) bool {
	if len(pairs) != len(expected) {
		return false
	}
	for _, lp := range pairs {
		if expected[lp.GetName()] != lp.GetValue() {
			return false
		}
	}
	return true
}


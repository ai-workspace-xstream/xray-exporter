package xray

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestParseRawStatSupportsUserTrafficPattern(t *testing.T) {
	counter, ok := parseRawStat("user>>>acct-1>>>traffic>>>uplink", "42")
	if !ok {
		t.Fatalf("expected stat to parse")
	}
	if counter.UUID != "acct-1" || counter.Direction != "uplink" || counter.Value != 42 {
		t.Fatalf("unexpected counter %#v", counter)
	}
}

func TestParseRawStatSupportsInboundTrafficPattern(t *testing.T) {
	counter, ok := parseRawStat("inbound>>>premium>>>user>>>acct-1>>>traffic>>>downlink", 64.0)
	if !ok {
		t.Fatalf("expected stat to parse")
	}
	if counter.UUID != "acct-1" || counter.InboundTag != "premium" || counter.Direction != "downlink" || counter.Value != 64 {
		t.Fatalf("unexpected counter %#v", counter)
	}
}

func TestParseExpvarCountersFromDebugVarsStylePayload(t *testing.T) {
	payloadPath := filepath.Join("testdata", "debug_vars.sample.json")
	raw, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatalf("read sample payload: %v", err)
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	counters := parseExpvarCounters(payload)
	if len(counters) != 4 {
		t.Fatalf("expected 4 counters, got %d", len(counters))
	}
	if counters[0].UUID == "" && counters[1].UUID == "" && counters[2].UUID == "" && counters[3].UUID == "" {
		t.Fatalf("expected parsed uuids in counters %#v", counters)
	}
}

func TestParsePrometheusCountersFromLegacyExporter(t *testing.T) {
	body := []byte("# HELP xray_traffic_downlink_bytes_total legacy\n" +
		"xray_traffic_downlink_bytes_total{dimension=\"user\",target=\"user@example.com\",inbound_tag=\"xhttp\"} 2048\n" +
		"xray_traffic_uplink_bytes_total{dimension=\"user\",target=\"user@example.com\",inbound_tag=\"xhttp\"} 512\n" +
		"xray_traffic_downlink_bytes_total{dimension=\"system\",target=\"ignored\"} 999\n")

	counters := parsePrometheusCounters(body)
	if len(counters) != 2 {
		t.Fatalf("expected two user counters, got %#v", counters)
	}
	if counters[0].UUID != "user@example.com" || counters[0].InboundTag != "xhttp" || counters[0].Direction != "downlink" || counters[0].Value != 2048 {
		t.Fatalf("unexpected downlink counter %#v", counters[0])
	}
	if counters[1].Direction != "uplink" || counters[1].Value != 512 {
		t.Fatalf("unexpected uplink counter %#v", counters[1])
	}
}

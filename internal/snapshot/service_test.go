package snapshot

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type fakeSource struct {
	counters []RawCounter
	err      error
}

func (f fakeSource) TrafficCounters(time.Duration) ([]RawCounter, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.counters, nil
}

type fakeIdentities map[string]Identity

func (f fakeIdentities) FetchIdentities(time.Duration) (map[string]Identity, error) {
	return f, nil
}

type errIdentities struct {
	err error
}

func (e errIdentities) FetchIdentities(time.Duration) (map[string]Identity, error) {
	return nil, e.err
}

var (
	snapshotRefRegex = regexp.MustCompile(`^[0-9a-f]{16}$`)
	traceParentRegex = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`)
)

func TestNormalizeResolvesEmailAndAggregatesCounters(t *testing.T) {
	store, err := NewStore("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	service := NewService("uat-xhttp", "uat", fakeSource{counters: []RawCounter{
		{Identifier: "USER@EXAMPLE.COM", Direction: "uplink", Value: 100},
		{Identifier: "USER@EXAMPLE.COM", Direction: "downlink", Value: 200},
		{Identifier: "account-1", Direction: "uplink", Value: 50},
		{Identifier: "account-1", Direction: "downlink", Value: 75},
	}}, fakeIdentities{
		"user@example.com": {ProxyUUID: "proxy-1", Email: "user@example.com", AccountUUID: "account-1"},
		"account-1":        {ProxyUUID: "proxy-1", Email: "user@example.com", AccountUUID: "account-1"},
	}, store, time.Second)

	got, err := service.Collect()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Samples) != 1 {
		t.Fatalf("expected one canonical sample, got %#v", got.Samples)
	}
	if got.Samples[0].UUID != "account-1" || got.Samples[0].Email != "user@example.com" {
		t.Fatalf("unexpected identity %#v", got.Samples[0])
	}
	if got.Samples[0].UplinkBytesTotal != 150 || got.Samples[0].DownlinkBytesTotal != 275 {
		t.Fatalf("unexpected totals %#v", got.Samples[0])
	}
}

func TestNormalizeDropsUnresolvedEmail(t *testing.T) {
	got := normalize([]RawCounter{{Identifier: "unknown@example.com", Direction: "uplink", Value: 1}}, nil)
	if len(got) != 0 {
		t.Fatalf("expected unresolved email to be dropped, got %#v", got)
	}
}

func TestNormalizeAggregatesMultipleInboundsByAccount(t *testing.T) {
	got := normalize([]RawCounter{
		{Identifier: "account-1", InboundTag: "xhttp-local", Direction: "uplink", Value: 100},
		{Identifier: "account-1", InboundTag: "xhttp-local", Direction: "downlink", Value: 200},
		{Identifier: "account-1", InboundTag: "tcp-local", Direction: "uplink", Value: 300},
		{Identifier: "account-1", InboundTag: "tcp-local", Direction: "downlink", Value: 400},
	}, map[string]Identity{
		"account-1": {AccountUUID: "account-1", Email: "user@example.com"},
	})
	if len(got) != 1 {
		t.Fatalf("expected one UUID sample, got %#v", got)
	}
	if got[0].UplinkBytesTotal != 400 || got[0].DownlinkBytesTotal != 600 {
		t.Fatalf("expected cross-inbound totals, got %#v", got[0])
	}
	if got[0].InboundTag != "" {
		t.Fatalf("billing sample must not retain a line/inbound dimension: %#v", got[0])
	}
}

func TestSnapshotHandlerRequiresBearerToken(t *testing.T) {
	store, err := NewStore("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(Snapshot{CollectedAt: time.Now().UTC(), NodeID: "node-1", Env: "uat"}); err != nil {
		t.Fatal(err)
	}
	handler := Handler(store, "secret")
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/snapshots/latest", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", unauthorized.Code)
	}
	authorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/snapshots/latest", nil)
	request.Header.Set("Authorization", "Bearer secret")
	handler.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", authorized.Code)
	}
}

func TestCollectPopulatesEnvelopeAndPreservesThroughStoreAndHTTP(t *testing.T) {
	var receivedAccountsTraceParent string
	accountsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAccountsTraceParent = r.Header.Get("traceparent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"identities":[{"uuid":"proxy-1","email":"user@example.com","accountUuid":"account-1"}]}`))
	}))
	defer accountsSrv.Close()

	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)

	store, err := NewStore("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	accountsClient := NewAccountsClient(accountsSrv.URL, "secret-token").WithTelemetry("uat", "xhttp", metrics)
	service := NewService("uat-xhttp", "uat", fakeSource{
		counters: []RawCounter{
			{Identifier: "account-1", Direction: "uplink", Value: 1024},
		},
	}, accountsClient, store, time.Second).WithTelemetry("xhttp", metrics)

	snap, err := service.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}
	if !snapshotRefRegex.MatchString(snap.SnapshotRef) {
		t.Fatalf("expected 16-char hex snapshot_ref, got %q", snap.SnapshotRef)
	}
	expectedRef := GenerateSnapshotRef("uat", "uat-xhttp", snap.CollectedAt)
	if snap.SnapshotRef != expectedRef {
		t.Fatalf("snapshot_ref mismatch: got %q, want %q", snap.SnapshotRef, expectedRef)
	}
	if !traceParentRegex.MatchString(snap.TraceParent) {
		t.Fatalf("expected valid W3C traceparent, got %q", snap.TraceParent)
	}
	if receivedAccountsTraceParent != snap.TraceParent {
		t.Fatalf("expected Accounts request traceparent %q to match snapshot traceparent %q", receivedAccountsTraceParent, snap.TraceParent)
	}

	// Verify JSON serialization and /v1/snapshots/latest preservation
	handler := Handler(store, "secret-token")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/snapshots/latest", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /v1/snapshots/latest, got %d", rec.Code)
	}

	var latest Snapshot
	if err := json.NewDecoder(rec.Body).Decode(&latest); err != nil {
		t.Fatalf("decode latest snapshot: %v", err)
	}
	if latest.SnapshotRef != snap.SnapshotRef {
		t.Fatalf("expected latest.SnapshotRef=%q, got %q", snap.SnapshotRef, latest.SnapshotRef)
	}
	if latest.TraceParent != snap.TraceParent {
		t.Fatalf("expected latest.TraceParent=%q, got %q", snap.TraceParent, latest.TraceParent)
	}
}

func TestCollectMetricsOutcomesAndLowCardinality(t *testing.T) {
	reg := prometheus.NewRegistry()
	metrics := NewMetrics(reg)
	store, err := NewStore("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// 1. "ok" outcome
	svcOK := NewService("uat-xhttp", "uat", fakeSource{
		counters: []RawCounter{{Identifier: "account-1", Direction: "uplink", Value: 500}},
	}, fakeIdentities{
		"account-1": {AccountUUID: "account-1", Email: "user@example.com"},
	}, store, time.Second).WithTelemetry("xhttp", metrics)
	if _, err := svcOK.Collect(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2. "empty_samples" outcome
	svcEmpty := NewService("uat-xhttp", "uat", fakeSource{counters: nil}, fakeIdentities{}, store, time.Second).WithTelemetry("xhttp", metrics)
	if _, err := svcEmpty.Collect(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 3. "xray_error" outcome
	svcXrayErr := NewService("uat-xhttp", "uat", fakeSource{err: errors.New("grpc down")}, fakeIdentities{}, store, time.Second).WithTelemetry("xhttp", metrics)
	if _, err := svcXrayErr.Collect(); err == nil {
		t.Fatal("expected xray error")
	}

	// 4. "identities_error" outcome
	svcIdErr := NewService("uat-xhttp", "uat", fakeSource{counters: []RawCounter{{Identifier: "account-1", Direction: "uplink", Value: 1}}}, errIdentities{err: errors.New("accounts down")}, store, time.Second).WithTelemetry("xhttp", metrics)
	if _, err := svcIdErr.Collect(); err == nil {
		t.Fatal("expected identities error")
	}

	// 5. "store_error" outcome (unwritable path)
	badStore := &Store{path: "/dev/null/cannot-create-dir/snapshots.json", retention: time.Hour}
	svcStoreErr := NewService("uat-xhttp", "uat", fakeSource{counters: []RawCounter{{Identifier: "account-1", Direction: "uplink", Value: 1}}}, fakeIdentities{"account-1": {AccountUUID: "account-1"}}, badStore, time.Second).WithTelemetry("xhttp", metrics)
	if _, err := svcStoreErr.Collect(); err == nil {
		t.Fatal("expected store error")
	}

	// Gather and verify metric names, outcomes, and strict label privacy/cardinality
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	forbiddenLabels := map[string]bool{
		"uuid":                   true,
		"email":                  true,
		"accountUuid":            true,
		"account_uuid":           true,
		"snapshot_ref":           true,
		"trace_id":               true,
		"traceparent":            true,
		"token":                  true,
		"internal_service_token": true,
	}
	forbiddenValues := []string{
		"account-1",
		"user@example.com",
		"proxy-1",
		"secret-token",
	}

	foundOutcomes := map[string]float64{}
	foundStages := map[string]uint64{}
	var hasLastCollected, hasSamplesCount bool

	for _, fam := range families {
		for _, m := range fam.GetMetric() {
			for _, lp := range m.GetLabel() {
				if forbiddenLabels[lp.GetName()] {
					t.Fatalf("metric %s contains forbidden high-cardinality/sensitive label %q", fam.GetName(), lp.GetName())
				}
				for _, fv := range forbiddenValues {
					if lp.GetValue() == fv {
						t.Fatalf("metric %s label %s contains forbidden sensitive value %q", fam.GetName(), lp.GetName(), fv)
					}
				}
			}
			switch fam.GetName() {
			case "xray_snapshot_collect_total":
				outcome := labelValue(m, "outcome")
				foundOutcomes[outcome] = m.GetCounter().GetValue()
			case "xray_snapshot_collect_duration_seconds":
				stage := labelValue(m, "stage")
				foundStages[stage] = m.GetHistogram().GetSampleCount()
			case "xray_snapshot_last_collected_timestamp_seconds":
				if m.GetGauge().GetValue() > 0 {
					hasLastCollected = true
				}
			case "xray_snapshot_samples_count":
				hasSamplesCount = true
			}
		}
	}

	for _, expectedOutcome := range []string{"ok", "empty_samples", "xray_error", "identities_error", "store_error"} {
		if foundOutcomes[expectedOutcome] != 1 {
			t.Fatalf("expected outcome %q count=1, got %v (all=%v)", expectedOutcome, foundOutcomes[expectedOutcome], foundOutcomes)
		}
	}
	for _, expectedStage := range []string{"xray_counters", "accounts_identities", "normalize_save"} {
		if foundStages[expectedStage] == 0 {
			t.Fatalf("expected stage %q to have observations, got 0", expectedStage)
		}
	}
	if !hasLastCollected {
		t.Fatal("expected xray_snapshot_last_collected_timestamp_seconds > 0")
	}
	if !hasSamplesCount {
		t.Fatal("expected xray_snapshot_samples_count to be recorded")
	}
}

func labelValue(m *dto.Metric, name string) string {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return lp.GetValue()
		}
	}
	return ""
}


package snapshot

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeSource struct {
	counters []RawCounter
}

func (f fakeSource) TrafficCounters(time.Duration) ([]RawCounter, error) {
	return f.counters, nil
}

type fakeIdentities map[string]Identity

func (f fakeIdentities) FetchIdentities(time.Duration) (map[string]Identity, error) {
	return f, nil
}

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

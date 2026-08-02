package snapshotpush

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"xray-exporter/internal/model"
)

func TestPushSnapshotPostsAuthenticatedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/snapshots" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("unexpected authorization header %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("unexpected content type %q", got)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client := New(server.URL+"/snapshots", "secret")
	err := client.PushSnapshot(context.Background(), model.Snapshot{
		CollectedAt: time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC),
		NodeID:      "node-1",
		Env:         "uat",
	})
	if err != nil {
		t.Fatalf("push snapshot: %v", err)
	}
}

func TestPushSnapshotReportsNonSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "billing unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := New(server.URL, "").PushSnapshot(context.Background(), model.Snapshot{})
	if err == nil {
		t.Fatal("expected push error")
	}
}

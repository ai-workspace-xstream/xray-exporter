package snapshot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPusherPostsAuthorizedSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer token" {
			t.Fatalf("unexpected request: %s %s auth=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
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

	pusher := NewPusher(server.URL, "token", time.Second)
	err := pusher.Push(context.Background(), Snapshot{
		CollectedAt: time.Now().UTC(),
		NodeID:      "node-1",
		Env:         "uat",
		Samples:     []Sample{{UUID: "00000000-0000-0000-0000-000000000001"}},
	})
	if err != nil {
		t.Fatalf("push snapshot: %v", err)
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

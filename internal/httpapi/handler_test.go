package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"xray-exporter/internal/model"
	"xray-exporter/internal/service"
)

type handlerCounterSource struct{}

func (handlerCounterSource) FetchCounters(context.Context) ([]model.RawCounter, error) {
	return nil, nil
}

type handlerIdentitySource struct{}

func (handlerIdentitySource) FetchIdentities(context.Context) (map[string]model.Identity, error) {
	return map[string]model.Identity{}, nil
}

type handlerHistory struct {
	snapshots []model.Snapshot
}

func (h *handlerHistory) SaveSnapshot(context.Context, model.Snapshot) error { return nil }

func (h *handlerHistory) LatestSnapshot(context.Context) (model.Snapshot, error) {
	if len(h.snapshots) == 0 {
		return model.Snapshot{}, nil
	}
	return h.snapshots[len(h.snapshots)-1], nil
}

func (h *handlerHistory) WindowSnapshots(_ context.Context, since, until time.Time, limit int, cursor *time.Time) ([]model.Snapshot, error) {
	var results []model.Snapshot
	for _, snapshot := range h.snapshots {
		if snapshot.CollectedAt.Before(since) || snapshot.CollectedAt.After(until) {
			continue
		}
		if cursor != nil && !snapshot.CollectedAt.After(cursor.UTC()) {
			continue
		}
		results = append(results, snapshot)
		if len(results) == limit {
			break
		}
	}
	return results, nil
}

func TestSnapshotEndpointsRequireBearerAuth(t *testing.T) {
	history := &handlerHistory{}
	svc := service.New("jp-node", "prod", time.Minute, handlerCounterSource{}, handlerIdentitySource{}, history)
	handler := New(svc, "secret").Routes()

	for _, path := range []string{"/v1/snapshots/latest", "/v1/snapshots/window?since=2026-04-12T00:00:00Z"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected unauthorized for %s, got %d", path, rec.Code)
		}
	}
}

func TestSnapshotWindowSupportsPagination(t *testing.T) {
	history := &handlerHistory{
		snapshots: []model.Snapshot{
			{CollectedAt: time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC), NodeID: "jp-node", Env: "prod"},
			{CollectedAt: time.Date(2026, 4, 12, 10, 1, 0, 0, time.UTC), NodeID: "jp-node", Env: "prod"},
			{CollectedAt: time.Date(2026, 4, 12, 10, 2, 0, 0, time.UTC), NodeID: "jp-node", Env: "prod"},
		},
	}
	svc := service.New("jp-node", "prod", time.Minute, handlerCounterSource{}, handlerIdentitySource{}, history)
	handler := New(svc, "secret").Routes()

	req := httptest.NewRequest(http.MethodGet, "/v1/snapshots/window?since=2026-04-12T10:00:00Z&until=2026-04-12T10:02:00Z&limit=2", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var page model.SnapshotWindowPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if len(page.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(page.Snapshots))
	}
	if !page.HasMore {
		t.Fatalf("expected has_more true")
	}
	if page.NextCursor != "2026-04-12T10:01:00Z" {
		t.Fatalf("unexpected next cursor %q", page.NextCursor)
	}
}

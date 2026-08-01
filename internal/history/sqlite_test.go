package history

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"xray-exporter/internal/model"
)

func TestSQLiteStorePersistsWindowsAndPrunesByRetention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshots.db")
	store, err := NewSQLiteStore(path, time.Minute)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	first := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	third := second.Add(time.Minute)
	for _, snapshot := range []model.Snapshot{
		{CollectedAt: first, NodeID: "uat-node", Env: "uat"},
		{CollectedAt: second, NodeID: "uat-node", Env: "uat"},
		{CollectedAt: third, NodeID: "uat-node", Env: "uat"},
	} {
		if err := store.SaveSnapshot(context.Background(), snapshot); err != nil {
			t.Fatalf("save snapshot: %v", err)
		}
	}

	window, err := store.WindowSnapshots(context.Background(), first, third, 10, nil)
	if err != nil {
		t.Fatalf("read window: %v", err)
	}
	if len(window) != 2 || !window[0].CollectedAt.Equal(second) || !window[1].CollectedAt.Equal(third) {
		t.Fatalf("expected retention to keep second and third snapshots, got %#v", window)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	mode, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat store: %v", err)
	}
	if mode.Mode().Perm() != 0o600 {
		t.Fatalf("expected private snapshot file mode 0600, got %o", mode.Mode().Perm())
	}

	reopened, err := NewSQLiteStore(path, time.Minute)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	latest, err := reopened.LatestSnapshot(context.Background())
	if err != nil {
		t.Fatalf("read latest after reopen: %v", err)
	}
	if !latest.CollectedAt.Equal(third) {
		t.Fatalf("expected latest snapshot %s, got %s", third, latest.CollectedAt)
	}
}

func TestSQLiteStoreRejectsInvalidWindows(t *testing.T) {
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "snapshots.db"), time.Hour)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	first := time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	if _, err := store.WindowSnapshots(context.Background(), first, first.Add(-time.Minute), 10, nil); err == nil {
		t.Fatal("expected reversed window to fail")
	}
}

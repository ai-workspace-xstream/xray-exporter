package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Store struct {
	path      string
	retention time.Duration
	mu        sync.RWMutex
	items     []Snapshot
}

func NewStore(path string, retention time.Duration) (*Store, error) {
	s := &Store{path: path, retention: retention}
	if path == "" {
		return s, nil
	}
	payload, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot store: %w", err)
	}
	if len(payload) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(payload, &s.items); err != nil {
		return nil, fmt.Errorf("decode snapshot store: %w", err)
	}
	sortSnapshots(s.items)
	return s, nil
}

func (s *Store) Save(snapshot Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, snapshot)
	cutoff := snapshot.CollectedAt.Add(-s.retention)
	kept := s.items[:0]
	for _, item := range s.items {
		if s.retention <= 0 || !item.CollectedAt.Before(cutoff) {
			kept = append(kept, item)
		}
	}
	s.items = kept
	sortSnapshots(s.items)
	return s.persistLocked()
}

func (s *Store) Latest() (Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.items) == 0 {
		return Snapshot{}, false
	}
	return cloneSnapshot(s.items[len(s.items)-1]), true
}

func (s *Store) Window(since, until time.Time, limit int, cursor *time.Time) (WindowPage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	page := WindowPage{Snapshots: make([]Snapshot, 0)}
	start := since.UTC()
	end := until.UTC()
	for _, item := range s.items {
		if item.CollectedAt.Before(start) || item.CollectedAt.After(end) {
			continue
		}
		if cursor != nil && !item.CollectedAt.After(cursor.UTC()) {
			continue
		}
		page.Snapshots = append(page.Snapshots, cloneSnapshot(item))
		if limit > 0 && len(page.Snapshots) == limit {
			break
		}
	}
	if limit > 0 {
		for _, item := range s.items {
			if item.CollectedAt.Before(start) || item.CollectedAt.After(end) {
				continue
			}
			if cursor != nil && !item.CollectedAt.After(cursor.UTC()) {
				continue
			}
			if len(page.Snapshots) > 0 && item.CollectedAt.After(page.Snapshots[len(page.Snapshots)-1].CollectedAt) {
				page.HasMore = true
				page.NextCursor = page.Snapshots[len(page.Snapshots)-1].CollectedAt.UTC().Format(time.RFC3339Nano)
				break
			}
		}
	}
	if len(page.Snapshots) > 0 {
		page.NodeID = page.Snapshots[0].NodeID
		page.Env = page.Snapshots[0].Env
	}
	return page, true
}

func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0750); err != nil {
		return fmt.Errorf("create snapshot store directory: %w", err)
	}
	payload, err := json.Marshal(s.items)
	if err != nil {
		return fmt.Errorf("encode snapshot store: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".snapshots-*.tmp")
	if err != nil {
		return fmt.Errorf("create snapshot store temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod snapshot store: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write snapshot store: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close snapshot store: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replace snapshot store: %w", err)
	}
	return nil
}

func sortSnapshots(items []Snapshot) {
	sort.Slice(items, func(i, j int) bool { return items[i].CollectedAt.Before(items[j].CollectedAt) })
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	snapshot.Samples = append([]Sample(nil), snapshot.Samples...)
	return snapshot
}

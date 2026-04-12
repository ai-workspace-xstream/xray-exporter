package history

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"xray-exporter/internal/model"

	_ "modernc.org/sqlite"
)

type SQLiteStore struct {
	db        *sql.DB
	retention time.Duration
}

func NewSQLiteStore(path string, retention time.Duration) (*SQLiteStore, error) {
	if retention <= 0 {
		return nil, fmt.Errorf("retention must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create snapshot store directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store: %w", err)
	}

	store := &SQLiteStore{db: db, retention: retention}
	if err := store.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) SaveSnapshot(ctx context.Context, snapshot model.Snapshot) error {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO snapshots (collected_at, payload)
		VALUES (?, ?)
		ON CONFLICT(collected_at) DO UPDATE SET payload = excluded.payload
	`, snapshot.CollectedAt.UTC().Format(time.RFC3339Nano), payload); err != nil {
		return fmt.Errorf("insert snapshot: %w", err)
	}

	cutoff := snapshot.CollectedAt.UTC().Add(-s.retention)
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM snapshots
		WHERE collected_at < ?
	`, cutoff.Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("prune snapshots: %w", err)
	}
	return nil
}

func (s *SQLiteStore) LatestSnapshot(ctx context.Context) (model.Snapshot, error) {
	var payload []byte
	if err := s.db.QueryRowContext(ctx, `
		SELECT payload
		FROM snapshots
		ORDER BY collected_at DESC
		LIMIT 1
	`).Scan(&payload); err != nil {
		if err == sql.ErrNoRows {
			return model.Snapshot{}, nil
		}
		return model.Snapshot{}, fmt.Errorf("query latest snapshot: %w", err)
	}
	return decodeSnapshot(payload)
}

func (s *SQLiteStore) WindowSnapshots(ctx context.Context, since, until time.Time, limit int, cursor *time.Time) ([]model.Snapshot, error) {
	if limit <= 0 {
		limit = 1
	}

	args := []any{
		since.UTC().Format(time.RFC3339Nano),
		until.UTC().Format(time.RFC3339Nano),
	}
	query := `
		SELECT payload
		FROM snapshots
		WHERE collected_at >= ?
		  AND collected_at <= ?
	`
	if cursor != nil {
		query += " AND collected_at > ?"
		args = append(args, cursor.UTC().Format(time.RFC3339Nano))
	}
	query += " ORDER BY collected_at ASC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query snapshots window: %w", err)
	}
	defer rows.Close()

	var snapshots []model.Snapshot
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan snapshot payload: %w", err)
		}
		snapshot, err := decodeSnapshot(payload)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot rows: %w", err)
	}
	return snapshots, nil
}

func (s *SQLiteStore) init(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS snapshots (
			collected_at TEXT PRIMARY KEY,
			payload BLOB NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create snapshots table: %w", err)
	}
	return nil
}

func decodeSnapshot(payload []byte) (model.Snapshot, error) {
	var snapshot model.Snapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return model.Snapshot{}, fmt.Errorf("decode snapshot payload: %w", err)
	}
	return snapshot, nil
}

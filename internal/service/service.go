package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"xray-exporter/internal/model"
)

type counterSource interface {
	FetchCounters(ctx context.Context) ([]model.RawCounter, error)
}

type identitySource interface {
	FetchIdentities(ctx context.Context) (map[string]model.Identity, error)
}

type historyStore interface {
	SaveSnapshot(ctx context.Context, snapshot model.Snapshot) error
	LatestSnapshot(ctx context.Context) (model.Snapshot, error)
	WindowSnapshots(ctx context.Context, since, until time.Time, limit int, cursor *time.Time) ([]model.Snapshot, error)
}

type Service struct {
	nodeID         string
	env            string
	scrapeInterval time.Duration
	counters       counterSource
	identities     identitySource
	history        historyStore

	mu              sync.RWMutex
	latest          model.Snapshot
	lastError       string
	lastSuccess     time.Time
	lastCollectTime time.Time
	lastCollectOK   bool
}

func New(nodeID, env string, scrapeInterval time.Duration, counters counterSource, identities identitySource, history historyStore) *Service {
	return &Service{
		nodeID:         strings.TrimSpace(nodeID),
		env:            strings.TrimSpace(env),
		scrapeInterval: scrapeInterval,
		counters:       counters,
		identities:     identities,
		history:        history,
	}
}

func (s *Service) Start(ctx context.Context) {
	go func() {
		s.Collect(ctx)
		ticker := time.NewTicker(s.scrapeInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.Collect(ctx)
			}
		}
	}()
}

func (s *Service) Collect(ctx context.Context) error {
	s.mu.Lock()
	s.lastCollectTime = time.Now().UTC()
	s.mu.Unlock()

	counters, err := s.counters.FetchCounters(ctx)
	if err != nil {
		s.recordFailure(err)
		return err
	}

	identityMap, identitiesErr := s.identities.FetchIdentities(ctx)
	if identitiesErr != nil {
		identityMap = map[string]model.Identity{}
	}

	collectedAt := time.Now().UTC().Truncate(time.Minute)
	snapshot := normalizeSnapshot(s.nodeID, s.env, collectedAt, counters, identityMap)
	if err := s.history.SaveSnapshot(ctx, snapshot); err != nil {
		s.recordFailure(err)
		return err
	}
	if identitiesErr != nil {
		s.recordSuccess(snapshot, fmt.Sprintf("identity lookup degraded: %v", identitiesErr))
		return nil
	}

	s.recordSuccess(snapshot, "")
	return nil
}

func (s *Service) Snapshot() model.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneSnapshot(s.latest)
}

func (s *Service) SnapshotJSON() ([]byte, error) {
	return json.Marshal(s.Snapshot())
}

func (s *Service) LatestSnapshot(ctx context.Context) (model.Snapshot, error) {
	snapshot := s.Snapshot()
	if !snapshot.CollectedAt.IsZero() {
		return snapshot, nil
	}
	return s.history.LatestSnapshot(ctx)
}

func (s *Service) SnapshotWindow(ctx context.Context, since, until time.Time, limit int, cursor *time.Time) (model.SnapshotWindowPage, error) {
	if limit <= 0 {
		limit = 120
	}

	snapshots, err := s.history.WindowSnapshots(ctx, since.UTC(), until.UTC(), limit+1, cursor)
	if err != nil {
		return model.SnapshotWindowPage{}, err
	}

	page := model.SnapshotWindowPage{
		NodeID: s.nodeID,
		Env:    s.env,
	}
	if len(snapshots) > limit {
		page.HasMore = true
		page.Snapshots = append(page.Snapshots, snapshots[:limit]...)
		page.NextCursor = page.Snapshots[len(page.Snapshots)-1].CollectedAt.UTC().Format(time.RFC3339)
		return page, nil
	}

	page.Snapshots = append(page.Snapshots, snapshots...)
	return page, nil
}

func (s *Service) Health() (bool, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.lastCollectOK {
		return false, s.lastError
	}
	return true, s.lastError
}

func (s *Service) MetricsText() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var b strings.Builder
	b.WriteString("# HELP xray_user_uplink_bytes Raw cumulative uplink bytes by user identity.\n")
	b.WriteString("# TYPE xray_user_uplink_bytes gauge\n")
	for _, sample := range s.latest.Samples {
		b.WriteString(fmt.Sprintf(
			"xray_user_uplink_bytes{uuid=%q,email=%q,node_id=%q,env=%q,inbound_tag=%q} %d\n",
			escapeLabel(sample.UUID),
			escapeLabel(sample.Email),
			escapeLabel(s.latest.NodeID),
			escapeLabel(s.latest.Env),
			escapeLabel(sample.InboundTag),
			sample.UplinkBytesTotal,
		))
	}
	b.WriteString("# HELP xray_user_downlink_bytes Raw cumulative downlink bytes by user identity.\n")
	b.WriteString("# TYPE xray_user_downlink_bytes gauge\n")
	for _, sample := range s.latest.Samples {
		b.WriteString(fmt.Sprintf(
			"xray_user_downlink_bytes{uuid=%q,email=%q,node_id=%q,env=%q,inbound_tag=%q} %d\n",
			escapeLabel(sample.UUID),
			escapeLabel(sample.Email),
			escapeLabel(s.latest.NodeID),
			escapeLabel(s.latest.Env),
			escapeLabel(sample.InboundTag),
			sample.DownlinkBytesTotal,
		))
	}
	b.WriteString("# HELP xray_exporter_collect_success Whether the latest collection succeeded.\n")
	b.WriteString("# TYPE xray_exporter_collect_success gauge\n")
	successValue := 0
	if s.lastCollectOK {
		successValue = 1
	}
	nodeID := s.latest.NodeID
	if nodeID == "" {
		nodeID = s.nodeID
	}
	env := s.latest.Env
	if env == "" {
		env = s.env
	}
	b.WriteString(fmt.Sprintf(
		"xray_exporter_collect_success{node_id=%q,env=%q} %d\n",
		escapeLabel(nodeID),
		escapeLabel(env),
		successValue,
	))
	b.WriteString("# HELP xray_exporter_collect_timestamp_seconds Unix timestamp of the latest successful collection.\n")
	b.WriteString("# TYPE xray_exporter_collect_timestamp_seconds gauge\n")
	timestamp := int64(0)
	if !s.lastSuccess.IsZero() {
		timestamp = s.lastSuccess.Unix()
	}
	b.WriteString(fmt.Sprintf(
		"xray_exporter_collect_timestamp_seconds{node_id=%q,env=%q} %d\n",
		escapeLabel(nodeID),
		escapeLabel(env),
		timestamp,
	))
	return b.String()
}

func (s *Service) recordFailure(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastCollectOK = false
	s.lastError = err.Error()
}

func (s *Service) recordSuccess(snapshot model.Snapshot, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latest = cloneSnapshot(snapshot)
	s.lastCollectOK = true
	s.lastSuccess = snapshot.CollectedAt
	s.lastError = message
}

func normalizeSnapshot(nodeID, env string, collectedAt time.Time, counters []model.RawCounter, identities map[string]model.Identity) model.Snapshot {
	type aggregate struct {
		uuid       string
		inboundTag string
		uplink     int64
		downlink   int64
	}

	aggregates := map[string]*aggregate{}
	for _, counter := range counters {
		uuid := strings.TrimSpace(counter.UUID)
		if uuid == "" {
			continue
		}
		key := uuid + "\x00" + strings.TrimSpace(counter.InboundTag)
		entry, ok := aggregates[key]
		if !ok {
			entry = &aggregate{uuid: uuid, inboundTag: strings.TrimSpace(counter.InboundTag)}
			aggregates[key] = entry
		}
		switch strings.TrimSpace(counter.Direction) {
		case "uplink":
			entry.uplink = counter.Value
		case "downlink":
			entry.downlink = counter.Value
		}
	}

	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	samples := make([]model.Sample, 0, len(keys))
	for _, key := range keys {
		entry := aggregates[key]
		email := "unknown"
		if identity, ok := identities[entry.uuid]; ok && strings.TrimSpace(identity.Email) != "" {
			email = strings.TrimSpace(identity.Email)
		}
		samples = append(samples, model.Sample{
			UUID:               entry.uuid,
			Email:              email,
			InboundTag:         entry.inboundTag,
			UplinkBytesTotal:   entry.uplink,
			DownlinkBytesTotal: entry.downlink,
		})
	}

	return model.Snapshot{
		CollectedAt: collectedAt.UTC(),
		NodeID:      strings.TrimSpace(nodeID),
		Env:         strings.TrimSpace(env),
		Samples:     samples,
	}
}

func cloneSnapshot(snapshot model.Snapshot) model.Snapshot {
	cloned := model.Snapshot{
		CollectedAt: snapshot.CollectedAt,
		NodeID:      snapshot.NodeID,
		Env:         snapshot.Env,
		Samples:     make([]model.Sample, len(snapshot.Samples)),
	}
	copy(cloned.Samples, snapshot.Samples)
	return cloned
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

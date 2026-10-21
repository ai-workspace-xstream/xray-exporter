package snapshot

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type contextualIdentitySource interface {
	FetchIdentitiesWithContext(ctx context.Context, timeout time.Duration) (map[string]Identity, error)
}

type Service struct {
	nodeID     string
	env        string
	transport  string
	source     CounterSource
	identities IdentitySource
	store      *Store
	timeout    time.Duration
	metrics    *Metrics
}

func NewService(nodeID, env string, source CounterSource, identities IdentitySource, store *Store, timeout time.Duration) *Service {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	trimmedNode := strings.TrimSpace(nodeID)
	trimmedEnv := strings.TrimSpace(env)
	transport := DeriveTransport(trimmedNode, trimmedEnv)
	metrics := DefaultMetrics()
	if ac, ok := identities.(*AccountsClient); ok && ac != nil {
		ac.WithTelemetry(trimmedEnv, transport, metrics)
	}
	return &Service{
		nodeID:     trimmedNode,
		env:        trimmedEnv,
		transport:  transport,
		source:     source,
		identities: identities,
		store:      store,
		timeout:    timeout,
		metrics:    metrics,
	}
}

func (s *Service) WithTelemetry(transport string, metrics *Metrics) *Service {
	if s == nil {
		return nil
	}
	if strings.TrimSpace(transport) != "" {
		s.transport = strings.TrimSpace(transport)
	}
	if metrics != nil {
		s.metrics = metrics
	}
	if ac, ok := s.identities.(*AccountsClient); ok && ac != nil {
		ac.WithTelemetry(s.env, s.transport, s.metrics)
	}
	return s
}

func (s *Service) Collect() (Snapshot, error) {
	if s == nil || s.source == nil || s.store == nil {
		return Snapshot{}, fmt.Errorf("snapshot service is not configured")
	}
	metrics := s.metrics
	if metrics == nil {
		metrics = DefaultMetrics()
	}
	transport := sanitizeTransport(s.transport, s.nodeID, s.env)
	traceParent := GenerateTraceParent()
	ctx := WithTraceParent(context.Background(), traceParent)

	xrayStart := time.Now()
	counters, err := s.source.TrafficCounters(s.timeout)
	metrics.ObserveCollectStage(s.env, transport, "xray_counters", time.Since(xrayStart))
	if err != nil {
		metrics.IncCollectOutcome(s.env, transport, "xray_error")
		return Snapshot{}, err
	}

	identities := map[string]Identity{}
	if s.identities != nil {
		idStart := time.Now()
		if ctxSource, ok := s.identities.(contextualIdentitySource); ok {
			identities, err = ctxSource.FetchIdentitiesWithContext(ctx, s.timeout)
		} else {
			identities, err = s.identities.FetchIdentities(s.timeout)
		}
		metrics.ObserveCollectStage(s.env, transport, "accounts_identities", time.Since(idStart))
		if err != nil {
			metrics.IncCollectOutcome(s.env, transport, "identities_error")
			return Snapshot{}, err
		}
	}

	normStart := time.Now()
	collectedAt := time.Now().UTC()
	snapshot := Snapshot{
		CollectedAt: collectedAt,
		NodeID:      s.nodeID,
		Env:         s.env,
		SnapshotRef: GenerateSnapshotRef(s.env, s.nodeID, collectedAt),
		TraceParent: traceParent,
	}
	snapshot.Samples = normalize(counters, identities)
	err = s.store.Save(snapshot)
	metrics.ObserveCollectStage(s.env, transport, "normalize_save", time.Since(normStart))
	if err != nil {
		metrics.IncCollectOutcome(s.env, transport, "store_error")
		return Snapshot{}, err
	}

	metrics.RecordCollectSuccess(s.env, transport, collectedAt, len(snapshot.Samples))
	if len(snapshot.Samples) == 0 {
		metrics.IncCollectOutcome(s.env, transport, "empty_samples")
	} else {
		metrics.IncCollectOutcome(s.env, transport, "ok")
	}
	return snapshot, nil
}

func normalize(counters []RawCounter, identities map[string]Identity) []Sample {
	type aggregate struct {
		uuid, email      string
		uplink, downlink int64
	}
	aggregates := map[string]*aggregate{}
	for _, counter := range counters {
		identifier := strings.TrimSpace(counter.Identifier)
		if identifier == "" {
			continue
		}
		identity, ok := identities[strings.ToLower(identifier)]
		if !ok && strings.Contains(identifier, "@") {
			// An email without a canonical Accounts identity must not reach the
			// UUID-backed Billing tables.
			continue
		}
		uuid := identifier
		email := ""
		if ok {
			if identity.AccountUUID != "" {
				uuid = identity.AccountUUID
			} else if identity.ProxyUUID != "" {
				uuid = identity.ProxyUUID
			}
			email = identity.Email
		}
		if uuid == "" {
			continue
		}
		// Billing is account based. A user can appear on multiple inbounds on
		// one or many nodes; collapse all inbound counters into one UUID sample
		// before the snapshot leaves the exporter.
		key := uuid
		entry := aggregates[key]
		if entry == nil {
			entry = &aggregate{uuid: uuid, email: email}
			aggregates[key] = entry
		}
		switch strings.TrimSpace(counter.Direction) {
		case "uplink":
			entry.uplink += counter.Value
		case "downlink":
			entry.downlink += counter.Value
		}
	}
	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]Sample, 0, len(keys))
	for _, key := range keys {
		entry := aggregates[key]
		result = append(result, Sample{UUID: entry.uuid, Email: entry.email, UplinkBytesTotal: entry.uplink, DownlinkBytesTotal: entry.downlink})
	}
	return result
}

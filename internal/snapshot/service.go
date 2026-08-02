package snapshot

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Service struct {
	nodeID     string
	env        string
	source     CounterSource
	identities IdentitySource
	store      *Store
	timeout    time.Duration
}

func NewService(nodeID, env string, source CounterSource, identities IdentitySource, store *Store, timeout time.Duration) *Service {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Service{nodeID: strings.TrimSpace(nodeID), env: strings.TrimSpace(env), source: source, identities: identities, store: store, timeout: timeout}
}

func (s *Service) Collect() (Snapshot, error) {
	if s.source == nil || s.store == nil {
		return Snapshot{}, fmt.Errorf("snapshot service is not configured")
	}
	counters, err := s.source.TrafficCounters(s.timeout)
	if err != nil {
		return Snapshot{}, err
	}
	identities := map[string]Identity{}
	if s.identities != nil {
		identities, err = s.identities.FetchIdentities(s.timeout)
		if err != nil {
			return Snapshot{}, err
		}
	}
	snapshot := Snapshot{CollectedAt: time.Now().UTC(), NodeID: s.nodeID, Env: s.env}
	snapshot.Samples = normalize(counters, identities)
	if err := s.store.Save(snapshot); err != nil {
		return Snapshot{}, err
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

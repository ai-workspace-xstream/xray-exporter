package model

import "time"

type Sample struct {
	UUID               string `json:"uuid"`
	Email              string `json:"email"`
	InboundTag         string `json:"inbound_tag"`
	UplinkBytesTotal   int64  `json:"uplink_bytes_total"`
	DownlinkBytesTotal int64  `json:"downlink_bytes_total"`
}

type Snapshot struct {
	CollectedAt time.Time `json:"collected_at"`
	NodeID      string    `json:"node_id"`
	Env         string    `json:"env"`
	Samples     []Sample  `json:"samples"`
}

type SnapshotWindowPage struct {
	NodeID     string     `json:"node_id"`
	Env        string     `json:"env"`
	Snapshots  []Snapshot `json:"snapshots"`
	HasMore    bool       `json:"has_more"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

type RawCounter struct {
	UUID       string
	InboundTag string
	Direction  string
	Value      int64
}

type Identity struct {
	UUID        string `json:"uuid"`
	Email       string `json:"email"`
	AccountUUID string `json:"accountUuid"`
}

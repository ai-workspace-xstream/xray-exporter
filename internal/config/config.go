package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	XrayStatsURL         string
	XrayStatsToken       string
	AccountsBaseURL      string
	InternalServiceToken string
	SnapshotStorePath    string
	SnapshotRetention    time.Duration
	NodeID               string
	Env                  string
	ScrapeInterval       time.Duration
	ListenAddr           string
}

func Load() (Config, error) {
	cfg := Config{
		XrayStatsURL:         strings.TrimSpace(os.Getenv("XRAY_STATS_URL")),
		XrayStatsToken:       strings.TrimSpace(os.Getenv("XRAY_STATS_TOKEN")),
		AccountsBaseURL:      strings.TrimSpace(os.Getenv("ACCOUNTS_BASE_URL")),
		InternalServiceToken: strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_TOKEN")),
		SnapshotStorePath:    strings.TrimSpace(os.Getenv("SNAPSHOT_STORE_PATH")),
		NodeID:               strings.TrimSpace(os.Getenv("EXPORTER_NODE_ID")),
		Env:                  strings.TrimSpace(os.Getenv("EXPORTER_ENV")),
		ListenAddr:           strings.TrimSpace(os.Getenv("LISTEN_ADDR")),
	}

	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.Env == "" {
		cfg.Env = "prod"
	}
	if cfg.SnapshotStorePath == "" {
		cfg.SnapshotStorePath = "/var/lib/xray-exporter/snapshots.db"
	}

	interval := strings.TrimSpace(os.Getenv("SCRAPE_INTERVAL"))
	if interval == "" {
		cfg.ScrapeInterval = time.Minute
	} else {
		parsed, err := time.ParseDuration(interval)
		if err != nil {
			return Config{}, fmt.Errorf("parse SCRAPE_INTERVAL: %w", err)
		}
		cfg.ScrapeInterval = parsed
	}

	retention := strings.TrimSpace(os.Getenv("SNAPSHOT_RETENTION"))
	if retention == "" {
		cfg.SnapshotRetention = 72 * time.Hour
	} else {
		parsed, err := time.ParseDuration(retention)
		if err != nil {
			return Config{}, fmt.Errorf("parse SNAPSHOT_RETENTION: %w", err)
		}
		cfg.SnapshotRetention = parsed
	}

	switch {
	case cfg.XrayStatsURL == "":
		return Config{}, fmt.Errorf("XRAY_STATS_URL is required")
	case cfg.AccountsBaseURL == "":
		return Config{}, fmt.Errorf("ACCOUNTS_BASE_URL is required")
	case cfg.InternalServiceToken == "":
		return Config{}, fmt.Errorf("INTERNAL_SERVICE_TOKEN is required")
	case cfg.NodeID == "":
		return Config{}, fmt.Errorf("EXPORTER_NODE_ID is required")
	}

	return cfg, nil
}

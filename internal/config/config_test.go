package config

import "testing"

func TestLoadReadsVectorSnapshotURL(t *testing.T) {
	t.Setenv("XRAY_STATS_URL", "http://127.0.0.1:28080/debug/vars")
	t.Setenv("ACCOUNTS_BASE_URL", "https://accounts-uat.example.test")
	t.Setenv("INTERNAL_SERVICE_TOKEN", "test-token")
	t.Setenv("EXPORTER_NODE_ID", "agent-1")
	t.Setenv("VECTOR_SNAPSHOT_URL", "http://127.0.0.1:8686")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.VectorSnapshotURL != "http://127.0.0.1:8686" {
		t.Fatalf("unexpected vector snapshot URL %q", cfg.VectorSnapshotURL)
	}
}

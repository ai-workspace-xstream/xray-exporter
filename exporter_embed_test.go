package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"xray-exporter/internal/snapshot"
	"xray-exporter/internal/xrayembed"
)

func TestParseCLIInstanceSeparation(t *testing.T) {
	t.Run("gateway run defaults disable HTTP and snapshot loops", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		cfg, err := parseCLI(
			[]string{"xray", "run", "-config", "/var/lib/xconnect-gateway/runtime/xray.json"},
			func(string) (string, bool) { return "", false },
			&stdout,
			&stderr,
		)
		if err != nil {
			t.Fatalf("parseCLI failed: %v", err)
		}
		if !cfg.Embedded || !cfg.RunSubcommand {
			t.Fatalf("expected Embedded=true RunSubcommand=true, got %+v", cfg)
		}
		if cfg.XRayConfigPath != "/var/lib/xconnect-gateway/runtime/xray.json" {
			t.Fatalf("unexpected config path %q", cfg.XRayConfigPath)
		}
		if cfg.Listen != "" {
			t.Fatalf("expected empty Listen in bare run mode, got %q", cfg.Listen)
		}
		if cfg.snapshotsEnabled() {
			t.Fatalf("expected snapshots disabled in bare gateway run mode")
		}
	})

	t.Run("proxy run enables per-instance listen, user metrics, and vector push", func(t *testing.T) {
		env := map[string]string{
			"EXPORTER_ENV":           "prod",
			"ACCOUNTS_BASE_URL":      "https://accounts.svc.plus",
			"INTERNAL_SERVICE_TOKEN": "tok-test",
			"VECTOR_SNAPSHOT_URL":    "http://127.0.0.1:8686",
		}
		getenv := func(k string) (string, bool) {
			v, ok := env[k]
			return v, ok
		}
		var stdout, stderr bytes.Buffer
		cfg, err := parseCLI(
			[]string{
				"xray", "run",
				"-config", "/usr/local/etc/xray/config.json",
				"-listen", "127.0.0.1:8080",
				"-user-traffic-metrics",
				"-log-path", "disabled",
				"-node-id", "jp-xconnect-02.svc.plus-xhttp",
				"-snapshot-store-path", "/var/lib/xray-exporter/xhttp-snapshots.json",
			},
			getenv,
			&stdout,
			&stderr,
		)
		if err != nil {
			t.Fatalf("parseCLI failed: %v", err)
		}
		if cfg.Listen != "127.0.0.1:8080" || cfg.NodeID != "jp-xconnect-02.svc.plus-xhttp" {
			t.Fatalf("unexpected listen/node-id: %+v", cfg)
		}
		if cfg.VectorSnapshotURL != "http://127.0.0.1:8686" || !cfg.UserTrafficMetrics || !cfg.snapshotsEnabled() {
			t.Fatalf("expected telemetry enabled in proxy run mode: %+v", cfg)
		}
		if cfg.LogPath != "disabled" {
			t.Fatalf("expected LogPath=disabled, got %q", cfg.LogPath)
		}
	})
}

func TestEmbeddedEngineScrapeAndVectorPush(t *testing.T) {
	// Start local loopback echo backend for Xray outbound.
	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	defer echoLn.Close()
	go func() {
		for {
			conn, acceptErr := echoLn.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()

	// Reserve free loopback port for embedded Xray dokodemo/socks inbound.
	probeLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	inboundPort := probeLn.Addr().(*net.TCPAddr).Port
	_ = probeLn.Close()

	_, echoPortStr, _ := net.SplitHostPort(echoLn.Addr().String())
	cfgPath := filepath.Join(t.TempDir(), "xray.json")
	cfgJSON := fmt.Sprintf(`{
  "log": {"loglevel": "warning"},
  "stats": {},
  "policy": {
    "levels": {"0": {"statsUserUplink": true, "statsUserDownlink": true}},
    "system": {"statsInboundUplink": true, "statsInboundDownlink": true, "statsOutboundUplink": true, "statsOutboundDownlink": true}
  },
  "inbounds": [{
    "tag": "dokodemo-in",
    "listen": "127.0.0.1",
    "port": %d,
    "protocol": "dokodemo-door",
    "settings": {"address": "127.0.0.1", "port": %s, "network": "tcp", "userLevel": 0},
    "sniffing": {"enabled": false}
  }],
  "outbounds": [{"tag": "direct", "protocol": "freedom"}]
}`, inboundPort, echoPortStr)
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	engine, err := xrayembed.NewEngine(cfgPath)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if err := engine.Start(); err != nil {
		t.Fatalf("engine.Start: %v", err)
	}
	defer engine.Close()

	// Seed per-user traffic counter directly inside the embedded StatsManager alongside real TCP echo traffic.
	reader := newInprocStatsReader(engine, false)
	exp, err := NewExporterWithStatsReader(reader, 2*time.Second, true, "disabled", 5*time.Minute)
	if err != nil {
		t.Fatalf("NewExporterWithStatsReader: %v", err)
	}
	defer exp.Close()

	// Send real TCP bytes through embedded Xray-core inbound -> outbound.
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", inboundPort), 2*time.Second)
	if err != nil {
		t.Fatalf("dial embedded inbound: %v", err)
	}
	payload := []byte("hello-embedded-xray")
	if _, err := conn.Write(payload); err != nil {
		_ = conn.Close()
		t.Fatalf("write payload: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, buf); err != nil {
		_ = conn.Close()
		t.Fatalf("read echo: %v", err)
	}
	_ = conn.Close()

	// Verify /scrape HTTP handler reports xray_up 1 and inbound/outbound traffic metrics directly from memory.
	scrapeRec := httptest.NewRecorder()
	scrapeHandler(exp)(scrapeRec, httptest.NewRequest(http.MethodGet, "/scrape", nil))
	if scrapeRec.Code != http.StatusOK {
		t.Fatalf("scrape status=%d body=%s", scrapeRec.Code, scrapeRec.Body.String())
	}
	body := scrapeRec.Body.String()
	for _, want := range []string{
		"xray_up 1",
		`xray_traffic_uplink_bytes_total{dimension="inbound",target="dokodemo-in"}`,
		`xray_traffic_downlink_bytes_total{dimension="outbound",target="direct"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected /scrape output to contain %q, got:\n%s", want, body)
		}
	}

	// Verify snapshot collection + push to mock Vector 8686 works end-to-end.
	var pushed snapshot.Snapshot
	var authHeader string
	vectorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&pushed); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer vectorSrv.Close()

	acctSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/internal/network/identities" {
			_ = json.NewEncoder(w).Encode(map[string]any{"identities": []any{}})
			return
		}
		http.NotFound(w, r)
	}))
	defer acctSrv.Close()

	storePath := filepath.Join(t.TempDir(), "snapshots.json")
	snapStore, err := snapshot.NewStore(storePath, time.Hour)
	if err != nil {
		t.Fatalf("snapshot.NewStore: %v", err)
	}
	snapSvc := snapshot.NewService(
		"jp-xconnect-02.svc.plus-xhttp",
		"prod",
		exp,
		snapshot.NewAccountsClient(acctSrv.URL, "svc-tok"),
		snapStore,
		2*time.Second,
	)
	snapVal, err := snapSvc.Collect()
	if err != nil {
		t.Fatalf("snapSvc.Collect: %v", err)
	}
	pusher := snapshot.NewPusher(vectorSrv.URL, "svc-tok", 2*time.Second)
	if err := pusher.Push(context.Background(), snapVal); err != nil {
		t.Fatalf("pusher.Push: %v", err)
	}
	if authHeader != "Bearer svc-tok" {
		t.Fatalf("expected Vector push bearer token, got %q", authHeader)
	}
	if pushed.NodeID != "jp-xconnect-02.svc.plus-xhttp" || pushed.Env != "prod" {
		t.Fatalf("unexpected pushed snapshot metadata: %+v", pushed)
	}
}

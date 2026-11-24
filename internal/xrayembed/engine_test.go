package xrayembed

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/stats/command"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
)

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate free tcp port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func writeTestServerConfig(t *testing.T, path, listen string, port int, users [][2]string) {
	t.Helper()
	var clients []string
	for _, u := range users {
		clients = append(clients, fmt.Sprintf(`{"id": %q, "email": %q}`, u[0], u[1]))
	}
	portLine := ""
	if port > 0 {
		portLine = fmt.Sprintf(`"port": %d,`, port)
	}
	cfg := fmt.Sprintf(`{
  "log": {"loglevel": "warning"},
  "stats": {},
  "policy": {
    "levels": {
      "0": {
        "statsUserUplink": true,
        "statsUserDownlink": true
      }
    },
    "system": {
      "statsInboundUplink": true,
      "statsInboundDownlink": true,
      "statsOutboundUplink": true,
      "statsOutboundDownlink": true
    }
  },
  "inbounds": [
    {
      "tag": "xhttp-vless",
      "listen": %q,
      %s
      "protocol": "vless",
      "settings": {
        "clients": [%s],
        "decryption": "none"
      },
      "streamSettings": {
        "network": "xhttp",
        "xhttpSettings": {
          "mode": "auto",
          "path": "/split"
        }
      }
    }
  ],
  "outbounds": [
    {
      "tag": "direct",
      "protocol": "freedom",
      "settings": {
        "finalRules": [
          {
            "action": "allow",
            "ip": ["127.0.0.1/32"]
          }
        ]
      }
    }
  ]
}`, listen, portLine, strings.Join(clients, ","), )
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config %s: %v", path, err)
	}
}

func proxyHTTPGetViaXHTTP(t *testing.T, serverPort int, uuid string, targetURL string) string {
	t.Helper()
	u, err := url.Parse(targetURL)
	if err != nil {
		t.Fatalf("parse target url: %v", err)
	}
	targetHost, targetPortStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split target host/port: %v", err)
	}
	targetPort, err := strconv.Atoi(targetPortStr)
	if err != nil {
		t.Fatalf("parse target port: %v", err)
	}

	clientCfg := fmt.Sprintf(`{
  "log": {"loglevel": "warning"},
  "inbounds": [],
  "outbounds": [
    {
      "tag": "proxy",
      "protocol": "vless",
      "settings": {
        "vnext": [
          {
            "address": "127.0.0.1",
            "port": %d,
            "users": [
              {
                "id": %q,
                "encryption": "none"
              }
            ]
          }
        ]
      },
      "streamSettings": {
        "network": "xhttp",
        "xhttpSettings": {
          "mode": "auto",
          "path": "/split"
        }
      }
    }
  ]
}`, serverPort, uuid)

	pbCfg, err := core.LoadConfig("json", bytes.NewBufferString(clientCfg))
	if err != nil {
		t.Fatalf("load client config: %v", err)
	}
	clientInst, err := core.New(pbCfg)
	if err != nil {
		t.Fatalf("create client instance: %v", err)
	}
	if err := clientInst.Start(); err != nil {
		_ = clientInst.Close()
		t.Fatalf("start client instance: %v", err)
	}
	defer clientInst.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dest := xnet.TCPDestination(xnet.ParseAddress(targetHost), xnet.Port(targetPort))
	conn, err := core.Dial(ctx, clientInst, dest)
	if err != nil {
		t.Fatalf("core.Dial: %v", err)
	}
	defer conn.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Close = true
	if err := req.Write(conn); err != nil {
		t.Fatalf("write HTTP request through proxy: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		t.Fatalf("read HTTP response through proxy: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read HTTP body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected HTTP status: %d body=%q", resp.StatusCode, string(body))
	}
	return string(body)
}

func statMap(stats []*command.Stat) map[string]int64 {
	out := make(map[string]int64, len(stats))
	for _, s := range stats {
		out[s.GetName()] = s.GetValue()
	}
	return out
}

func TestValidateConfig(t *testing.T) {
	dir := t.TempDir()
	validPath := filepath.Join(dir, "valid.json")
	writeTestServerConfig(t, validPath, "127.0.0.1", freeTCPPort(t), [][2]string{
		{"11111111-1111-1111-1111-111111111111", "alice@example.com"},
	})

	if err := ValidateConfig(validPath); err != nil {
		t.Fatalf("expected valid config to pass, got: %v", err)
	}

	invalidProtoPath := filepath.Join(dir, "invalid-proto.json")
	if err := os.WriteFile(invalidProtoPath, []byte(`{"inbounds":[{"protocol":"does-not-exist","port":10000,"settings":{}}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfig(invalidProtoPath); err == nil {
		t.Fatal("expected unknown protocol config to fail validation")
	}

	invalidSyntaxPath := filepath.Join(dir, "invalid-syntax.json")
	if err := os.WriteFile(invalidSyntaxPath, []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfig(invalidSyntaxPath); err == nil {
		t.Fatal("expected malformed JSON config to fail validation")
	}

	if err := ValidateConfig(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("expected missing config file to fail validation")
	}
}

func TestEngineLifecycleTrafficStatsAndReloadContinuity(t *testing.T) {
	backendPayload := strings.Repeat("xray-embedded-data-plane-", 64)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(backendPayload))
	}))
	defer backend.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "xray.json")
	aliceUUID := "11111111-1111-1111-1111-111111111111"
	bobUUID := "22222222-2222-2222-2222-222222222222"
	proxyPort := freeTCPPort(t)

	writeTestServerConfig(t, cfgPath, "127.0.0.1", proxyPort, [][2]string{
		{aliceUUID, "alice@example.com"},
	})

	var reloads atomic.Int32
	eng, err := NewEngine(cfgPath, func() { reloads.Add(1) })
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	if _, err := eng.QueryStats(context.Background(), "", false); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("expected ErrNotRunning before Start, got %v", err)
	}
	if _, err := eng.GetSysStats(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("expected ErrNotRunning from GetSysStats before Start, got %v", err)
	}

	if err := eng.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !eng.IsRunning() {
		t.Fatal("expected engine to report IsRunning()=true")
	}
	if !eng.HasStatsManager() {
		t.Fatal("expected engine to have real stats.Manager enabled")
	}

	gotBody := proxyHTTPGetViaXHTTP(t, proxyPort, aliceUUID, backend.URL)
	if gotBody != backendPayload {
		t.Fatalf("proxied body mismatch: got %d bytes, want %d", len(gotBody), len(backendPayload))
	}

	stats1, err := eng.QueryStats(context.Background(), "", false)
	if err != nil {
		t.Fatalf("QueryStats: %v", err)
	}
	m1 := statMap(stats1)
	aliceUp1 := m1["user>>>alice@example.com>>>traffic>>>uplink"]
	aliceDown1 := m1["user>>>alice@example.com>>>traffic>>>downlink"]
	inboundDown1 := m1["inbound>>>xhttp-vless>>>traffic>>>downlink"]
	outboundDown1 := m1["outbound>>>direct>>>traffic>>>downlink"]
	if aliceUp1 <= 0 || aliceDown1 < int64(len(backendPayload)) {
		t.Fatalf("expected non-zero alice traffic stats, got uplink=%d downlink=%d", aliceUp1, aliceDown1)
	}
	if inboundDown1 <= 0 || outboundDown1 <= 0 {
		t.Fatalf("expected non-zero inbound/outbound stats, got inbound=%d outbound=%d", inboundDown1, outboundDown1)
	}

	sysStats, err := eng.GetSysStats(context.Background())
	if err != nil {
		t.Fatalf("GetSysStats: %v", err)
	}
	if sysStats.GetNumGoroutine() == 0 || sysStats.GetAlloc() == 0 {
		t.Fatalf("unexpected sys stats: %+v", sysStats)
	}

	// Attempt reload with an invalid config; existing instance and stats must survive.
	if err := os.WriteFile(cfgPath, []byte(`{"inbounds":[{"protocol":"broken"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := eng.Reload(); err == nil {
		t.Fatal("expected Reload with invalid config to fail")
	}
	if !eng.IsRunning() {
		t.Fatal("expected engine to remain running after failed Reload")
	}
	if reloads.Load() != 0 {
		t.Fatalf("expected no reload callback on failed Reload, got %d", reloads.Load())
	}

	// Reload with valid config adding Bob (and removing Alice or keeping both).
	// Even if Alice doesn't send post-reload traffic yet, her cumulative counters must remain intact.
	newPort := freeTCPPort(t)
	writeTestServerConfig(t, cfgPath, "127.0.0.1", newPort, [][2]string{
		{aliceUUID, "alice@example.com"},
		{bobUUID, "bob@example.com"},
	})
	if err := eng.Reload(); err != nil {
		t.Fatalf("Reload with valid config: %v", err)
	}
	if reloads.Load() != 1 {
		t.Fatalf("expected 1 reload callback, got %d", reloads.Load())
	}

	statsAfterReload, err := eng.QueryStats(context.Background(), "", false)
	if err != nil {
		t.Fatalf("QueryStats after reload: %v", err)
	}
	m2 := statMap(statsAfterReload)
	if m2["user>>>alice@example.com>>>traffic>>>uplink"] != aliceUp1 || m2["user>>>alice@example.com>>>traffic>>>downlink"] != aliceDown1 {
		t.Fatalf("alice counters were not preserved across reload: before=(%d,%d) after=(%d,%d)",
			aliceUp1, aliceDown1,
			m2["user>>>alice@example.com>>>traffic>>>uplink"],
			m2["user>>>alice@example.com>>>traffic>>>downlink"])
	}

	// Send post-reload traffic for both Alice and Bob on the new port.
	if got := proxyHTTPGetViaXHTTP(t, newPort, bobUUID, backend.URL); got != backendPayload {
		t.Fatalf("bob proxied body mismatch: got %d bytes", len(got))
	}
	if got := proxyHTTPGetViaXHTTP(t, newPort, aliceUUID, backend.URL); got != backendPayload {
		t.Fatalf("alice post-reload proxied body mismatch: got %d bytes", len(got))
	}

	statsFinal, err := eng.QueryStats(context.Background(), "", false)
	if err != nil {
		t.Fatalf("QueryStats final: %v", err)
	}
	m3 := statMap(statsFinal)
	if m3["user>>>alice@example.com>>>traffic>>>uplink"] <= aliceUp1 || m3["user>>>alice@example.com>>>traffic>>>downlink"] <= aliceDown1 {
		t.Fatalf("expected alice counters to increase monotonically after reload: before=(%d,%d) final=(%d,%d)",
			aliceUp1, aliceDown1,
			m3["user>>>alice@example.com>>>traffic>>>uplink"],
			m3["user>>>alice@example.com>>>traffic>>>downlink"])
	}
	if m3["user>>>bob@example.com>>>traffic>>>uplink"] <= 0 || m3["user>>>bob@example.com>>>traffic>>>downlink"] < int64(len(backendPayload)) {
		t.Fatalf("expected non-zero bob counters after reload, got (%d,%d)",
			m3["user>>>bob@example.com>>>traffic>>>uplink"],
			m3["user>>>bob@example.com>>>traffic>>>downlink"])
	}

	// Close must be idempotent.
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if eng.IsRunning() {
		t.Fatal("expected IsRunning()=false after Close")
	}
}

func TestEngineSupportsUnixSocketXHTTPAndReload(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("unix-ok"))
	}))
	defer backend.Close()

	sockDir, err := os.MkdirTemp("", "xe-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	sockPath := filepath.Join(sockDir, "x.sock")
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "xray-unix.json")
	writeTestServerConfig(t, cfgPath, sockPath+",0666", 0, [][2]string{
		{"11111111-1111-1111-1111-111111111111", "alice@example.com"},
	})

	eng, err := NewEngine(cfgPath)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer eng.Close()

	if err := eng.Start(); err != nil {
		t.Fatalf("Start unix socket xhttp: %v", err)
	}
	if _, err := os.Stat(sockPath); err != nil {
		t.Fatalf("expected unix socket at %s: %v", sockPath, err)
	}
	// Reloading unix socket config must cleanly release the FileLocker and rebind.
	if err := eng.Reload(); err != nil {
		t.Fatalf("Reload unix socket xhttp: %v", err)
	}
	if _, err := os.Stat(sockPath); err != nil {
		t.Fatalf("expected unix socket after reload at %s: %v", sockPath, err)
	}
}

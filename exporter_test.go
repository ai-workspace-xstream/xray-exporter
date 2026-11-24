package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"
)

type fakeStatsServer struct {
	command.UnimplementedStatsServiceServer
	stats []*command.Stat
}

func (s *fakeStatsServer) QueryStats(_ context.Context, _ *command.QueryStatsRequest) (*command.QueryStatsResponse, error) {
	return &command.QueryStatsResponse{Stat: s.stats}, nil
}

func (s *fakeStatsServer) GetSysStats(_ context.Context, _ *command.SysStatsRequest) (*command.SysStatsResponse, error) {
	return &command.SysStatsResponse{Uptime: 10}, nil
}

func startFakeStatsServer(t *testing.T, stats []*command.Stat) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	command.RegisterStatsServiceServer(srv, &fakeStatsServer{stats: stats})
	go func() {
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func labelMap(pb *dto.Metric) map[string]string {
	labels := make(map[string]string, len(pb.GetLabel()))
	for _, lp := range pb.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	return labels
}

func TestScrapeXrayMetricsExportsOutboundPoolTrafficRegardlessOfUserTrafficMetrics(t *testing.T) {
	stats := []*command.Stat{
		{Name: "outbound>>>pool-media-stream>>>traffic>>>uplink", Value: 110},
		{Name: "outbound>>>pool-media-stream>>>traffic>>>downlink", Value: 220},
		{Name: "outbound>>>pool-media-fallback>>>traffic>>>uplink", Value: 330},
		{Name: "outbound>>>pool-media-fallback>>>traffic>>>downlink", Value: 440},
		{Name: "outbound>>>blocked-media-on-pro>>>traffic>>>uplink", Value: 550},
		{Name: "outbound>>>blocked-media-on-pro>>>traffic>>>downlink", Value: 660},
		{Name: "user>>>alice@example.com>>>traffic>>>uplink", Value: 777},
		{Name: "user>>>alice@example.com>>>traffic>>>downlink", Value: 888},
	}
	endpoint := startFakeStatsServer(t, stats)

	for _, userTrafficMetrics := range []bool{false, true} {
		name := "userTrafficMetrics=false"
		if userTrafficMetrics {
			name = "userTrafficMetrics=true"
		}
		t.Run(name, func(t *testing.T) {
			exp, err := NewExporterWithLogConfig(endpoint, time.Second, userTrafficMetrics, "", time.Minute)
			if err != nil {
				t.Fatalf("new exporter: %v", err)
			}
			defer exp.Close()

			ch := make(chan prometheus.Metric, 32)
			if err := exp.scrapeXrayMetrics(context.Background(), ch); err != nil {
				t.Fatalf("scrapeXrayMetrics: %v", err)
			}
			close(ch)

			type metricKey struct {
				desc      string
				dimension string
				target    string
			}
			got := make(map[metricKey]float64)
			for m := range ch {
				var pb dto.Metric
				if err := m.Write(&pb); err != nil {
					t.Fatalf("write metric: %v", err)
				}
				lbls := labelMap(&pb)
				if pb.GetCounter() == nil {
					t.Fatalf("expected counter metric, got %v", &pb)
				}
				got[metricKey{
					desc:      m.Desc().String(),
					dimension: lbls["dimension"],
					target:    lbls["target"],
				}] = pb.GetCounter().GetValue()
			}

			uplinkDesc := exp.metricDescriptions["traffic_uplink_bytes_total"].String()
			downlinkDesc := exp.metricDescriptions["traffic_downlink_bytes_total"].String()

			expectedOutbounds := map[metricKey]float64{
				{desc: uplinkDesc, dimension: "outbound", target: "pool-media-stream"}:      110,
				{desc: downlinkDesc, dimension: "outbound", target: "pool-media-stream"}:    220,
				{desc: uplinkDesc, dimension: "outbound", target: "pool-media-fallback"}:    330,
				{desc: downlinkDesc, dimension: "outbound", target: "pool-media-fallback"}:  440,
				{desc: uplinkDesc, dimension: "outbound", target: "blocked-media-on-pro"}:   550,
				{desc: downlinkDesc, dimension: "outbound", target: "blocked-media-on-pro"}: 660,
			}
			for key, want := range expectedOutbounds {
				if value, ok := got[key]; !ok || value != want {
					t.Fatalf("missing or mismatched outbound metric %+v: got=%v (ok=%t), want=%v", key, value, ok, want)
				}
			}

			userUpKey := metricKey{desc: uplinkDesc, dimension: "user", target: "alice@example.com"}
			userDownKey := metricKey{desc: downlinkDesc, dimension: "user", target: "alice@example.com"}
			_, hasUserUp := got[userUpKey]
			_, hasUserDown := got[userDownKey]
			if userTrafficMetrics != (hasUserUp && hasUserDown) {
				t.Fatalf("userTrafficMetrics=%t mismatch: hasUserUp=%t hasUserDown=%t", userTrafficMetrics, hasUserUp, hasUserDown)
			}
		})
	}
}

func TestTrafficCountersIgnoresOutboundStats(t *testing.T) {
	stats := []*command.Stat{
		{Name: "outbound>>>pool-media-stream>>>traffic>>>uplink", Value: 110},
		{Name: "outbound>>>pool-media-fallback>>>traffic>>>downlink", Value: 220},
		{Name: "outbound>>>blocked-media-on-pro>>>traffic>>>uplink", Value: 330},
		{Name: "inbound>>>xhttp-vless>>>traffic>>>downlink", Value: 440},
		{Name: "user>>>alice@example.com>>>traffic>>>uplink", Value: 777},
		{Name: "user>>>alice@example.com>>>traffic>>>downlink", Value: 888},
	}
	endpoint := startFakeStatsServer(t, stats)

	t.Run("userTrafficMetrics=false", func(t *testing.T) {
		exp, err := NewExporterWithLogConfig(endpoint, time.Second, false, "", time.Minute)
		if err != nil {
			t.Fatalf("new exporter: %v", err)
		}
		defer exp.Close()

		counters, err := exp.TrafficCounters(time.Second)
		if err != nil {
			t.Fatalf("TrafficCounters: %v", err)
		}
		if len(counters) != 0 {
			t.Fatalf("expected no counters when userTrafficMetrics=false, got %#v", counters)
		}
	})

	t.Run("userTrafficMetrics=true", func(t *testing.T) {
		exp, err := NewExporterWithLogConfig(endpoint, time.Second, true, "", time.Minute)
		if err != nil {
			t.Fatalf("new exporter: %v", err)
		}
		defer exp.Close()

		counters, err := exp.TrafficCounters(time.Second)
		if err != nil {
			t.Fatalf("TrafficCounters: %v", err)
		}
		if len(counters) != 2 {
			t.Fatalf("expected only 2 user counters (ignoring outbound/inbound), got %#v", counters)
		}
		for _, c := range counters {
			if c.Identifier != "alice@example.com" {
				t.Fatalf("unexpected non-user counter %#v", c)
			}
		}
	})
}

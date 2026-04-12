package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"xray-exporter/internal/accounts"
	"xray-exporter/internal/config"
	"xray-exporter/internal/history"
	"xray-exporter/internal/httpapi"
	"xray-exporter/internal/service"
	"xray-exporter/internal/xray"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	store, err := history.NewSQLiteStore(cfg.SnapshotStorePath, cfg.SnapshotRetention)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svc := service.New(
		cfg.NodeID,
		cfg.Env,
		cfg.ScrapeInterval,
		xray.NewClient(cfg.XrayStatsURL, cfg.XrayStatsToken),
		accounts.NewClient(cfg.AccountsBaseURL, cfg.InternalServiceToken),
		store,
	)
	svc.Start(ctx)

	server := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: httpapi.New(svc, cfg.InternalServiceToken).Routes(),
	}

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	log.Printf("xray-exporter listening on %s", cfg.ListenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

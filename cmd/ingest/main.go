package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ingest"
	"github.com/metabrainz/synapse/internal/observability"
	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	flushSentry, err := observability.InitSentry(cfg.Observability.SentryDSN, cfg.Observability.Environment, cfg.Observability.Release)
	if err != nil {
		slog.Error("sentry init", "err", err)
		os.Exit(1)
	}
	defer flushSentry()

	pool, err := store.NewPool(ctx, cfg.Postgres, int32(cfg.Ingest.Workers))
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := rabbitmq.Setup(cfg.RabbitMQ.URL, adapter.ChannelTypes()); err != nil {
		slog.Error("rabbitmq topology", "err", err)
		os.Exit(1)
	}

	reg := schema.New(schema.KnownTenants)

	subRepo := subscriptions.New(pool)
	cache := fanout.NewCache(pool, subRepo, cfg.Postgres.DirectDSN, reg)
	if err := cache.Start(ctx); err != nil {
		slog.Error("fanout cache", "err", err)
		os.Exit(1)
	}

	fan := fanout.New(cache, adapter.MaxAttemptsFor, reg)
	consumer := ingest.NewConsumer(pool, fan, reg)

	slog.Info("ingest: starting", "workers", cfg.Ingest.Workers, "batch_size", cfg.Ingest.BatchSize, "drain_ms", cfg.Ingest.DrainMs)

	if err := consumer.Run(ctx, cfg.RabbitMQ.URL, cfg.Ingest.Workers, cfg.Ingest.BatchSize, cfg.Ingest.DrainMs); err != nil && ctx.Err() == nil {
		slog.Error("ingest", "err", err)
		os.Exit(1)
	}

	slog.Info("ingest: stopped")
}

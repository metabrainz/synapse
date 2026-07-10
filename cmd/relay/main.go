package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/broker"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/observability"
	"github.com/metabrainz/synapse/internal/relay"
	"github.com/metabrainz/synapse/internal/store"
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

	flushSentry, err := observability.InitSentry(cfg.Observability.SentryDSN, cfg.Observability.Environment, cfg.Observability.Release, cfg.Observability.TracesSampleRate)
	if err != nil {
		slog.Error("sentry init", "err", err)
		os.Exit(1)
	}
	defer flushSentry()

	pool, err := store.NewPool(ctx, cfg.Postgres, int32(cfg.Relay.Workers))
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := rabbitmq.Setup(cfg.RabbitMQ.URL, adapter.ChannelTypes()); err != nil {
		slog.Error("rabbitmq topology", "err", err)
		os.Exit(1)
	}

	newPub := func() (broker.Publisher, error) { return rabbitmq.New(cfg.RabbitMQ.URL) }

	slog.Info("relay: starting", "workers", cfg.Relay.Workers, "poll_ms", cfg.Relay.OutboxPollMs, "batch_size", cfg.Relay.BatchSize)

	if err := relay.Run(ctx, pool, newPub, cfg.Relay.Workers, cfg.Relay.OutboxPollMs, cfg.Relay.BatchSize); err != nil && ctx.Err() == nil {
		slog.Error("relay", "err", err)
		os.Exit(1)
	}

	slog.Info("relay: stopped")
}

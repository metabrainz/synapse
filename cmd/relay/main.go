package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ingest"
	"github.com/metabrainz/synapse/internal/relay"
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

	pool, err := store.NewPool(ctx, cfg.Postgres)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := rabbitmq.Setup(cfg.RabbitMQ.URL); err != nil {
		slog.Error("rabbitmq topology", "err", err)
		os.Exit(1)
	}

	pub, err := rabbitmq.New(cfg.RabbitMQ.URL)
	if err != nil {
		slog.Error("rabbitmq publisher", "err", err)
		os.Exit(1)
	}
	defer pub.Close()

	subRepo := subscriptions.New(pool)
	fan := fanout.New(subRepo)
	ingestConsumer := ingest.NewConsumer(pool, fan)

	g, ctx := errgroup.WithContext(ctx)

	slog.Info("relay: starting", "workers", cfg.Relay.Workers, "poll_ms", cfg.Relay.OutboxPollMs)

	g.Go(func() error {
		return relay.Run(ctx, pool, pub, cfg.Relay.Workers, cfg.Relay.OutboxPollMs)
	})
	g.Go(func() error {
		slog.Info("relay: ingest consumer starting")
		return ingestConsumer.Run(ctx, cfg.RabbitMQ.URL)
	})

	if err := g.Wait(); err != nil && ctx.Err() == nil {
		slog.Error("relay", "err", err)
		os.Exit(1)
	}

	slog.Info("relay: stopped")
}

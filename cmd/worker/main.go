package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/metabrainz/synapse/internal/adapter/webhook"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/worker"
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

	rdb, err := store.NewRedis(ctx, cfg.Redis)
	if err != nil {
		slog.Error("redis", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()

	if err := rabbitmq.Setup(cfg.RabbitMQ.URL); err != nil {
		slog.Error("rabbitmq topology", "err", err)
		os.Exit(1)
	}

	deduper := dedup.New(rdb)

	g, ctx := errgroup.WithContext(ctx)

	slog.Info("worker: starting", "webhook_concurrency", cfg.Worker.WebhookConcurrency)

	g.Go(func() error {
		return worker.Run(ctx, "webhook", cfg.Worker.WebhookConcurrency, cfg.RabbitMQ.URL, webhook.New(), deduper, pool)
	})

	if err := g.Wait(); err != nil && ctx.Err() == nil {
		slog.Error("worker", "err", err)
		os.Exit(1)
	}

	slog.Info("worker: stopped")
}

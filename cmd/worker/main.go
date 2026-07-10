package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/observability"
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

	flushSentry, err := observability.InitSentry(cfg.Observability.SentryDSN, cfg.Observability.Environment, cfg.Observability.Release, cfg.Observability.TracesSampleRate)
	if err != nil {
		slog.Error("sentry init", "err", err)
		os.Exit(1)
	}
	defer flushSentry()

	pool, err := store.NewPool(ctx, cfg.Postgres, int32(cfg.Worker.DBPool))
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

	reg := eventtype.NewRegistry(eventtype.KnownTenants)

	if err := adapter.Build(ctx, adapter.Options{
		Telegram: adapter.TelegramOptions{
			BotToken: cfg.Telegram.BotToken,
		},
		Redis:    rdb,
		Registry: reg,
	}); err != nil {
		slog.Error("adapter: startup failed", "err", err)
		os.Exit(1)
	}

	if err := rabbitmq.Setup(cfg.RabbitMQ.URL, adapter.ChannelTypes()); err != nil {
		slog.Error("rabbitmq topology", "err", err)
		os.Exit(1)
	}

	g, gctx := errgroup.WithContext(ctx)

	for channelType, chanCfg := range cfg.Worker.Channels {
		ad, ok := adapter.Registry[adapter.ChannelType(channelType)]
		if !ok {
			slog.Warn("worker: no adapter registered, skipping", "type", channelType)
			continue
		}
		slog.Info("worker: starting", "type", channelType, "concurrency", chanCfg.Concurrency, "prefetch", chanCfg.Prefetch)
		g.Go(func() error {
			return worker.Run(gctx, channelType, chanCfg.Concurrency, chanCfg.Prefetch, cfg.RabbitMQ.URL, ad, pool)
		})
	}

	if err := g.Wait(); err != nil && ctx.Err() == nil {
		slog.Error("worker", "err", err)
		os.Exit(1)
	}

	slog.Info("worker: stopped")
}

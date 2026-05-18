package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/metabrainz/synapse/internal/cleanup"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/outbox"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to config file")
	interval := flag.Duration("interval", 0, "run continuously at this interval (0 = run once and exit)")
	eventAge := flag.Duration("event-age", 30*24*time.Hour, "delete events older than this (default 30 days)")
	pendingAge := flag.Duration("pending-age", time.Hour, "mark PENDING deliveries as DEAD after this (default 1h)")
	retryAge := flag.Duration("retry-age", 3*time.Hour, "mark RETRYING deliveries as DEAD after this (default 3h)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := store.NewPool(ctx, cfg.Postgres, 1)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	run := func() {
		if err := cleanup.ReconcileStale(ctx, pool, *pendingAge, *retryAge); err != nil {
			slog.Error("cleanup: reconcile", "err", err)
		}
		if err := cleanup.PruneOldEvents(ctx, pool, *eventAge); err != nil {
			slog.Error("cleanup: prune", "err", err)
		}
		if n, err := outbox.ResetStuck(ctx, pool, 5*time.Minute); err != nil {
			slog.Error("cleanup: reset stuck outbox rows", "err", err)
		} else if n > 0 {
			slog.Warn("cleanup: reset stuck outbox rows", "count", n)
		}
	}

	slog.Info("cleanup: starting", "interval", *interval, "event_age", *eventAge, "pending_age", *pendingAge, "retry_age", *retryAge)
	run()

	if *interval == 0 {
		slog.Info("cleanup: done")
		return
	}

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("cleanup: shutting down")
			return
		case <-ticker.C:
			run()
		}
	}
}

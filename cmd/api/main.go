package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/channels"
	"github.com/metabrainz/synapse/internal/store/eventtypes"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/tenantrules"
	"github.com/metabrainz/synapse/internal/store/tenants"
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

	pool, err := store.NewPool(ctx, cfg.Postgres, cfg.HTTP.DBConns)
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

	tenantRepo := tenants.New(pool)
	channelRepo := channels.New(pool)
	subRepo := subscriptions.New(pool)
	etRepo := eventtypes.New(pool)
	rulesRepo := tenantrules.New(pool)

	reg := schema.New(schema.KnownTenants)

	cache := fanout.NewCache(pool, subRepo, cfg.Postgres.DirectDSN)
	if err := cache.Start(ctx); err != nil {
		slog.Error("fanout cache", "err", err)
		os.Exit(1)
	}

	fan := fanout.New(cache, adapter.MaxAttemptsFor)
	deduper := dedup.New(rdb)

	var limiter *ratelimit.Limiter
	if cfg.RateLimit.Burst > 0 {
		limiter = ratelimit.New(rdb, cfg.RateLimit.Burst, cfg.RateLimit.RatePerSec)
	}

	router := api.NewRouter(
		api.Config{
			AdminKey: cfg.HTTP.AdminKey,
			Health: api.HealthChecks{
				Redis: func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
			},
			Limiter: limiter,
		},
		pool,
		tenantRepo,
		channelRepo,
		subRepo,
		etRepo,
		rulesRepo,
		fan,
		deduper,
		reg,
	)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("api: listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("api: server error", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	slog.Info("api: shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("api: shutdown error", "err", err)
	}
}

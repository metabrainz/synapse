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

	"github.com/metabrainz/synapse/internal/api"
	"github.com/metabrainz/synapse/internal/config"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/channels"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
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

	tenantRepo := tenants.New(pool)
	channelRepo := channels.New(pool)
	subRepo := subscriptions.New(pool)

	cache := fanout.NewCache(pool, subRepo)
	if err := cache.Start(ctx); err != nil {
		slog.Error("fanout cache", "err", err)
		os.Exit(1)
	}

	fan := fanout.New(cache)
	deduper := dedup.New(rdb)
	limiter := ratelimit.New(rdb, 200, 100) // burst=200, 100 req/s sustained

	router := api.NewRouter(
		api.Config{AdminKey: cfg.HTTP.AdminKey, Limiter: limiter},
		pool,
		tenantRepo,
		channelRepo,
		subRepo,
		fan,
		deduper,
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

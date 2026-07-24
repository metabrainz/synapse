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
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/internal/observability"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/users"
	"github.com/metabrainz/synapse/internal/store/usertenant"
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

	subRepo := subscriptions.New(pool)
	usersRepo := users.New(pool)
	userChannels := userchannels.New(pool)
	tenantMappings := usertenant.New(pool)
	eventSubs := usereventsubs.New(pool)

	var introspector oauth.Introspector
	if cfg.OAuth.IntrospectionURL != "" && cfg.OAuth.ClientID != "" {
		introspector = oauth.NewMBIntrospector(
			cfg.OAuth.ClientID,
			cfg.OAuth.ClientSecret,
			cfg.OAuth.IntrospectionURL,
			rdb,
		)
	} else {
		slog.Warn("oauth: not configured — /v1/me routes will return 503")
	}

	// Build the event-type registry first.
	tenants := eventtype.KnownTenants
	for i := range tenants {
		if tc, ok := cfg.Tenants[tenants[i].ID]; ok {
			tenants[i].APIKey = tc.APIKey
		}
	}
	reg := eventtype.NewRegistry(tenants)

	tenantFrom := make(map[string]string, len(cfg.Tenants))
	tenantNotifyURL := make(map[string]string, len(cfg.Tenants))
	for id, tc := range cfg.Tenants {
		if tc.EmailFrom != "" {
			tenantFrom[id] = tc.EmailFrom
		}
		if tc.NotificationSettingsURL != "" {
			tenantNotifyURL[id] = tc.NotificationSettingsURL
		}
	}

	if err := adapter.Build(ctx, adapter.Options{
		Webhook: adapter.WebhookOptions{
			AllowPrivateURLs: cfg.Webhook.AllowPrivateURLs,
		},
		Telegram: adapter.TelegramOptions{
			BotToken:      cfg.Telegram.BotToken,
			WebhookURL:    cfg.Telegram.WebhookURL,
			WebhookSecret: cfg.Telegram.WebhookSecret,
		},
		MailService: adapter.MailServiceOptions{
			URL:                           cfg.MailService.URL,
			TenantFrom:                    tenantFrom,
			TenantNotificationSettingsURL: tenantNotifyURL,
		},
		Redis:    rdb,
		Registry: reg,
	}); err != nil {
		slog.Error("adapter: startup failed", "err", err)
		os.Exit(1)
	}

	cache := fanout.NewCache(pool, subRepo, cfg.Postgres.DirectDSN, reg)
	if err := cache.Start(ctx); err != nil {
		slog.Error("fanout cache", "err", err)
		os.Exit(1)
	}

	fan := fanout.New(cache, adapter.MaxAttemptsFor, reg)

	var limiter *ratelimit.Limiter
	if cfg.RateLimit.Burst > 0 {
		limiter = ratelimit.New(rdb, cfg.RateLimit.Burst, cfg.RateLimit.RatePerSec)
	}

	router := api.NewRouter(
		api.Config{
			Introspector: introspector,
			Health: api.HealthChecks{
				Redis: func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
			},
			Limiter: limiter,
		},
		api.Deps{
			Pool:           pool,
			Redis:          rdb,
			Users:          usersRepo,
			UserChannels:   userChannels,
			TenantMappings: tenantMappings,
			Subscriptions:  eventSubs,
			Fanout:         fan,
			Registry:       reg,
		},
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

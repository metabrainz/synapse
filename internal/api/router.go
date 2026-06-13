package api

import (
	"context"
	"net/http"
	"time"

	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/users"
	"github.com/metabrainz/synapse/internal/store/usertenant"
	"github.com/redis/go-redis/v9"
)

// HealthChecks holds optional probe functions for /health/ready.
type HealthChecks struct {
	Redis func(ctx context.Context) error
	AMQP  func(ctx context.Context) error
}

type Config struct {
	Introspector oauth.Introspector
	Health       HealthChecks
	Limiter      *ratelimit.Limiter
}

// NewRouter wires all HTTP routes and returns the root handler.
//
// Three auth surfaces:
//
//	Surface A — tenant API key (static registry): POST /v1/events, GET /v1/events/{id}/deliveries
//	Surface B — MetaBrainz OAuth token:           /v1/me/**
//	Internal   — per-adapter secret headers:       /internal/**  (e.g. Telegram webhook)
func NewRouter(
	cfg Config,
	pool *pgxpool.Pool,
	rdb *redis.Client,
	usersRepo *users.Repo,
	userChannels *userchannels.Repo,
	tenantMappings *usertenant.Repo,
	subscriptions *usereventsubs.Repo,
	fan *fanout.Fanout,
	deduper *dedup.Deduper,
	reg *eventtype.Registry,
) http.Handler {
	r := chi.NewRouter()
	r.Use(sentryhttp.New(sentryhttp.Options{Repanic: true}).Handle)
	r.Use(chimw.Recoverer)
	r.Use(chimw.RealIP)

	// GET /health/ready — liveness probe; checks Postgres, Redis, and AMQP.
	// Uses a 3 s deadline so a hung dependency doesn't stall the load balancer.
	r.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "postgres unavailable")
			return
		}
		if cfg.Health.Redis != nil {
			if err := cfg.Health.Redis(ctx); err != nil {
				writeError(w, http.StatusServiceUnavailable, "redis unavailable")
				return
			}
		}
		if cfg.Health.AMQP != nil {
			if err := cfg.Health.AMQP(ctx); err != nil {
				writeError(w, http.StatusServiceUnavailable, "rabbitmq unavailable")
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Surface A — tenant API key auth.
	// Rate-limited per tenant ID.
	authMW := middleware.NewAuth(reg)
	r.With(authMW).Route("/v1", func(r chi.Router) {
		if cfg.Limiter != nil {
			r.Use(cfg.Limiter.Middleware(func(r *http.Request) string {
				if tenant := middleware.TenantFromContext(r.Context()); tenant != nil {
					return tenant.ID
				}
				return ""
			}))
		}
		ing := &ingestHandler{pool: pool, fan: fan, deduper: deduper, reg: reg}
		// POST /v1/events — ingest an event and fan it out to matching subscriber channels.
		r.Post("/events", ing.ServeHTTP)
		// GET /v1/events/{event_id}/deliveries — list delivery records for an event (tenant-scoped).
		r.Get("/events/{event_id}/deliveries", (&deliveriesHandler{pool: pool}).listByEvent)
	})

	// Surface B — MetaBrainz OAuth token auth.
	// Rate-limited per user ID. When OAuth is not configured, all /v1/me requests return 503.
	var userMW func(http.Handler) http.Handler
	if cfg.Introspector != nil {
		userMW = middleware.NewUserAuth(cfg.Introspector, usersRepo)
	} else {
		userMW = func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeError(w, http.StatusServiceUnavailable, "oauth not configured")
			})
		}
	}
	me := &meHandler{
		channels:       userChannels,
		tenantMappings: tenantMappings,
		subscriptions:  subscriptions,
		reg:            reg,
	}

	r.With(userMW).Route("/v1/me", func(r chi.Router) {
		if cfg.Limiter != nil {
			r.Use(cfg.Limiter.Middleware(func(r *http.Request) string {
				return middleware.UserFromContext(r.Context())
			}))
		}

		// Channel management — CRUD for the user's notification channels.
		r.Get("/channels", me.listChannels)
		r.Post("/channels", me.createChannel)
		r.Delete("/channels/{id}", me.deleteChannel)

		// Catalog — read-only view of what a tenant exposes.
		r.Get("/tenants/{tenant_id}/event-types", me.listTenantEventTypes)

		// Tenant channel mappings — which channel receives deliveries for a given tenant.
		r.Get("/tenants/{tenant_id}/channels", me.listTenantChannels)
		r.Put("/tenants/{tenant_id}/channels/{channel_type}", me.assignTenantChannel)
		r.Delete("/tenants/{tenant_id}/channels/{channel_type}", me.removeTenantChannel)

		// Subscriptions — opt in/out of specific event types per tenant and channel.
		r.Get("/tenants/{tenant_id}/subscriptions", me.listSubscriptions)
		r.Put("/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}", me.subscribe)
		r.Delete("/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}", me.unsubscribe)
	})

	// Internal routes — each adapter mounts its own under /internal/** or /v1/me/**.
	// Auth is adapter-specific (e.g. Telegram validates X-Telegram-Bot-Api-Secret-Token).
	for _, ct := range adapter.ChannelTypes() {
		if rp, ok := adapter.Registry[ct].(adapter.RouteProvider); ok {
			rp.MountRoutes(r, userMW, rdb, userChannels)
		}
	}

	return r
}

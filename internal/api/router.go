package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/schema"
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
	reg *schema.Registry,
) http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RealIP)

	// Health
	r.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
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

	// Tenant-authenticated routes (Surface A — tenant API key via static registry)
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
		r.Post("/events", ing.ServeHTTP)
		r.Get("/events/{event_id}/deliveries", (&deliveriesHandler{pool: pool}).listByEvent)
	})

	// User OAuth routes (Surface B — MetaBrainz OAuth token).
	userMW := middleware.NewUserAuth(cfg.Introspector, usersRepo)
	me := &meHandler{
		channels:       userChannels,
		tenantMappings: tenantMappings,
		subscriptions:  subscriptions,
		reg:            reg,
	}

	r.With(userMW).Route("/v1/me", func(r chi.Router) {
		r.Get("/channels", me.listChannels)
		r.Post("/channels", me.createChannel)
		r.Delete("/channels/{id}", me.deleteChannel)

		r.Get("/tenants/{tenant_id}/event-types", me.listTenantEventTypes)

		r.Get("/tenants/{tenant_id}/channels", me.listTenantChannels)
		r.Put("/tenants/{tenant_id}/channels/{channel_type}", me.assignTenantChannel)
		r.Delete("/tenants/{tenant_id}/channels/{channel_type}", me.removeTenantChannel)

		r.Get("/tenants/{tenant_id}/subscriptions", me.listSubscriptions)
		r.Put("/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}", me.subscribe)
		r.Delete("/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}", me.unsubscribe)
	})

	// Let each adapter mount its own routes (connect flows, inbound webhooks, etc.)
	for _, ct := range adapter.ChannelTypes() {
		if rp, ok := adapter.Registry[ct].(adapter.RouteProvider); ok {
			rp.MountRoutes(r, userMW, rdb, userChannels)
		}
	}

	return r
}

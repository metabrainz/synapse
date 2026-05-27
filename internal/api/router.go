package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store/channels"
	"github.com/metabrainz/synapse/internal/store/eventtypes"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/tenantrules"
	"github.com/metabrainz/synapse/internal/store/tenants"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/users"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

// HealthChecks holds optional probe functions for /health/ready.
type HealthChecks struct {
	Redis func(ctx context.Context) error
	AMQP  func(ctx context.Context) error
}

type Config struct {
	AdminKey     string
	Introspector oauth.Introspector
	Health       HealthChecks
	Limiter      *ratelimit.Limiter
}

func NewRouter(
	cfg Config,
	pool *pgxpool.Pool,
	tenantRepo *tenants.Repo,
	channelRepo *channels.Repo,
	subRepo *subscriptions.Repo,
	etRepo *eventtypes.Repo,
	rulesRepo *tenantrules.Repo,
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

	// Admin routes
	authMW, evictKey := middleware.NewAuth(tenantRepo)
	admin := &adminHandler{repo: tenantRepo, evictKey: evictKey}

	et := &eventTypesHandler{repo: etRepo}
	cr := &channelRulesHandler{repo: rulesRepo}

	r.With(middleware.RequireAdminKey(cfg.AdminKey)).Route("/v1/admin", func(r chi.Router) {
		r.Post("/tenants", admin.create)
		r.Get("/tenants", admin.list)
		r.Post("/tenants/{id}/rotate-key", admin.rotateKey)
		r.Post("/tenants/{id}/event-types", et.register)
		r.Get("/tenants/{id}/event-types", et.list)
		r.Delete("/tenants/{id}/event-types/{event_type}", et.delete)
		r.Put("/tenants/{id}/channel-rules", cr.upsert)
		r.Get("/tenants/{id}/channel-rules", cr.list)
		r.Delete("/tenants/{id}/channel-rules/{event_type}/{channel_type}", cr.delete)
	})

	// Tenant-authenticated routes
	r.With(authMW).Route("/v1", func(r chi.Router) {
		if cfg.Limiter != nil {
			r.Use(cfg.Limiter.Middleware(func(r *http.Request) string {
				if t := middleware.TenantFromContext(r.Context()); t != nil {
					return t.ID
				}
				return ""
			}))
		}
		// Event ingestion
		ing := &ingestHandler{pool: pool, fan: fan, deduper: deduper, reg: reg}
		r.Post("/events", ing.ServeHTTP)
		r.Get("/events/{event_id}/deliveries", (&deliveriesHandler{pool: pool}).listByEvent)

		// Channel management
		ch := &channelsHandler{repo: channelRepo, pool: pool}
		r.Route("/users/{user_id}/channels", func(r chi.Router) {
			r.Post("/", ch.create)
			r.Get("/", ch.list)
			r.Delete("/{id}", ch.delete)

			// Subscriptions nested under channels
			sub := &subscriptionsHandler{repo: subRepo}
			r.Post("/{id}/subscriptions", sub.create)
			r.Get("/{id}/subscriptions", sub.list)
			r.Delete("/{id}/subscriptions/{event_type}", sub.delete)
		})
	})

	// User OAuth routes.
	userMW := middleware.NewUserAuth(cfg.Introspector, usersRepo)
	me := &meHandler{
		channels:       userChannels,
		tenantMappings: tenantMappings,
		subscriptions:  subscriptions,
	}

	r.With(userMW).Route("/v1/me", func(r chi.Router) {
		r.Get("/channels", me.listChannels)
		r.Post("/channels", me.createChannel)
		r.Delete("/channels/{id}", me.deleteChannel)

		r.Get("/tenants/{tenant_id}/channels", me.listTenantChannels)
		r.Put("/tenants/{tenant_id}/channels/{channel_type}", me.assignTenantChannel)
		r.Delete("/tenants/{tenant_id}/channels/{channel_type}", me.removeTenantChannel)

		r.Get("/tenants/{tenant_id}/subscriptions", me.listSubscriptions)
		r.Put("/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}", me.subscribe)
		r.Delete("/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}", me.unsubscribe)
	})

	return r
}

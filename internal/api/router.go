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
	"github.com/metabrainz/synapse/internal/store/channels"
	"github.com/metabrainz/synapse/internal/store/eventtypes"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/tenants"
)

// HealthChecks holds optional probe functions for /health/ready.
type HealthChecks struct {
	Redis func(ctx context.Context) error
	AMQP  func(ctx context.Context) error
}

type Config struct {
	AdminKey string
	Health   HealthChecks
}

func NewRouter(
	cfg Config,
	pool *pgxpool.Pool,
	tenantRepo *tenants.Repo,
	channelRepo *channels.Repo,
	subRepo *subscriptions.Repo,
	etRepo *eventtypes.Repo,
	fan *fanout.Fanout,
	deduper *dedup.Deduper,
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
	admin := &adminHandler{repo: tenantRepo}

	// Event types routes
	et := &eventTypesHandler{repo: etRepo}

	r.With(middleware.RequireAdminKey(cfg.AdminKey)).Route("/v1/admin", func(r chi.Router) {
		r.Post("/tenants", admin.create)
		r.Get("/tenants", admin.list)
		r.Post("/tenants/{id}/rotate-key", admin.rotateKey)
		r.Post("/tenants/{id}/event-types", et.register)
		r.Get("/tenants/{id}/event-types", et.list)
		r.Delete("/tenants/{id}/event-types/{event_type}", et.delete)
	})

	// Tenant-authenticated routes
	r.With(middleware.Authenticate(tenantRepo)).Route("/v1", func(r chi.Router) {
		// Event ingestion
		ing := &ingestHandler{pool: pool, fan: fan, deduper: deduper}
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

	return r
}

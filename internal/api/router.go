package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/store/channels"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/tenants"
)

type Config struct {
	AdminKey string
	Limiter  *ratelimit.Limiter // nil = no rate limiting
}

func NewRouter(
	cfg Config,
	pool *pgxpool.Pool,
	tenantRepo *tenants.Repo,
	channelRepo *channels.Repo,
	subRepo *subscriptions.Repo,
	fan *fanout.Fanout,
	deduper *dedup.Deduper,
) http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)
	r.Use(chimw.RealIP)

	// Health
	r.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Admin — separate key, not tenant auth
	admin := &adminHandler{repo: tenantRepo}
	r.With(middleware.RequireAdminKey(cfg.AdminKey)).Route("/v1/admin", func(r chi.Router) {
		r.Post("/tenants", admin.create)
		r.Get("/tenants", admin.list)
		r.Post("/tenants/{id}/rotate-key", admin.rotateKey)
	})

	// Tenant-authenticated routes
	r.With(middleware.Authenticate(tenantRepo)).Route("/v1", func(r chi.Router) {
		// Event ingestion — rate limited per tenant when limiter is configured
		ing := &ingestHandler{pool: pool, fan: fan, deduper: deduper}
		if cfg.Limiter != nil {
			tenantID := func(r *http.Request) string {
				if t := middleware.TenantFromContext(r.Context()); t != nil {
					return t.ID
				}
				return ""
			}
			r.With(cfg.Limiter.Middleware(tenantID)).Post("/events", ing.ServeHTTP)
		} else {
			r.Post("/events", ing.ServeHTTP)
		}
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

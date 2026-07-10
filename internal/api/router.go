package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	sentryhttp "github.com/getsentry/sentry-go/http"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api/middleware"
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
}

type Config struct {
	Introspector oauth.Introspector
	Health       HealthChecks
	Limiter      *ratelimit.Limiter
}

// Deps bundles the data-layer dependencies injected into the router.
type Deps struct {
	Pool           *pgxpool.Pool
	Redis          *redis.Client
	Users          *users.Repo
	UserChannels   *userchannels.Repo
	TenantMappings *usertenant.Repo
	Subscriptions  *usereventsubs.Repo
	Fanout         *fanout.Fanout
	Registry       *eventtype.Registry
}

// NewRouter wires all HTTP routes and returns the root handler.
func NewRouter(cfg Config, d Deps) http.Handler {
	r := chi.NewRouter()
	r.Use(sentryhttp.New(sentryhttp.Options{Repanic: true}).Handle)
	r.Use(chimw.Recoverer)
	r.Use(chimw.RealIP)

	// Global body size limit — huma reads the body centrally, so the limit must be
	// applied before huma sees the request.
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB
			}
			next.ServeHTTP(w, r)
		})
	})

	authMW := middleware.NewAuth(d.Registry)

	var userMW func(http.Handler) http.Handler
	if cfg.Introspector != nil {
		userMW = middleware.NewUserAuth(cfg.Introspector, d.Users)
	} else {
		userMW = func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeError(w, http.StatusServiceUnavailable, "oauth not configured")
			})
		}
	}

	// Path-dispatching middleware: applies Surface A or B auth + rate limiting based on
	// URL prefix. /v1/me/** → Surface B (OAuth); /v1/** → Surface A (tenant API key);
	// everything else (health check, internal adapter routes) passes through unauthenticated.
	//
	// Auth runs before rate limiting so the rate-limit key function can read the
	// tenant/user ID set in context by auth.
	r.Use(func(next http.Handler) http.Handler {
		surfaceAInner := next
		if cfg.Limiter != nil {
			surfaceAInner = cfg.Limiter.Middleware(func(r *http.Request) string {
				if t := middleware.TenantFromContext(r.Context()); t != nil {
					return t.ID
				}
				return ""
			})(next)
		}
		surfaceA := authMW(surfaceAInner)

		surfaceBInner := next
		if cfg.Limiter != nil {
			surfaceBInner = cfg.Limiter.Middleware(func(r *http.Request) string {
				return middleware.UserFromContext(r.Context())
			})(next)
		}
		surfaceB := userMW(surfaceBInner)

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p := r.URL.Path
			switch {
			case strings.HasPrefix(p, "/v1/me"):
				surfaceB.ServeHTTP(w, r)
			case strings.HasPrefix(p, "/v1/"):
				surfaceA.ServeHTTP(w, r)
			default:
				next.ServeHTTP(w, r)
			}
		})
	})

	// GET /health/ready — liveness probe; checks Postgres, Redis, and AMQP.
	// Uses a 3 s deadline so a hung dependency doesn't stall the load balancer.
	r.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := d.Pool.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "postgres unavailable")
			return
		}
		if cfg.Health.Redis != nil {
			if err := cfg.Health.Redis(ctx); err != nil {
				writeError(w, http.StatusServiceUnavailable, "redis unavailable")
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// Huma API — generates OpenAPI spec from handler types.
	// Spec: GET /openapi.json   Swagger UI: GET /docs
	api := humachi.New(r, huma.DefaultConfig("Synapse API", "1.0.0"))

	// Security schemes — wired to the Swagger UI "Authorize" button.
	spec := api.OpenAPI()
	if spec.Components == nil {
		spec.Components = &huma.Components{}
	}
	spec.Components.SecuritySchemes = map[string]*huma.SecurityScheme{
		"TenantAPIKey": {
			Type:         "http",
			Scheme:       "bearer",
			BearerFormat: "API Key",
			Description:  "Tenant API key — Surface A routes (/v1/events/**)",
		},
		"UserOAuth": {
			Type:         "http",
			Scheme:       "bearer",
			BearerFormat: "OAuth token",
			Description:  "MetaBrainz OAuth Bearer token — Surface B routes (/v1/me/**)",
		},
	}

	registerRoutes(api, d.Pool, d.Fanout, d.Registry, d.UserChannels, d.TenantMappings, d.Subscriptions)

	for _, ct := range adapter.ChannelTypes() {
		if rp, ok := adapter.Registry[ct].(adapter.RouteProvider); ok {
			rp.MountRoutes(r, userMW, d.Redis, d.UserChannels)
		}
	}

	return r
}

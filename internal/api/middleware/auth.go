package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/metabrainz/synapse/internal/store/tenants"
)

type contextKey string

const tenantKey contextKey = "tenant"

// authCache is a short-lived in-memory cache for API-key → tenant lookups.
// It keeps auth off the hot DB path under high ingest load.
type authCache struct {
	mu  sync.RWMutex
	m   map[string]authEntry
	ttl time.Duration
}

type authEntry struct {
	tenant    *tenants.Tenant
	expiresAt time.Time
}

func newAuthCache(ttl time.Duration) *authCache {
	return &authCache{m: make(map[string]authEntry), ttl: ttl}
}

func (c *authCache) get(key string) (*tenants.Tenant, bool) {
	c.mu.RLock()
	e, ok := c.m[key]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.tenant, true
}

func (c *authCache) set(key string, t *tenants.Tenant) {
	c.mu.Lock()
	c.m[key] = authEntry{tenant: t, expiresAt: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}

// Authenticate validates the Bearer API key and puts the tenant in the request context.
// Results are cached for 30 s so auth lookups don't compete with ingest transactions
// for DB pool connections under heavy load.
func Authenticate(repo *tenants.Repo) func(http.Handler) http.Handler {
	cache := newAuthCache(30 * time.Second)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if key == "" {
				http.Error(w, `{"error":"missing authorization"}`, http.StatusUnauthorized)
				return
			}

			if t, ok := cache.get(key); ok {
				ctx := context.WithValue(r.Context(), tenantKey, t)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			tenant, err := repo.GetByAPIKey(r.Context(), key)
			if errors.Is(err, tenants.ErrNotFound) || (err == nil && tenant == nil) {
				http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
				return
			}
			if err != nil {
				// DB error — don't pretend it's an auth failure.
				http.Error(w, `{"error":"service unavailable"}`, http.StatusServiceUnavailable)
				return
			}

			cache.set(key, tenant)
			ctx := context.WithValue(r.Context(), tenantKey, tenant)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// TenantFromContext retrieves the authenticated tenant from the request context.
func TenantFromContext(ctx context.Context) *tenants.Tenant {
	t, _ := ctx.Value(tenantKey).(*tenants.Tenant)
	return t
}

// RequireAdminKey validates a static admin API key from the X-Admin-Key header.
func RequireAdminKey(adminKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Admin-Key") != adminKey {
				http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

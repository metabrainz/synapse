package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/metabrainz/synapse/internal/store/tenants"
)

type contextKey string

const tenantKey contextKey = "tenant"

// Authenticate validates the Bearer API key and puts the tenant in the request context.
func Authenticate(repo *tenants.Repo) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if key == "" {
				http.Error(w, `{"error":"missing authorization"}`, http.StatusUnauthorized)
				return
			}

			tenant, err := repo.GetByAPIKey(r.Context(), key)
			if err != nil || tenant == nil {
				http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
				return
			}

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

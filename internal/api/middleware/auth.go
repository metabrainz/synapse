package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/metabrainz/synapse/internal/eventtype"
)

type contextKey string

const tenantKey contextKey = "tenant"

// NewAuth returns middleware that authenticates Surface A requests (tenant API keys)
// against the static registry.
func NewAuth(reg *eventtype.Registry) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if key == "" {
				http.Error(w, `{"error":"missing authorization"}`, http.StatusUnauthorized)
				return
			}
			t, ok := reg.LookupByAPIKey(key)
			if !ok {
				http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), tenantKey, t)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// TenantFromContext retrieves the authenticated tenant from the request context.
func TenantFromContext(ctx context.Context) *eventtype.Tenant {
	t, _ := ctx.Value(tenantKey).(*eventtype.Tenant)
	return t
}

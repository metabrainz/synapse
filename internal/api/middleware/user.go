package middleware

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/metabrainz/synapse/internal/oauth"
)

type userContextKey struct{}

// UserRepo is the minimal interface the user auth middleware needs.
// Implemented by *users.Repo
type UserRepo interface {
	Upsert(ctx context.Context, id string) error
}

// writeJSONError writes a JSON error response with the correct Content-Type.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}

// NewUserAuth returns middleware that validates a Bearer token via the given
// Introspector, upserts the user, and puts the stable MetaBrainz user ID in context.
func NewUserAuth(i oauth.Introspector, repo UserRepo) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token == "" || token == authHeader {
				w.Header().Set("WWW-Authenticate", `Bearer realm="synapse"`)
				writeJSONError(w, http.StatusUnauthorized, "missing authorization")
				return
			}

			claims, err := i.Introspect(r.Context(), token)
			if errors.Is(err, oauth.ErrInactive) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="synapse"`)
				writeJSONError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}
			if err != nil {
				slog.Error("user oauth: introspect error", "err", err)
				writeJSONError(w, http.StatusServiceUnavailable, "service unavailable")
				return
			}

			if err := repo.Upsert(r.Context(), claims.ID); err != nil {
				slog.Error("user oauth: upsert failed", "err", err)
				writeJSONError(w, http.StatusServiceUnavailable, "service unavailable")
				return
			}

			ctx := context.WithValue(r.Context(), userContextKey{}, claims.ID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// UserFromContext returns the authenticated user's stable MetaBrainz user ID from context,
// or "" if not set.
func UserFromContext(ctx context.Context) string {
	v, _ := ctx.Value(userContextKey{}).(string)
	return v
}

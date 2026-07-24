package middleware_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/oauth"
)

type stubIntrospector struct {
	claims oauth.Claims
	err    error
}

func (s *stubIntrospector) Introspect(_ context.Context, _ string) (oauth.Claims, error) {
	return s.claims, s.err
}

type stubUserRepo struct{ upsertErr error }

func (s *stubUserRepo) Upsert(_ context.Context, _, _ string) error { return s.upsertErr }

func TestNewUserAuth_ValidToken(t *testing.T) {
	mw := middleware.NewUserAuth(&stubIntrospector{claims: oauth.Claims{ID: "42"}}, &stubUserRepo{})

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := middleware.UserFromContext(r.Context())
		if uid != "42" {
			t.Fatalf("want 42, got %q", uid)
		}
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer some-token")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}

func TestNewUserAuth_MissingToken(t *testing.T) {
	mw := middleware.NewUserAuth(&stubIntrospector{claims: oauth.Claims{}}, &stubUserRepo{})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
}

func TestNewUserAuth_InactiveToken(t *testing.T) {
	mw := middleware.NewUserAuth(&stubIntrospector{err: oauth.ErrInactive, claims: oauth.Claims{}}, &stubUserRepo{})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer bad")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
}

func TestNewUserAuth_UpsertError(t *testing.T) {
	mw := middleware.NewUserAuth(
		&stubIntrospector{claims: oauth.Claims{ID: "42"}},
		&stubUserRepo{upsertErr: errors.New("db down")},
	)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer tok")
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", w.Code)
	}
}

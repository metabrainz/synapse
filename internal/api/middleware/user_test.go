package middleware_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

type stubUserRepo struct {
	upsertErr error
	upserts   int
}

func (s *stubUserRepo) Upsert(_ context.Context, _, _ string) error {
	s.upserts++
	return s.upsertErr
}

func TestNewUserAuth_ValidToken(t *testing.T) {
	mw := middleware.NewUserAuth(&stubIntrospector{claims: oauth.Claims{ID: "42", Read: true, Full: true}}, &stubUserRepo{})

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
		&stubIntrospector{claims: oauth.Claims{ID: "42", Read: true, Full: true}},
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

func TestNewUserAuth_Scopes(t *testing.T) {
	var (
		none = oauth.Claims{ID: "42"}
		read = oauth.Claims{ID: "42", Read: true}
		full = oauth.Claims{ID: "42", Read: true, Full: true}
	)
	// Per-route coverage lives in api.TestDocumentedScopesMatchEnforced.
	readRoutes := []string{
		"POST /v1/me/tenants/listenbrainz/notifications/7/read",
		"GET /v1/me/notifications",
		"GET /v1/stream/tenants/listenbrainz/notifications",
	}
	fullRoutes := []string{
		"POST /v1/me/channels",
		// a tenant named "notifications" is still a channels route
		"GET /v1/me/tenants/notifications/channels",
		// chi routes this to the channels handler; the check must not decode it
		"GET /v1/me/tenants/a%2Fnotifications/channels",
	}
	const (
		readHeader = `Bearer realm="synapse", error="insufficient_scope", scope="synapse:notifications:read"`
		fullHeader = `Bearer realm="synapse", error="insufficient_scope", scope="synapse:notifications"`
	)

	check := func(t *testing.T, claims oauth.Claims, route string, wantStatus int, wantHeader string) {
		t.Helper()
		repo := &stubUserRepo{}
		mw := middleware.NewUserAuth(&stubIntrospector{claims: claims}, repo)
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		method, path, _ := strings.Cut(route, " ")
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, nil)
		r.Header.Set("Authorization", "Bearer tok")
		handler.ServeHTTP(w, r)

		if w.Code != wantStatus {
			t.Errorf("%s: status %d, want %d", route, w.Code, wantStatus)
		}
		if got := w.Header().Get("WWW-Authenticate"); got != wantHeader {
			t.Errorf("%s: WWW-Authenticate %q, want %q", route, got, wantHeader)
		}
		if wantUpsert := wantStatus == http.StatusOK; (repo.upserts > 0) != wantUpsert {
			t.Errorf("%s: upserted=%v, want %v", route, repo.upserts > 0, wantUpsert)
		}
	}

	for _, route := range readRoutes {
		check(t, none, route, http.StatusForbidden, readHeader)
		check(t, read, route, http.StatusOK, "")
		check(t, full, route, http.StatusOK, "")
	}
	for _, route := range fullRoutes {
		check(t, none, route, http.StatusForbidden, fullHeader)
		check(t, read, route, http.StatusForbidden, fullHeader)
		check(t, full, route, http.StatusOK, "")
	}
}

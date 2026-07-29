package oauth_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/oauth"
)

func TestMBIntrospector_ValidToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		if r.FormValue("client_id") != "client-id" || r.FormValue("client_secret") != "client-secret" {
			http.Error(w, "bad credentials", http.StatusUnauthorized)
			return
		}
		if r.FormValue("token") != "valid-token" {
			json.NewEncoder(w).Encode(map[string]any{"active": false})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"active":     true,
			"sub":        "42",
			"username":   "testuser",
			"expires_at": time.Now().Add(time.Hour).Unix(),
			"scope":      []string{"profile"},
		})
	}))
	defer ts.Close()

	i := oauth.NewMBIntrospector("client-id", "client-secret", ts.URL, nil)
	claims, err := i.Introspect(context.Background(), "valid-token")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if claims.ID != "42" {
		t.Fatalf("want id=42, got %q", claims.ID)
	}
	if claims.Username != "testuser" {
		t.Fatalf("want username=testuser, got %q", claims.Username)
	}
}

func TestMBIntrospector_InactiveToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"active": false})
	}))
	defer ts.Close()

	i := oauth.NewMBIntrospector("client-id", "client-secret", ts.URL, nil)
	_, err := i.Introspect(context.Background(), "bad-token")
	if !errors.Is(err, oauth.ErrInactive) {
		t.Fatalf("want ErrInactive, got %v", err)
	}
}

func TestMBIntrospector_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer ts.Close()

	i := oauth.NewMBIntrospector("client-id", "client-secret", ts.URL, nil)
	_, err := i.Introspect(context.Background(), "any")
	if err == nil || errors.Is(err, oauth.ErrInactive) {
		t.Fatalf("want a non-ErrInactive error, got %v", err)
	}
}

func TestMBIntrospector_ExpiredToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"active":     true,
			"sub":        "42",
			"username":   "testuser",
			"expires_at": time.Now().Add(-time.Hour).Unix(), // already expired
		})
	}))
	defer ts.Close()

	i := oauth.NewMBIntrospector("client-id", "client-secret", ts.URL, nil)
	_, err := i.Introspect(context.Background(), "expired-token")
	if !errors.Is(err, oauth.ErrInactive) {
		t.Fatalf("want ErrInactive for expired token, got %v", err)
	}
}

func TestMBIntrospector_ClientCredentialsToken(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"active":     true,
			"sub":        "-1",
			"expires_at": time.Now().Add(time.Hour).Unix(),
			"scope":      []string{"profile"},
		})
	}))
	defer ts.Close()

	i := oauth.NewMBIntrospector("client-id", "client-secret", ts.URL, nil)
	_, err := i.Introspect(context.Background(), "cc-token")
	if !errors.Is(err, oauth.ErrInactive) {
		t.Fatalf("want ErrInactive for client-credentials token, got %v", err)
	}
}

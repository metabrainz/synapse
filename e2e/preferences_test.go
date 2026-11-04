//go:build integration

package e2e_test

import (
	"net/http"
	"testing"
)

func TestPreferences(t *testing.T) {
	e := setup(t)
	e.stub.grant("tok-alice", "alice")
	e.stub.grant("tok-bob", "bob")

	get := func(token string) bool {
		t.Helper()
		var out struct {
			CrossTenant bool `json:"cross_tenant_notifications"`
		}
		decodeJSON(t, e.userDo("GET", "/v1/me/preferences", nil, token), &out)
		return out.CrossTenant
	}
	rows := func() (n int) {
		e.pool.QueryRow(e.ctx, `SELECT count(*) FROM user_preferences`).Scan(&n)
		return n
	}
	patch := func(token string, body map[string]any) (int, bool) {
		t.Helper()
		r := e.userDo("PATCH", "/v1/me/preferences", body, token)
		var out struct {
			CrossTenant bool `json:"cross_tenant_notifications"`
		}
		status := r.StatusCode
		if status == http.StatusOK {
			decodeJSON(t, r, &out)
		} else {
			r.Body.Close()
		}
		return status, out.CrossTenant
	}

	t.Run("defaults without a row", func(t *testing.T) {
		if get("tok-alice") {
			t.Fatal("cross_tenant_notifications must default to false")
		}
		if rows() != 0 {
			t.Fatal("a GET must not create a row")
		}
	})

	t.Run("the first change creates the row and returns the result", func(t *testing.T) {
		status, on := patch("tok-alice", map[string]any{"cross_tenant_notifications": true})
		if status != http.StatusOK || !on {
			t.Fatalf("want 200 and true, got %d %v", status, on)
		}
		if rows() != 1 || !get("tok-alice") {
			t.Fatal("the change was not stored")
		}
	})

	t.Run("only the given fields change", func(t *testing.T) {
		if status, on := patch("tok-alice", map[string]any{}); status != http.StatusOK || !on {
			t.Fatalf("an empty patch must leave the value alone, got %d %v", status, on)
		}
		if status, on := patch("tok-alice", map[string]any{"cross_tenant_notifications": false}); status != http.StatusOK || on {
			t.Fatalf("false must be stored, not treated as absent, got %d %v", status, on)
		}
	})

	t.Run("unknown fields and wrong types are rejected", func(t *testing.T) {
		for name, body := range map[string]map[string]any{
			"unknown field": {"mute": true},
			"wrong type":    {"cross_tenant_notifications": "yes"},
		} {
			if status, _ := patch("tok-alice", body); status != http.StatusUnprocessableEntity {
				t.Errorf("%s: want 422, got %d", name, status)
			}
		}
	})

	t.Run("preferences are per user", func(t *testing.T) {
		patch("tok-alice", map[string]any{"cross_tenant_notifications": true})
		if get("tok-bob") {
			t.Fatal("alice's preference leaked to bob")
		}
	})
}

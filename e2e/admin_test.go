//go:build integration

package e2e_test

import (
	"net/http"
	"testing"
)

func TestTenantConflict(t *testing.T) {
	e := setup(t)
	e.createTenant("t1", "Tenant One")

	resp := e.adminDo("POST", "/v1/admin/tenants", map[string]string{"id": "t1", "name": "Tenant One Again"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate tenant: want 409, got %d", resp.StatusCode)
	}
}

func TestTenantRotateKey(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("t2", "Tenant Two")
	e.registerEventType("t2", "ping")

	// Old key works.
	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "u", "event_type": "ping", "payload": map[string]string{}},
		apiKey,
	)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("old key before rotate: want 202, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Rotate.
	rot := e.adminDo("POST", "/v1/admin/tenants/t2/rotate-key", nil)
	var rotOut map[string]string
	decodeJSON(t, rot, &rotOut)
	newKey := rotOut["api_key"]
	if newKey == "" || newKey == apiKey {
		t.Fatalf("expected a new non-empty key different from the old one")
	}

	// Old key now rejected.
	resp2 := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "u", "event_type": "ping", "payload": map[string]string{}},
		apiKey,
	)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("old key after rotate: want 401, got %d", resp2.StatusCode)
	}

	// New key works.
	resp3 := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "u", "event_type": "ping", "payload": map[string]string{}},
		newKey,
	)
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusAccepted {
		t.Fatalf("new key after rotate: want 202, got %d", resp3.StatusCode)
	}
}

func TestEventTypeCRUD(t *testing.T) {
	e := setup(t)
	e.createTenant("t3", "Tenant Three")

	e.registerEventType("t3", "foo.created")
	e.registerEventType("t3", "foo.deleted")

	// List — both types present.
	resp := e.adminDo("GET", "/v1/admin/tenants/t3/event-types", nil)
	var types []map[string]string
	decodeJSON(t, resp, &types)
	if len(types) != 2 {
		t.Fatalf("list: want 2 event types, got %d", len(types))
	}

	// Upsert — idempotent re-register with new description.
	resp2 := e.adminDo("POST", "/v1/admin/tenants/t3/event-types",
		map[string]string{"event_type": "foo.created", "description": "updated"},
	)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("upsert: want 201, got %d", resp2.StatusCode)
	}

	// Delete one.
	resp3 := e.adminDo("DELETE", "/v1/admin/tenants/t3/event-types/foo.deleted", nil)
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: want 204, got %d", resp3.StatusCode)
	}

	// List again — only one remains.
	resp4 := e.adminDo("GET", "/v1/admin/tenants/t3/event-types", nil)
	var remaining []map[string]string
	decodeJSON(t, resp4, &remaining)
	if len(remaining) != 1 || remaining[0]["event_type"] != "foo.created" {
		t.Fatalf("after delete: want [foo.created], got %v", remaining)
	}
}

func TestAdminKeyRequired(t *testing.T) {
	e := setup(t)

	// No key.
	resp := e.do("POST", "/v1/admin/tenants", map[string]string{"id": "x", "name": "X"}, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("no admin key: want 403, got %d", resp.StatusCode)
	}

	// Wrong key.
	resp2 := e.do("POST", "/v1/admin/tenants",
		map[string]string{"id": "x", "name": "X"},
		map[string]string{"X-Admin-Key": "wrong"},
	)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong admin key: want 403, got %d", resp2.StatusCode)
	}
}

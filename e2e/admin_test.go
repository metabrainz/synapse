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

func TestChannelRulesCRUD(t *testing.T) {
	e := setup(t)
	e.createTenant("cr1", "ChannelRules")

	// Upsert a rule.
	e.registerChannelRule("cr1", "listen", "webhook")

	// List — rule present and allowed.
	resp := e.adminDo("GET", "/v1/admin/tenants/cr1/channel-rules", nil)
	var rules []map[string]any
	decodeJSON(t, resp, &rules)
	if len(rules) != 1 {
		t.Fatalf("list: want 1 rule, got %d", len(rules))
	}
	if rules[0]["event_type"] != "listen" || rules[0]["channel_type"] != "webhook" {
		t.Fatalf("unexpected rule: %v", rules[0])
	}
	if rules[0]["is_allowed"] != true {
		t.Fatalf("want is_allowed=true, got %v", rules[0]["is_allowed"])
	}

	// Upsert same rule with is_allowed=false (disable).
	resp2 := e.adminDo("PUT", "/v1/admin/tenants/cr1/channel-rules",
		map[string]any{"event_type": "listen", "channel_type": "webhook", "is_allowed": false},
	)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("disable: want 204, got %d", resp2.StatusCode)
	}

	resp3 := e.adminDo("GET", "/v1/admin/tenants/cr1/channel-rules", nil)
	var updated []map[string]any
	decodeJSON(t, resp3, &updated)
	if updated[0]["is_allowed"] != false {
		t.Fatalf("after disable: want is_allowed=false, got %v", updated[0]["is_allowed"])
	}

	// Delete the rule.
	resp4 := e.adminDo("DELETE", "/v1/admin/tenants/cr1/channel-rules/listen/webhook", nil)
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: want 204, got %d", resp4.StatusCode)
	}

	resp5 := e.adminDo("GET", "/v1/admin/tenants/cr1/channel-rules", nil)
	var empty []map[string]any
	decodeJSON(t, resp5, &empty)
	if len(empty) != 0 {
		t.Fatalf("after delete: want 0 rules, got %d", len(empty))
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

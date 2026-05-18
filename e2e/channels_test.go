//go:build integration

package e2e_test

import (
	"fmt"
	"net/http"
	"testing"
)

func TestChannelCRUD(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("ch1", "ChanTest")

	id := e.createWebhookChannel(apiKey, "user-1", "https://example.com/wh")

	// List.
	resp := e.tenantDo("GET", "/v1/users/user-1/channels", nil, apiKey)
	var list []map[string]any
	decodeJSON(t, resp, &list)
	if len(list) != 1 {
		t.Fatalf("list: want 1 channel, got %d", len(list))
	}
	if int64(list[0]["id"].(float64)) != id {
		t.Fatalf("channel ID mismatch: want %d, got %v", id, list[0]["id"])
	}

	// Delete.
	resp2 := e.tenantDo("DELETE", fmt.Sprintf("/v1/users/user-1/channels/%d", id), nil, apiKey)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: want 204, got %d", resp2.StatusCode)
	}

	// List after delete — empty.
	resp3 := e.tenantDo("GET", "/v1/users/user-1/channels", nil, apiKey)
	var list2 []map[string]any
	decodeJSON(t, resp3, &list2)
	if len(list2) != 0 {
		t.Fatalf("after delete: want 0 channels, got %d", len(list2))
	}
}

func TestChannelDuplicateRejected(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("ch2", "ChanDup")
	e.createWebhookChannel(apiKey, "user-1", "https://example.com/wh")

	// The unique constraint covers (tenant_id, user_id, type, config), so
	// registering the exact same destination twice must be rejected.
	resp := e.tenantDo("POST", "/v1/users/user-1/channels",
		map[string]any{"type": "webhook", "config": map[string]string{"url": "https://example.com/wh"}},
		apiKey,
	)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate channel: want 409, got %d", resp.StatusCode)
	}
}

func TestChannelTenantIsolation(t *testing.T) {
	e := setup(t)
	keyA := e.createTenant("iso-a", "Tenant A")
	keyB := e.createTenant("iso-b", "Tenant B")

	e.createWebhookChannel(keyA, "user-1", "https://example.com/a")

	// Tenant B cannot see tenant A's channels.
	resp := e.tenantDo("GET", "/v1/users/user-1/channels", nil, keyB)
	var list []map[string]any
	decodeJSON(t, resp, &list)
	if len(list) != 0 {
		t.Fatalf("tenant isolation: tenant B sees %d channel(s) belonging to tenant A", len(list))
	}
}

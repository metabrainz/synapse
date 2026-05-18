//go:build integration

package e2e_test

import (
	"fmt"
	"net/http"
	"testing"
)

func TestSubscriptionCRUD(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("sub1", "SubTest")
	chanID := e.createWebhookChannel(apiKey, "user-1", "https://example.com/wh")

	e.createSubscription(apiKey, "user-1", chanID, "order.created")

	// List.
	resp := e.tenantDo("GET", fmt.Sprintf("/v1/users/user-1/channels/%d/subscriptions", chanID), nil, apiKey)
	var list []map[string]any
	decodeJSON(t, resp, &list)
	if len(list) != 1 {
		t.Fatalf("list: want 1 subscription, got %d", len(list))
	}
	if list[0]["event_type"] != "order.created" {
		t.Fatalf("unexpected event_type: %v", list[0]["event_type"])
	}

	// Delete.
	resp2 := e.tenantDo(
		"DELETE",
		fmt.Sprintf("/v1/users/user-1/channels/%d/subscriptions/order.created", chanID),
		nil, apiKey,
	)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: want 204, got %d", resp2.StatusCode)
	}

	// List after delete — empty.
	resp3 := e.tenantDo("GET", fmt.Sprintf("/v1/users/user-1/channels/%d/subscriptions", chanID), nil, apiKey)
	var list2 []map[string]any
	decodeJSON(t, resp3, &list2)
	if len(list2) != 0 {
		t.Fatalf("after delete: want 0 subscriptions, got %d", len(list2))
	}
}

func TestSubscriptionDuplicateRejected(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("sub2", "SubDup")
	chanID := e.createWebhookChannel(apiKey, "user-1", "https://example.com/wh")
	e.createSubscription(apiKey, "user-1", chanID, "ping")

	resp := e.tenantDo(
		"POST",
		fmt.Sprintf("/v1/users/user-1/channels/%d/subscriptions", chanID),
		map[string]string{"event_type": "ping"},
		apiKey,
	)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate subscription: want 409, got %d", resp.StatusCode)
	}
}

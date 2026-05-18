//go:build integration

package e2e_test

import (
	"fmt"
	"testing"
)

func TestDeliveriesListByEvent(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("del1", "DeliveryList")
	e.registerEventType("del1", "item.created")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "item.created")
	e.waitForCacheWarm(apiKey, "user-1", "item.created")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "item.created", "payload": map[string]string{"k": "v"}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	if resp.StatusCode != 202 {
		t.Fatalf("ingest: want 202, got %d", resp.StatusCode)
	}
	eventID := int64(out["event_id"].(float64))

	// Deliveries endpoint returns the rows created by the fanout.
	resp2 := e.tenantDo("GET", fmt.Sprintf("/v1/events/%d/deliveries", eventID), nil, apiKey)
	var deliveries []map[string]any
	decodeJSON(t, resp2, &deliveries)
	if resp2.StatusCode != 200 {
		t.Fatalf("list deliveries: want 200, got %d", resp2.StatusCode)
	}
	if len(deliveries) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(deliveries))
	}
	if deliveries[0]["status"] != "PENDING" {
		t.Fatalf("want status PENDING, got %v", deliveries[0]["status"])
	}
	if deliveries[0]["channel_type"] != "webhook" {
		t.Fatalf("want channel_type webhook, got %v", deliveries[0]["channel_type"])
	}
}

func TestDeliveriesEmptyWhenNoSubscribers(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("del2", "DeliveryEmpty")
	e.registerEventType("del2", "empty.event")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "empty.event", "payload": map[string]string{}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

	resp2 := e.tenantDo("GET", fmt.Sprintf("/v1/events/%d/deliveries", eventID), nil, apiKey)
	var deliveries []map[string]any
	decodeJSON(t, resp2, &deliveries)
	if len(deliveries) != 0 {
		t.Fatalf("no subscribers: want 0 deliveries, got %d", len(deliveries))
	}
}

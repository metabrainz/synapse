//go:build integration

package e2e_test

import (
	"fmt"
	"testing"
)

func TestDeliveriesListByEvent(t *testing.T) {
	e := setup(t)
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	if resp.StatusCode != 202 {
		t.Fatalf("ingest: want 202, got %d", resp.StatusCode)
	}
	eventID := int64(out["event_id"].(float64))

	resp2 := e.tenantDo("GET", fmt.Sprintf("/v1/events/%d/deliveries", eventID), nil, e.apiKey)
	var dels []map[string]any
	decodeJSON(t, resp2, &dels)
	if resp2.StatusCode != 200 {
		t.Fatalf("list deliveries: want 200, got %d", resp2.StatusCode)
	}
	if len(dels) != 1 {
		t.Fatalf("want 1 delivery, got %d", len(dels))
	}
	if dels[0]["status"] != "PENDING" {
		t.Fatalf("want status PENDING, got %v", dels[0]["status"])
	}
	if dels[0]["channel_type"] != "webhook" {
		t.Fatalf("want channel_type webhook, got %v", dels[0]["channel_type"])
	}
}

func TestDeliveriesEmptyWhenNoSubscribers(t *testing.T) {
	e := setup(t)

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

	resp2 := e.tenantDo("GET", fmt.Sprintf("/v1/events/%d/deliveries", eventID), nil, e.apiKey)
	var dels []map[string]any
	decodeJSON(t, resp2, &dels)
	if len(dels) != 0 {
		t.Fatalf("no subscribers: want 0 deliveries, got %d", len(dels))
	}
}

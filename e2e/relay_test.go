//go:build integration

package e2e_test

import (
	"testing"

	"github.com/metabrainz/synapse/internal/store/outbox"
)

func TestRelayPublishesAndClearsOutbox(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("rel1", "RelayTest")
	e.registerEventType("rel1", "order.placed")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "order.placed")
	e.waitForCacheWarm(apiKey, "user-1", "order.placed")

	e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "order.placed", "payload": map[string]string{}},
		apiKey,
	).Body.Close()

	// After ingest, one row sits pending in the outbox.
	pending, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("before relay: want 1 pending outbox row, got %d", len(pending))
	}

	published := e.relayTick()
	if published != 1 {
		t.Fatalf("relayTick: want 1 confirmed, got %d", published)
	}

	// After relay, the outbox row must be gone.
	pending2, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch pending after relay: %v", err)
	}
	if len(pending2) != 0 {
		t.Fatalf("after relay: want 0 pending outbox rows, got %d", len(pending2))
	}
}

func TestRelayIdleIsNoop(t *testing.T) {
	e := setup(t)
	published := e.relayTick()
	if published != 0 {
		t.Fatalf("empty outbox: want 0 published, got %d", published)
	}
}

func TestRelayMultipleSubscribers(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("rel2", "RelayMulti")
	e.registerEventType("rel2", "ping")

	// Two subscribers for the same event.
	ch1 := e.createWebhookChannel(apiKey, "user-1", "https://example.com/h1")
	ch2 := e.createWebhookChannel(apiKey, "user-2", "https://example.com/h2")
	e.createSubscription(apiKey, "user-1", ch1, "ping")
	e.createSubscription(apiKey, "user-2", ch2, "ping")
	e.waitForCacheWarm(apiKey, "user-1", "ping")
	e.waitForCacheWarm(apiKey, "user-2", "ping")

	e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "ping", "payload": map[string]string{}},
		apiKey,
	).Body.Close()
	e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-2", "event_type": "ping", "payload": map[string]string{}},
		apiKey,
	).Body.Close()

	pending, _ := outbox.FetchPending(e.ctx, e.pool, 10)
	if len(pending) != 2 {
		t.Fatalf("want 2 pending outbox rows, got %d", len(pending))
	}

	published := e.relayTick()
	if published != 2 {
		t.Fatalf("want 2 published, got %d", published)
	}

	pending2, _ := outbox.FetchPending(e.ctx, e.pool, 10)
	if len(pending2) != 0 {
		t.Fatalf("want 0 pending after relay, got %d", len(pending2))
	}
}

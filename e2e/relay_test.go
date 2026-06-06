//go:build integration

package e2e_test

import (
	"testing"

	"github.com/metabrainz/synapse/internal/store/outbox"
)

func TestRelayPublishesAndClearsOutbox(t *testing.T) {
	e := setup(t)
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"user-1"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	).Body.Close()

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

	// Two users subscribed to the same event type.
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/h1")
	e.setupWebhookChannel(testTenantID, "user-2", "listen", "https://example.com/h2")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")
	e.waitForCacheWarm(e.apiKey, "user-2", "listen")

	e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"user-1"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	).Body.Close()
	e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"user-2"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
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

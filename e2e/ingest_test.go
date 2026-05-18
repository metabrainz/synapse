//go:build integration

package e2e_test

import (
	"net/http"
	"testing"

	"github.com/metabrainz/synapse/internal/store/outbox"
)

// ── Auth ──────────────────────────────────────────────────────────────────────

func TestIngestRequiresAuth(t *testing.T) {
	e := setup(t)

	resp := e.do("POST", "/v1/events",
		map[string]any{"user_id": "u", "event_type": "ping"},
		nil,
	)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no auth header: want 401, got %d", resp.StatusCode)
	}

	resp2 := e.do("POST", "/v1/events",
		map[string]any{"user_id": "u", "event_type": "ping"},
		map[string]string{"Authorization": "Bearer wrong-key"},
	)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong api key: want 401, got %d", resp2.StatusCode)
	}
}

// ── Validation ────────────────────────────────────────────────────────────────

func TestIngestMissingFields(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("v1", "Validation")
	e.registerEventType("v1", "ping")

	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing user_id", map[string]any{"event_type": "ping"}},
		{"missing event_type", map[string]any{"user_id": "u"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := e.tenantDo("POST", "/v1/events", tc.body, apiKey)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s: want 400, got %d", tc.name, resp.StatusCode)
			}
		})
	}
}

func TestIngestUnregisteredEventType(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("v2", "Validation2")
	// Intentionally skip registerEventType.

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "u", "event_type": "not.registered", "payload": map[string]string{}},
		apiKey,
	)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unregistered event type: want 400, got %d", resp.StatusCode)
	}
}

// ── Core ingest behaviour ─────────────────────────────────────────────────────

func TestIngestNoSubscribers(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("ns1", "NoSubs")
	e.registerEventType("ns1", "ping")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "u", "event_type": "ping", "payload": map[string]string{}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202, got %d", resp.StatusCode)
	}
	if out["delivery_count"].(float64) != 0 {
		t.Fatalf("want delivery_count=0, got %v", out["delivery_count"])
	}

	// No outbox rows — nothing to relay.
	msgs, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("want empty outbox, got %d rows", len(msgs))
	}
}

func TestIngestCreatesDeliveryAndOutboxRows(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("lb", "ListenBrainz")
	e.registerEventType("lb", "listen")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "listen")
	e.waitForCacheWarm(apiKey, "user-1", "listen")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{
			"user_id":    "user-1",
			"event_type": "listen",
			"payload":    map[string]string{"track": "Pyramid Song"},
		},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202, got %d", resp.StatusCode)
	}
	if out["delivery_count"].(float64) != 1 {
		t.Fatalf("want delivery_count=1, got %v", out["delivery_count"])
	}

	msgs, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 outbox row, got %d", len(msgs))
	}
	if msgs[0].RoutingKey != "webhook" {
		t.Fatalf("want routing key 'webhook', got %q", msgs[0].RoutingKey)
	}
}

func TestIngestWildcardSubscription(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("wc1", "WildCard")
	e.registerEventType("wc1", "a.created")
	e.registerEventType("wc1", "b.updated")

	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/wh")
	e.createSubscription(apiKey, "user-1", ch, "*")
	e.waitForCacheWarm(apiKey, "user-1", "a.created")

	// Both event types should match the wildcard subscription.
	for _, et := range []string{"a.created", "b.updated"} {
		resp := e.tenantDo("POST", "/v1/events",
			map[string]any{"user_id": "user-1", "event_type": et, "payload": map[string]string{}},
			apiKey,
		)
		var out map[string]any
		decodeJSON(t, resp, &out)
		if out["delivery_count"].(float64) != 1 {
			t.Fatalf("%s: want delivery_count=1, got %v", et, out["delivery_count"])
		}
	}
}

// ── Idempotency ───────────────────────────────────────────────────────────────

func TestIngestIdempotency(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("mb", "MusicBrainz")
	e.registerEventType("mb", "edit.created")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "edit.created")
	e.waitForCacheWarm(apiKey, "user-1", "edit.created")

	payload := map[string]any{
		"user_id":         "user-1",
		"event_type":      "edit.created",
		"payload":         map[string]string{"edit_id": "42"},
		"idempotency_key": "edit-42-v1",
	}

	resp1 := e.tenantDo("POST", "/v1/events", payload, apiKey)
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("first request: want 202, got %d", resp1.StatusCode)
	}
	resp1.Body.Close()

	resp2 := e.tenantDo("POST", "/v1/events", payload, apiKey)
	var out map[string]any
	decodeJSON(t, resp2, &out)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("duplicate request: want 200, got %d", resp2.StatusCode)
	}
	if out["deduplicated"] != true {
		t.Fatalf("want deduplicated=true, got %v", out["deduplicated"])
	}

	// Only one event row in DB.
	var count int
	e.pool.QueryRow(e.ctx, `SELECT COUNT(*) FROM events WHERE tenant_id = 'mb'`).Scan(&count)
	if count != 1 {
		t.Fatalf("want 1 event in DB, got %d", count)
	}
}

// ── Dry run ───────────────────────────────────────────────────────────────────

func TestIngestDryRun(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("bb", "BookBrainz")
	e.registerEventType("bb", "review.created")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "review.created")
	e.waitForCacheWarm(apiKey, "user-1", "review.created")

	resp := e.tenantDo("POST", "/v1/events?dry_run=true",
		map[string]any{"user_id": "user-1", "event_type": "review.created", "payload": map[string]string{}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if out["delivery_count"].(float64) != 1 {
		t.Fatalf("want delivery_count=1, got %v", out["delivery_count"])
	}

	// Dry run must not write anything.
	msgs, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("dry_run wrote %d outbox rows, want 0", len(msgs))
	}
}

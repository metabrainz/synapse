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
		map[string]any{"recipients": []string{"u"}, "event_type": "listen"},
		nil,
	)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no auth header: want 401, got %d", resp.StatusCode)
	}

	resp2 := e.do("POST", "/v1/events",
		map[string]any{"recipients": []string{"u"}, "event_type": "listen"},
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

	cases := []struct {
		name string
		body map[string]any
	}{
		{"missing recipients", map[string]any{"event_type": "listen"}},
		{"missing event_type", map[string]any{"recipients": []string{"u"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := e.tenantDo("POST", "/v1/events", tc.body, e.apiKey)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s: want 400, got %d", tc.name, resp.StatusCode)
			}
		})
	}
}

// ── Core ingest behaviour ─────────────────────────────────────────────────────

func TestIngestNoSubscribers(t *testing.T) {
	e := setup(t)

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"u"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202, got %d", resp.StatusCode)
	}
	if out["delivery_count"].(float64) != 0 {
		t.Fatalf("want delivery_count=0, got %v", out["delivery_count"])
	}

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
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{
			"recipients": []string{"user-1"},
			"event_type": "listen",
			"payload":    testListenPayload(),
		},
		e.apiKey,
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

// ── Idempotency ───────────────────────────────────────────────────────────────

func TestIngestIdempotency(t *testing.T) {
	e := setup(t)
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	payload := map[string]any{
		"recipients":      []string{"user-1"},
		"event_type":      "listen",
		"payload":         testListenPayload(),
		"idempotency_key": "listen-42-key",
	}

	resp1 := e.tenantDo("POST", "/v1/events", payload, e.apiKey)
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("first request: want 202, got %d", resp1.StatusCode)
	}
	resp1.Body.Close()

	resp2 := e.tenantDo("POST", "/v1/events", payload, e.apiKey)
	var out map[string]any
	decodeJSON(t, resp2, &out)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("duplicate request: want 200, got %d", resp2.StatusCode)
	}
	if out["deduplicated"] != true {
		t.Fatalf("want deduplicated=true, got %v", out["deduplicated"])
	}

	var count int
	e.pool.QueryRow(e.ctx, `SELECT COUNT(*) FROM events WHERE tenant_id = $1`, testTenantID).Scan(&count)
	if count != 1 {
		t.Fatalf("want 1 event in DB, got %d", count)
	}
}

// ── Dry run ───────────────────────────────────────────────────────────────────

func TestIngestDryRun(t *testing.T) {
	e := setup(t)
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	resp := e.tenantDo("POST", "/v1/events?dry_run=true",
		map[string]any{"recipients": []string{"user-1"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	if out["delivery_count"].(float64) != 1 {
		t.Fatalf("want delivery_count=1, got %v", out["delivery_count"])
	}

	msgs, err := outbox.FetchPending(e.ctx, e.pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("dry_run wrote %d outbox rows, want 0", len(msgs))
	}
}

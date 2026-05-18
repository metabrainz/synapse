//go:build integration

package e2e_test

import (
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/cleanup"
	"github.com/metabrainz/synapse/internal/store/deliveries"
)

// TestCleanupReconcileStale verifies that ReconcileStale marks stuck deliveries
// as DEAD. We back-date rows directly in SQL to simulate deliveries that have
// been PENDING far longer than the threshold.
func TestCleanupReconcileStale(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("cl1", "Cleanup")
	e.registerEventType("cl1", "stuck.event")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "stuck.event")
	e.waitForCacheWarm(apiKey, "user-1", "stuck.event")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "stuck.event", "payload": map[string]string{}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

	// Backdate the delivery so it looks like it has been PENDING for 2 hours.
	if _, err := e.pool.Exec(e.ctx,
		`UPDATE deliveries SET created_at = NOW() - INTERVAL '2 hours' WHERE event_id = $1`,
		eventID,
	); err != nil {
		t.Fatalf("backdate delivery: %v", err)
	}

	// Threshold: PENDING > 1 hour → DEAD.
	if err := cleanup.ReconcileStale(e.ctx, e.pool, time.Hour, 3*time.Hour); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	list, err := deliveries.ListByEvent(e.ctx, e.pool, eventID)
	if err != nil || len(list) == 0 {
		t.Fatalf("list deliveries: %v", err)
	}
	if list[0].Status != deliveries.StatusDead {
		t.Fatalf("want status DEAD, got %q", list[0].Status)
	}
}

func TestCleanupReconcileDoesNotTouchRecentDeliveries(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("cl2", "CleanupRecent")
	e.registerEventType("cl2", "fresh.event")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "fresh.event")
	e.waitForCacheWarm(apiKey, "user-1", "fresh.event")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "fresh.event", "payload": map[string]string{}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

	// Reconcile with a very long threshold — no delivery should be marked DEAD.
	if err := cleanup.ReconcileStale(e.ctx, e.pool, 24*time.Hour, 72*time.Hour); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	list, _ := deliveries.ListByEvent(e.ctx, e.pool, eventID)
	if len(list) > 0 && list[0].Status == deliveries.StatusDead {
		t.Fatal("recent delivery should not be marked DEAD")
	}
}

func TestCleanupPruneOldEvents(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("cl3", "CleanupPrune")
	e.registerEventType("cl3", "old.event")

	// Ingest two events: one old, one recent.
	e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "u1", "event_type": "old.event", "payload": map[string]string{}},
		apiKey,
	).Body.Close()
	e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "u2", "event_type": "old.event", "payload": map[string]string{}},
		apiKey,
	).Body.Close()

	// Backdate the first event so it falls outside the retention window.
	if _, err := e.pool.Exec(e.ctx,
		`UPDATE events SET created_at = NOW() - INTERVAL '8 days'
		 WHERE tenant_id = 'cl3' AND user_id = 'u1'`,
	); err != nil {
		t.Fatalf("backdate event: %v", err)
	}

	// Prune events older than 7 days.
	if err := cleanup.PruneOldEvents(e.ctx, e.pool, 7*24*time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}

	var count int
	e.pool.QueryRow(e.ctx, `SELECT COUNT(*) FROM events WHERE tenant_id = 'cl3'`).Scan(&count)
	if count != 1 {
		t.Fatalf("want 1 event remaining, got %d", count)
	}
}

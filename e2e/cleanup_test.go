//go:build integration

package e2e_test

import (
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/cleanup"
	"github.com/metabrainz/synapse/internal/store/deliveries"
)

func TestCleanupReconcileStale(t *testing.T) {
	e := setup(t)
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"user-1"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

	if _, err := e.pool.Exec(e.ctx,
		`UPDATE deliveries SET created_at = NOW() - INTERVAL '2 hours' WHERE event_id = $1`,
		eventID,
	); err != nil {
		t.Fatalf("backdate delivery: %v", err)
	}

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
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"user-1"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

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

	resp1 := e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"u1"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	)
	var out1 map[string]any
	decodeJSON(t, resp1, &out1)
	oldEventID := int64(out1["event_id"].(float64))

	e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"u2"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	).Body.Close()

	if _, err := e.pool.Exec(e.ctx,
		`UPDATE events SET created_at = NOW() - INTERVAL '8 days' WHERE id = $1`,
		oldEventID,
	); err != nil {
		t.Fatalf("backdate event: %v", err)
	}

	if err := cleanup.PruneOldEvents(e.ctx, e.pool, 7*24*time.Hour); err != nil {
		t.Fatalf("prune: %v", err)
	}

	var count int
	e.pool.QueryRow(e.ctx, `SELECT COUNT(*) FROM events WHERE tenant_id = $1`, testTenantID).Scan(&count)
	if count != 1 {
		t.Fatalf("want 1 event remaining, got %d", count)
	}
}

func TestCleanupPruneExpiredNotifications(t *testing.T) {
	e := setup(t)

	var eventID int64
	for range 3 {
		resp := e.tenantDo("POST", "/v1/events", map[string]any{
			"recipients": []string{"alice"},
			"event_type": "follow",
			"payload":    map[string]any{"actor": map[string]any{"username": "x", "url": "https://listenbrainz.org/user/x"}},
		}, e.apiKey)
		var out map[string]any
		decodeJSON(t, resp, &out)
		eventID = int64(out["event_id"].(float64))
	}
	// Expire two of the three; a batch size of 1 forces more than one batch.
	e.mustExec(`UPDATE notifications SET expires_at = now() - interval '1 minute'
		WHERE id IN (SELECT id FROM notifications ORDER BY id LIMIT 2)`)

	n, err := cleanup.PruneExpired(e.ctx, e.pool, 1)
	if err != nil {
		t.Fatalf("prune expired: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 notifications deleted, got %d", n)
	}
	var left, events int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM notifications`).Scan(&left)
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM events WHERE id = $1`, eventID).Scan(&events)
	if left != 1 {
		t.Fatalf("want the unexpired notification to remain, got %d", left)
	}
	if events != 1 {
		t.Fatal("deleting notifications must not delete events")
	}
}

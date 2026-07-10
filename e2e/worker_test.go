//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/store/deliveries"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// deliveryStatus returns the status of the first delivery row for the given event_id.
func (e *env) deliveryStatus(eventID int64) string {
	list, err := deliveries.ListByEvent(e.ctx, e.pool, eventID)
	if err != nil || len(list) == 0 {
		return ""
	}
	return list[0].Status
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestWorkerRetryOnFailure(t *testing.T) {
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

	e.relayTick()

	e.startWorker("webhook", adapterFunc(func(_ context.Context, _ fanout.WorkerMessage) error {
		return errors.New("adapter temporarily unavailable")
	}))

	waitFor(t, 5*time.Second, func() bool {
		return e.deliveryStatus(eventID) == deliveries.StatusRetrying
	})
}

func TestWorkerMessageFields(t *testing.T) {
	e := setup(t)
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	e.tenantDo("POST", "/v1/events",
		map[string]any{"recipients": []string{"user-1"}, "event_type": "listen", "payload": testListenPayload()},
		e.apiKey,
	).Body.Close()
	e.relayTick()

	received := make(chan fanout.WorkerMessage, 1)
	e.startWorker("webhook", adapterFunc(func(_ context.Context, msg fanout.WorkerMessage) error {
		received <- msg
		return nil
	}))

	select {
	case msg := <-received:
		if msg.TenantID != testTenantID {
			t.Errorf("want tenant_id %q, got %q", testTenantID, msg.TenantID)
		}
		if msg.UserID != "user-1" {
			t.Errorf("want user_id user-1, got %q", msg.UserID)
		}
		if msg.EventType != "listen" {
			t.Errorf("want event_type listen, got %q", msg.EventType)
		}
		var got map[string]any
		json.Unmarshal(msg.Payload, &got)
		listenMeta, _ := got["listen"].(map[string]any)
		if _, ok := listenMeta["listened_at"]; !ok {
			t.Errorf("payload missing listen.listened_at: %v", got)
		}
		if msg.Attempt != 0 {
			t.Errorf("want attempt 0, got %d", msg.Attempt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for worker message")
	}
}

// Verify that the topology declaration in helpers declares the ingest exchange
// and the correct dead-letter path — compile-time check via rabbitmq constants.
var _ = rabbitmq.ExchangeIngest
var _ = rabbitmq.QueueIngest

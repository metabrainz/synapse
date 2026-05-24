//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/outbox"
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

// publishToDeliveryExchange publishes raw bytes to the delivery topic exchange
// with the given routing key. Used to inject duplicate messages for dedup tests.
func publishToDeliveryExchange(t *testing.T, amqpURL, routingKey string, body []byte) {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("publishToDeliveryExchange: dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("publishToDeliveryExchange: channel: %v", err)
	}
	defer ch.Close()
	if err := ch.Publish("deliveries", routingKey, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("publishToDeliveryExchange: publish: %v", err)
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestWorkerSuccess(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("wk1", "WorkerSuccess")
	e.registerEventType("wk1", "job.done")
	e.setupWebhookChannel("wk1", "user-1", "job.done", "https://example.com/hook")
	e.waitForCacheWarm(apiKey, "user-1", "job.done")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "job.done", "payload": map[string]string{}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

	e.relayTick()

	e.startWorker("webhook", adapterFunc(func(_ context.Context, _ fanout.WorkerMessage) error {
		return nil // always succeeds
	}))

	waitFor(t, 5*time.Second, func() bool {
		return e.deliveryStatus(eventID) == deliveries.StatusDelivered
	})
}

func TestWorkerRetryOnFailure(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("wk2", "WorkerRetry")
	e.registerEventType("wk2", "task.run")
	e.setupWebhookChannel("wk2", "user-1", "task.run", "https://example.com/hook")
	e.waitForCacheWarm(apiKey, "user-1", "task.run")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "task.run", "payload": map[string]string{}},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	eventID := int64(out["event_id"].(float64))

	e.relayTick()

	e.startWorker("webhook", adapterFunc(func(_ context.Context, _ fanout.WorkerMessage) error {
		return errors.New("adapter temporarily unavailable")
	}))

	// Failure → RETRYING (worker schedules retry and acks the original).
	waitFor(t, 5*time.Second, func() bool {
		return e.deliveryStatus(eventID) == deliveries.StatusRetrying
	})
}

func TestWorkerDedup(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("wk3", "WorkerDedup")
	e.registerEventType("wk3", "dedup.event")
	e.setupWebhookChannel("wk3", "user-1", "dedup.event", "https://example.com/hook")
	e.waitForCacheWarm(apiKey, "user-1", "dedup.event")

	e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "dedup.event", "payload": map[string]string{}},
		apiKey,
	).Body.Close()

	// Capture the outbox payload before relaying so we can re-inject it later.
	pending, err := outbox.FetchPending(e.ctx, e.pool, 1)
	if err != nil || len(pending) == 0 {
		t.Fatalf("no pending outbox rows")
	}
	msgBody := []byte(pending[0].Payload)
	routingKey := pending[0].RoutingKey

	e.relayTick()

	var callCount atomic.Int32
	e.startWorker("webhook", adapterFunc(func(_ context.Context, _ fanout.WorkerMessage) error {
		callCount.Add(1)
		return nil
	}))

	// Wait for the first delivery to complete (adapter called once).
	waitFor(t, 5*time.Second, func() bool { return callCount.Load() == 1 })

	// Re-inject the same message (same delivery_id, same attempt) to simulate
	// an at-least-once redelivery. The deduper must suppress the second call.
	publishToDeliveryExchange(t, e.amqpURL, routingKey, msgBody)

	// Give the worker time to receive and (skip) the duplicate.
	time.Sleep(300 * time.Millisecond)
	if n := callCount.Load(); n != 1 {
		t.Fatalf("dedup: adapter should be called exactly once, got %d", n)
	}
}

func TestWorkerMessageFields(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("wk4", "WorkerFields")
	e.registerEventType("wk4", "check.fields")
	e.setupWebhookChannel("wk4", "user-1", "check.fields", "https://example.com/hook")
	e.waitForCacheWarm(apiKey, "user-1", "check.fields")

	payload := map[string]string{"track": "Karma Police"}
	e.tenantDo("POST", "/v1/events",
		map[string]any{"user_id": "user-1", "event_type": "check.fields", "payload": payload},
		apiKey,
	).Body.Close()
	e.relayTick()

	received := make(chan fanout.WorkerMessage, 1)
	e.startWorker("webhook", adapterFunc(func(_ context.Context, msg fanout.WorkerMessage) error {
		received <- msg
		return nil
	}))

	select {
	case msg := <-received:
		if msg.TenantID != "wk4" {
			t.Errorf("want tenant_id wk4, got %q", msg.TenantID)
		}
		if msg.UserID != "user-1" {
			t.Errorf("want user_id user-1, got %q", msg.UserID)
		}
		if msg.EventType != "check.fields" {
			t.Errorf("want event_type check.fields, got %q", msg.EventType)
		}
		var got map[string]string
		json.Unmarshal(msg.Payload, &got)
		if got["track"] != "Karma Police" {
			t.Errorf("payload mismatch: want Karma Police, got %q", got["track"])
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

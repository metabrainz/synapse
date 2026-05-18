//go:build integration

package e2e_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ingest"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/outbox"
)

// startIngestConsumer runs the AMQP ingest consumer in a goroutine.
// Returns a cancel func that stops the consumer and waits for it to drain.
func (e *env) startIngestConsumer() context.CancelFunc {
	e.t.Helper()
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c := ingest.NewConsumer(e.pool, e.fan)
		c.Run(ctx, e.amqpURL, 1, 10, 50)
	}()
	e.t.Cleanup(func() { cancel(); <-done })
	return cancel
}

// publishIngestEvent publishes one message directly to the events.ingest
// exchange, bypassing the HTTP API. Used to test the AMQP ingest path.
func publishIngestEvent(t *testing.T, amqpURL string, msg ingest.Message) {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("publishIngestEvent: dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("publishIngestEvent: channel: %v", err)
	}
	defer ch.Close()
	body, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("publishIngestEvent: marshal: %v", err)
	}
	if err := ch.Publish(rabbitmq.ExchangeIngest, rabbitmq.RoutingKeyIngest, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}); err != nil {
		t.Fatalf("publishIngestEvent: publish: %v", err)
	}
}

// ── smoke tests ───────────────────────────────────────────────────────────────

// TestPipelineHTTPIngest exercises the full HTTP-ingest path:
//
//	POST /v1/events → Postgres (event+delivery+outbox) → relay → RabbitMQ → worker → adapter
func TestPipelineHTTPIngest(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("pipe1", "Pipeline")
	e.registerEventType("pipe1", "play.track")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "play.track")
	e.waitForCacheWarm(apiKey, "user-1", "play.track")

	resp := e.tenantDo("POST", "/v1/events",
		map[string]any{
			"user_id":    "user-1",
			"event_type": "play.track",
			"payload":    map[string]string{"artist": "Radiohead", "track": "Everything in Its Right Place"},
		},
		apiKey,
	)
	var out map[string]any
	decodeJSON(t, resp, &out)
	if resp.StatusCode != 202 {
		t.Fatalf("ingest: want 202, got %d", resp.StatusCode)
	}
	eventID := int64(out["event_id"].(float64))
	if out["delivery_count"].(float64) != 1 {
		t.Fatalf("want delivery_count=1, got %v", out["delivery_count"])
	}

	received := make(chan fanout.WorkerMessage, 1)
	e.startWorker("webhook", adapterFunc(func(_ context.Context, msg fanout.WorkerMessage) error {
		received <- msg
		return nil
	}))

	e.relayTick()

	select {
	case msg := <-received:
		if msg.TenantID != "pipe1" {
			t.Errorf("want tenant_id pipe1, got %q", msg.TenantID)
		}
		if msg.EventType != "play.track" {
			t.Errorf("want event_type play.track, got %q", msg.EventType)
		}
		if msg.EventID != eventID {
			t.Errorf("want event_id %d, got %d", eventID, msg.EventID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: worker never received the message")
	}

	waitFor(t, 3*time.Second, func() bool {
		return e.deliveryStatus(eventID) == deliveries.StatusDelivered
	})
}

// TestPipelineAMQPIngest exercises the AMQP-ingest path:
//
//	events.ingest exchange → ingest consumer → Postgres → relay → RabbitMQ → worker → adapter
func TestPipelineAMQPIngest(t *testing.T) {
	e := setup(t)
	apiKey := e.createTenant("pipe2", "PipelineAMQP")
	e.registerEventType("pipe2", "submit.edit")
	ch := e.createWebhookChannel(apiKey, "user-1", "https://example.com/hook")
	e.createSubscription(apiKey, "user-1", ch, "submit.edit")
	e.waitForCacheWarm(apiKey, "user-1", "submit.edit")

	e.startIngestConsumer()

	publishIngestEvent(t, e.amqpURL, ingest.Message{
		TenantID:  "pipe2",
		UserID:    "user-1",
		EventType: "submit.edit",
		Payload:   json.RawMessage(`{"edit_id":"99"}`),
	})

	// Wait for the ingest consumer to fan out and write the outbox row.
	waitFor(t, 5*time.Second, func() bool {
		msgs, _ := outbox.FetchPending(e.ctx, e.pool, 1)
		return len(msgs) == 1
	})

	received := make(chan fanout.WorkerMessage, 1)
	e.startWorker("webhook", adapterFunc(func(_ context.Context, msg fanout.WorkerMessage) error {
		received <- msg
		return nil
	}))

	e.relayTick()

	select {
	case msg := <-received:
		if msg.TenantID != "pipe2" {
			t.Errorf("want tenant_id pipe2, got %q", msg.TenantID)
		}
		if msg.EventType != "submit.edit" {
			t.Errorf("want event_type submit.edit, got %q", msg.EventType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: worker never received the AMQP-ingested message")
	}

	// Confirm the API is consistent: list events shows it was recorded.
	var count int
	e.pool.QueryRow(e.ctx, `SELECT COUNT(*) FROM events WHERE tenant_id = 'pipe2'`).Scan(&count)
	if count != 1 {
		t.Fatalf("want 1 event in DB, got %d", count)
	}

	// Check delivery is DELIVERED.
	waitFor(t, 3*time.Second, func() bool {
		var s string
		e.pool.QueryRow(e.ctx,
			`SELECT status FROM deliveries d
			 JOIN events ev ON ev.id = d.event_id
			 WHERE ev.tenant_id = 'pipe2'`,
		).Scan(&s)
		return s == deliveries.StatusDelivered
	})
}

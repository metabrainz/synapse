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
	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/outbox"
)

// startIngestConsumer runs the AMQP ingest consumer in a goroutine.
func (e *env) startIngestConsumer() context.CancelFunc {
	e.t.Helper()
	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c := ingest.NewConsumer(e.pool, e.fan, schema.New(schema.KnownTenants))
		c.Run(ctx, e.amqpURL, 1, 10, 50)
	}()
	e.t.Cleanup(func() { cancel(); <-done })
	return cancel
}

// publishIngestEvent publishes one message directly to the events.ingest exchange.
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
		if msg.TenantID != testTenantID {
			t.Errorf("want tenant_id %q, got %q", testTenantID, msg.TenantID)
		}
		if msg.EventType != "listen" {
			t.Errorf("want event_type listen, got %q", msg.EventType)
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
	e.setupWebhookChannel(testTenantID, "user-1", "listen", "https://example.com/hook")
	e.waitForCacheWarm(e.apiKey, "user-1", "listen")

	e.startIngestConsumer()

	payload, _ := json.Marshal(testListenPayload())
	publishIngestEvent(t, e.amqpURL, ingest.Message{
		TenantID:   testTenantID,
		Recipients: []string{"user-1"},
		EventType:  "listen",
		Payload:    json.RawMessage(payload),
	})

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
		if msg.TenantID != testTenantID {
			t.Errorf("want tenant_id %q, got %q", testTenantID, msg.TenantID)
		}
		if msg.EventType != "listen" {
			t.Errorf("want event_type listen, got %q", msg.EventType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: worker never received the AMQP-ingested message")
	}

	var count int
	e.pool.QueryRow(e.ctx, `SELECT COUNT(*) FROM events WHERE tenant_id = $1`, testTenantID).Scan(&count)
	if count != 1 {
		t.Fatalf("want 1 event in DB, got %d", count)
	}

	waitFor(t, 3*time.Second, func() bool {
		var s string
		e.pool.QueryRow(e.ctx,
			`SELECT status FROM deliveries d
			 JOIN events ev ON ev.id = d.event_id
			 WHERE ev.tenant_id = $1`,
			testTenantID,
		).Scan(&s)
		return s == deliveries.StatusDelivered
	})
}

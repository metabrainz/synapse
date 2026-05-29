// Package ingest consumes raw events from the RabbitMQ ingest queue, validates
// them, and fans them out into the delivery pipeline. It uses batch processing
// to amortise transaction overhead: one DB transaction handles up to BatchSize
// events. Individual constraint violations fall back to per-event processing
// so a single bad message cannot block an entire batch.
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/events"
	"golang.org/x/sync/errgroup"
)

// Message is the payload producers publish to the events.ingest exchange.
type Message struct {
	TenantID       string          `json:"tenant_id"`
	UserID         string          `json:"user_id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

// Consumer reads ingest messages from RabbitMQ and fans them out.
type Consumer struct {
	pool *pgxpool.Pool
	fan  *fanout.Fanout
	reg  *schema.Registry
}

func NewConsumer(pool *pgxpool.Pool, fan *fanout.Fanout, reg *schema.Registry) *Consumer {
	return &Consumer{pool: pool, fan: fan, reg: reg}
}

// Run starts workers goroutines, each collecting up to batchSize messages per
// transaction. Increasing batchSize amortises the fixed per-transaction overhead
// (BEGIN + events.InsertBatch + deliveries.InsertBatch + outbox.InsertBatch + COMMIT)
// across more events, so throughput scales with batchSize rather than worker count.
// drainMs controls how long each worker waits to accumulate a batch after the
// first message arrives — higher values increase batch fill at high throughput.
func (c *Consumer) Run(ctx context.Context, amqpURL string, workers, batchSize, drainMs int) error {
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			return rabbitmq.ConsumeBatchQueue(
				gctx, amqpURL, rabbitmq.QueueIngest,
				batchSize, drainMs,
				c.handleBatch,
			)
		})
	}
	return g.Wait()
}

// parseMessages decodes, field-validates, and schema-validates raw AMQP bodies,
// dropping invalid messages with a log line. All valid events share the same timestamp.
func parseMessages(bodies [][]byte, now time.Time, reg *schema.Registry) []events.Event {
	evs := make([]events.Event, 0, len(bodies))
	for _, body := range bodies {
		var msg Message
		if err := json.Unmarshal(body, &msg); err != nil {
			slog.Error("ingest: malformed message, dropping", "err", err)
			continue
		}
		if msg.TenantID == "" || msg.UserID == "" || msg.EventType == "" {
			slog.Error("ingest: missing required fields, dropping")
			continue
		}
		if msg.Payload == nil {
			msg.Payload = json.RawMessage(`{}`)
		}
		if err := reg.Validate(msg.TenantID, msg.EventType, msg.Payload); err != nil {
			slog.Error("ingest: payload schema validation failed, dropping",
				"tenant", msg.TenantID, "event_type", msg.EventType, "err", err)
			continue
		}
		evs = append(evs, events.Event{
			TenantID:       msg.TenantID,
			UserID:         msg.UserID,
			EventType:      msg.EventType,
			Payload:        msg.Payload,
			IdempotencyKey: msg.IdempotencyKey,
			CreatedAt:      now,
		})
	}
	return evs
}

// handleBatch processes one drained batch end-to-end inside a single transaction:
//  1. Parse + validate each AMQP body via parseMessages — malformed or incomplete
//     messages are logged and dropped; they will never ack so RabbitMQ won't redeliver them.
//  2. events.InsertBatch — one round-trip; returned IDs are positionally aligned.
//  3. fanout.FanBatch — resolves subscriptions, writes all deliveries (gets IDs),
//     marshals payloads, writes all outbox rows; two round-trips, N events flat.
//
// Returning an error nacks the entire batch, causing RabbitMQ to redeliver all
// messages in it. Constraint violations (duplicate idempotency key, unregistered
// event type) trigger per-event fallback processing instead of nacking — they
// signal bad data that will not fix itself on retry.
func (c *Consumer) handleBatch(ctx context.Context, bodies [][]byte) error {
	evs := parseMessages(bodies, time.Now(), c.reg)
	if len(evs) == 0 {
		return nil
	}

	var deliveryCount int
	err := store.WithTx(ctx, c.pool, func(q store.Querier) error {
		ids, err := events.InsertBatch(ctx, q, evs)
		if err != nil {
			return fmt.Errorf("insert events: %w", err)
		}
		for i := range evs {
			evs[i].ID = ids[i]
		}
		count, err := c.fan.FanBatch(ctx, q, evs)
		if err != nil {
			return fmt.Errorf("fanout: %w", err)
		}
		deliveryCount = count
		return nil
	})
	if err == nil {
		slog.Info("ingest: batch processed", "events", len(evs), "deliveries", deliveryCount)
		return nil
	}

	if store.IsUniqueViolation(err) || store.IsForeignKeyViolation(err) {
		return c.handleEach(ctx, evs)
	}
	return err
}

// handleEach is the per-event fallback when the batch path hits a constraint
// violation. Each event is processed independently so one bad event cannot
// block or poison the rest of the batch.
func (c *Consumer) handleEach(ctx context.Context, evs []events.Event) error {
	for _, ev := range evs {
		if err := c.processOne(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// processOne inserts a single event and fans it out in one transaction.
// Constraint violations are classified and dropped rather than propagated:
//   - Unique violation  → duplicate idempotency key; skip (acked).
//   - FK violation      → unregistered event type; drop (acked).
//   - Any other error   → transient failure; caller nacks the batch.
//     Already-committed events in the batch are skipped as duplicates on retry
//     if they carry an idempotency key; events without one may be inserted twice.
func (c *Consumer) processOne(ctx context.Context, ev events.Event) error {
	var count int
	err := store.WithTx(ctx, c.pool, func(q store.Querier) error {
		ev, err := events.Insert(ctx, q, ev)
		if err != nil {
			return err
		}
		n, err := c.fan.Fan(ctx, q, ev)
		if err != nil {
			return err
		}
		count = n
		return nil
	})
	switch {
	case err == nil:
		slog.Info("ingest: event processed", "tenant", ev.TenantID, "event_type", ev.EventType, "deliveries", count)
		return nil
	case store.IsUniqueViolation(err):
		slog.Info("ingest: duplicate event skipped", "tenant", ev.TenantID, "idempotency_key", ev.IdempotencyKey)
		return nil
	case store.IsForeignKeyViolation(err):
		slog.Error("ingest: unregistered event type, dropping", "tenant", ev.TenantID, "event_type", ev.EventType)
		return nil
	default:
		return err
	}
}

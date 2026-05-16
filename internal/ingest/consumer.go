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
}

func NewConsumer(pool *pgxpool.Pool, fan *fanout.Fanout) *Consumer {
	return &Consumer{pool: pool, fan: fan}
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

// handleBatch parses all bodies, drops malformed ones, then writes the valid
// set in a single transaction: one events.InsertBatch + one fanout.FanBatch.
func (c *Consumer) handleBatch(ctx context.Context, bodies [][]byte) error {
	now := time.Now()
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
		evs = append(evs, events.Event{
			TenantID:       msg.TenantID,
			UserID:         msg.UserID,
			EventType:      msg.EventType,
			Payload:        msg.Payload,
			IdempotencyKey: msg.IdempotencyKey,
			CreatedAt:      now,
		})
	}
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
	if err != nil {
		if store.IsUniqueViolation(err) {
			// Batch contains a duplicate idempotency key; nack so RabbitMQ
			// redelivers — the relay's dedup check will filter the true duplicate.
			slog.Warn("ingest: batch has duplicate idempotency key, will redeliver", "size", len(evs))
			return err
		}
		return err
	}

	slog.Info("ingest: batch processed", "events", len(evs), "deliveries", deliveryCount)
	return nil
}

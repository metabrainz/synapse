package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

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

// Run starts workers goroutines, each with its own AMQP connection and channel.
// Each goroutine processes one message at a time; workers controls DB concurrency.
func (c *Consumer) Run(ctx context.Context, amqpURL string, workers int) error {
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			return rabbitmq.ConsumeQueue(gctx, amqpURL, rabbitmq.QueueIngest, 1, c.handle)
		})
	}
	return g.Wait()
}

func (c *Consumer) handle(ctx context.Context, body []byte) error {
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("invalid ingest message: %w", err)
	}
	if msg.TenantID == "" || msg.UserID == "" || msg.EventType == "" {
		return fmt.Errorf("missing required fields (tenant_id, user_id, event_type)")
	}
	if msg.Payload == nil {
		msg.Payload = json.RawMessage(`{}`)
	}

	var eventID int64
	var deliveryCount int
	err := store.WithTx(ctx, c.pool, func(q store.Querier) error {
		ev := events.Event{
			TenantID:       msg.TenantID,
			UserID:         msg.UserID,
			EventType:      msg.EventType,
			Payload:        msg.Payload,
			IdempotencyKey: msg.IdempotencyKey,
		}
		id, err := events.Insert(ctx, q, ev)
		if err != nil {
			return err
		}
		eventID = id
		ev.ID = id
		count, err := c.fan.Fan(ctx, q, ev)
		if err != nil {
			return err
		}
		deliveryCount = count
		return nil
	})
	if err != nil {
		if store.IsUniqueViolation(err) {
			slog.Info("ingest: deduplicated", "tenant", msg.TenantID, "key", msg.IdempotencyKey)
			return nil
		}
		return err
	}

	slog.Info("ingest: processed", "tenant", msg.TenantID, "event_id", eventID, "deliveries", deliveryCount)
	return nil
}

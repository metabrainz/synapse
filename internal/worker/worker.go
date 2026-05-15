package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/store/deliveries"
)

// backoffMs returns the retry delay in milliseconds for the given attempt number.
// Exponential: 30s → 60s → 120s → 240s … capped at 30 minutes.
func backoffMs(attempt int) int64 {
	const base int64 = 30_000
	const ceiling int64 = 1_800_000
	ms := base << (attempt - 1)
	if ms > ceiling {
		return ceiling
	}
	return ms
}

// Handler returns a rabbitmq.Handler for one channel type.
// On success it Acks. On retriable failure it publishes to the retry exchange
// and Acks the original (retry is now in the retry queue). On exhausted
// retries it returns an error so the consumer Nacks → DLQ.
func Handler(
	channelType string,
	ad adapter.Adapter,
	consumer *rabbitmq.Consumer,
	deduper *dedup.Deduper,
	pool *pgxpool.Pool,
) rabbitmq.Handler {
	return func(ctx context.Context, body []byte) error {
		var msg fanout.WorkerMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			// Malformed message — reject to DLQ, no retry.
			return fmt.Errorf("unmarshal worker message: %w", err)
		}

		// Guard against at-least-once redelivery producing duplicate calls.
		seen, _ := deduper.Seen(ctx, msg.DeliveryID, msg.Attempt)
		if seen {
			slog.Info("worker: skipping duplicate", "delivery_id", msg.DeliveryID)
			return nil
		}

		err := ad.Deliver(ctx, msg)

		if err == nil {
			if dbErr := deliveries.UpdateStatus(ctx, pool, msg.DeliveryID, deliveries.StatusDelivered, msg.Attempt+1, nil); dbErr != nil {
				slog.Error("worker: mark delivered", "delivery_id", msg.DeliveryID, "err", dbErr)
			}
			slog.Info("worker: delivered", "delivery_id", msg.DeliveryID, "type", channelType, "attempt", msg.Attempt+1)
			return nil
		}

		errStr := err.Error()
		nextAttempt := msg.Attempt + 1

		slog.Warn("worker: delivery failed", "delivery_id", msg.DeliveryID, "attempt", msg.Attempt, "err", err)

		if nextAttempt >= msg.MaxAttempts {
			if dbErr := deliveries.UpdateStatus(ctx, pool, msg.DeliveryID, deliveries.StatusDead, nextAttempt, &errStr); dbErr != nil {
				slog.Error("worker: mark dead", "delivery_id", msg.DeliveryID, "err", dbErr)
			}
			slog.Error("worker: delivery dead", "delivery_id", msg.DeliveryID, "attempts", nextAttempt)
			return err // Nack → DLX → deliveries.dead.{type}
		}

		// Publish updated message (with bumped attempt) to the retry queue.
		msg.Attempt = nextAttempt
		retryBody, _ := json.Marshal(msg)
		ttl := backoffMs(nextAttempt)

		if pubErr := consumer.PublishRetry(ctx, channelType, retryBody, ttl); pubErr != nil {
			// If we can't schedule the retry, Nack so the DLQ catches it.
			slog.Error("worker: retry publish failed", "delivery_id", msg.DeliveryID, "err", pubErr)
			return pubErr
		}

		if dbErr := deliveries.UpdateStatus(ctx, pool, msg.DeliveryID, deliveries.StatusRetrying, nextAttempt, &errStr); dbErr != nil {
			slog.Error("worker: mark retrying", "delivery_id", msg.DeliveryID, "err", dbErr)
		}
		slog.Info("worker: retry scheduled", "delivery_id", msg.DeliveryID, "attempt", nextAttempt, "ttl_ms", ttl)
		return nil // Ack the original — retry is now queued
	}
}

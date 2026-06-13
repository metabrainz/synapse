package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/getsentry/sentry-go"
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
	return func(ctx context.Context, body []byte) (retErr error) {
		var msg fanout.WorkerMessage
		if err := json.Unmarshal(body, &msg); err != nil {
			// Malformed message — reject to DLQ, no retry.
			return fmt.Errorf("unmarshal worker message: %w", err)
		}

		// Rate limit check comes BEFORE dedup. If we checked dedup first, the dedup
		// key for (deliveryID, attempt) would be set, and when the re-queued message
		// comes back with the same attempt counter it would be silently dropped.
		if rl, ok := ad.(adapter.RateLimiter); ok {
			if allowed, retryAfter := rl.RateLimit(ctx, msg); !allowed {
				ttl := int64(retryAfter.Milliseconds())
				if ttl <= 0 {
					ttl = 1000
				}
				if pubErr := consumer.PublishRetry(ctx, channelType, body, ttl); pubErr != nil {
					slog.Error("worker: rate-limit retry publish failed", "delivery_id", msg.DeliveryID, "err", pubErr)
					return pubErr
				}
				slog.Info("worker: rate limited, re-queued", "delivery_id", msg.DeliveryID, "type", channelType, "ttl_ms", ttl)
				return nil
			}
		}

		// Guard against at-least-once redelivery producing duplicate calls.
		seen, _ := deduper.Seen(ctx, msg.DeliveryID, msg.Attempt)
		if seen {
			slog.Info("worker: skipping duplicate", "delivery_id", msg.DeliveryID)
			return nil
		}
		// If we Nack (return an error), the dedup key must be cleared so that
		// redelivery is not silently suppressed by a key set before work succeeded.
		defer func() {
			if retErr != nil {
				deduper.DeleteSeen(ctx, msg.DeliveryID, msg.Attempt)
			}
		}()

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
			sentry.WithScope(func(scope *sentry.Scope) {
				scope.SetTag("channel_type", channelType)
				scope.SetContext("delivery", sentry.Context{
					"delivery_id": msg.DeliveryID,
					"attempts":    nextAttempt,
				})
				sentry.CaptureException(err)
			})
			if dbErr := deliveries.UpdateStatus(ctx, pool, msg.DeliveryID, deliveries.StatusDead, nextAttempt, &errStr); dbErr != nil {
				slog.Error("worker: mark dead", "delivery_id", msg.DeliveryID, "err", dbErr)
			}
			slog.Error("worker: delivery dead", "delivery_id", msg.DeliveryID, "attempts", nextAttempt)
			return err // Nack → DLX → deliveries.dead.{type}
		}

		// Publish updated message (with bumped attempt) to the retry queue.
		msg.Attempt = nextAttempt
		retryBody, err := json.Marshal(msg)
		if err != nil {
			// WorkerMessage contains only JSON-safe types; this path is unreachable
			// in practice but we must not silently drop the message.
			slog.Error("worker: marshal retry message", "delivery_id", msg.DeliveryID, "err", err)
			return err
		}
		ttl := backoffMs(nextAttempt)
		// If the downstream API told us exactly how long to wait (e.g. Telegram's
		// retry_after on 429), respect that but never go below our own backoff.
		if d, ok := adapter.RetryAfter(err); ok {
			if ms := d.Milliseconds(); ms > ttl {
				ttl = ms
			}
		}

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

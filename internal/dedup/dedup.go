// Package dedup provides two Redis-backed deduplication checks:
//   - Delivery dedup: prevents a worker from processing the same (delivery, attempt)
//     pair twice when RabbitMQ redelivers an already-processed message.
//   - Idempotency dedup: fast-path pre-check for API-supplied idempotency keys.
//
// Both checks fail open on Redis errors — the PostgreSQL unique constraint is
// the hard backstop that prevents actual duplicate writes.
package dedup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultTTL = 30 * time.Minute

type Deduper struct {
	rdb *redis.Client
	ttl time.Duration
}

func New(rdb *redis.Client) *Deduper {
	return &Deduper{rdb: rdb, ttl: defaultTTL}
}

// Seen returns true if (deliveryID, attempt) has been seen before (worker-level dedup).
// Keying by attempt ensures legitimate retries are not mistaken for AMQP redeliveries:
// same delivery_id + same attempt = redelivery (skip); same delivery_id + new attempt = retry (process).
// Fail-open on Redis error — the PG UNIQUE constraint is the hard backstop.
func (d *Deduper) Seen(ctx context.Context, deliveryID int64, attempt int) (bool, error) {
	key := fmt.Sprintf("synapse:dedup:%d:%d", deliveryID, attempt)
	ok, err := d.rdb.SetNX(ctx, key, 1, d.ttl).Result()
	if err != nil {
		return false, nil // fail-open
	}
	return !ok, nil
}

// DeleteSeen removes the dedup key for (deliveryID, attempt).
// Call before returning an error that triggers a Nack, so redelivery is not
// suppressed by a key that was set before the work succeeded.
func (d *Deduper) DeleteSeen(ctx context.Context, deliveryID int64, attempt int) {
	if err := d.rdb.Del(ctx, fmt.Sprintf("synapse:dedup:%d:%d", deliveryID, attempt)).Err(); err != nil {
		slog.Error("dedup: failed to delete seen key — redelivery may be suppressed",
			"delivery_id", deliveryID, "attempt", attempt, "err", err)
	}
}

// SeenIdempotency checks whether (tenantID, idempotencyKey) was already processed.
// Fail-open on Redis error — the PG UNIQUE constraint catches duplicates anyway.
func (d *Deduper) SeenIdempotency(ctx context.Context, tenantID, idempotencyKey string) (bool, error) {
	key := fmt.Sprintf("synapse:idem:%s:%s", tenantID, idempotencyKey)
	exists, err := d.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, nil // fail-open
	}
	return exists > 0, nil
}

// MarkIdempotency records (tenantID, idempotencyKey) as processed.
// Called AFTER the PG transaction commits — setting it before risks losing
// the event if we crash between the Redis SET and the DB INSERT.
func (d *Deduper) MarkIdempotency(ctx context.Context, tenantID, idempotencyKey string) {
	key := fmt.Sprintf("synapse:idem:%s:%s", tenantID, idempotencyKey)
	d.rdb.Set(ctx, key, 1, d.ttl)
}

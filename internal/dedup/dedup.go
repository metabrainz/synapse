package dedup

import (
	"context"
	"fmt"
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

// Seen returns true if deliveryID has been seen before (worker-level dedup).
// Fail-open on Redis error — the PG UNIQUE constraint is the hard backstop.
func (d *Deduper) Seen(ctx context.Context, deliveryID int64) (bool, error) {
	key := fmt.Sprintf("synapse:dedup:%d", deliveryID)
	ok, err := d.rdb.SetNX(ctx, key, 1, d.ttl).Result()
	if err != nil {
		return false, nil // fail-open
	}
	return !ok, nil
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

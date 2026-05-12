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

// Seen returns true if deliveryID has been seen before.
// On Redis error, returns false (fail-open) — the PG UNIQUE constraint
// on deliveries is the hard backstop.
func (d *Deduper) Seen(ctx context.Context, deliveryID int64) (bool, error) {
	key := fmt.Sprintf("synapse:dedup:%d", deliveryID)
	ok, err := d.rdb.SetNX(ctx, key, 1, d.ttl).Result()
	if err != nil {
		return false, nil // fail-open
	}
	return !ok, nil // SetNX returns true on first set, false if already existed
}

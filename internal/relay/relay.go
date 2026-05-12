package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/broker"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/outbox"
)

const batchSize = 100

type Relay struct {
	pool   *pgxpool.Pool
	pub    broker.Publisher
	pollMs int
}

func New(pool *pgxpool.Pool, pub broker.Publisher, pollMs int) *Relay {
	return &Relay{pool: pool, pub: pub, pollMs: pollMs}
}

// Run polls the outbox on every tick until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	interval := time.Duration(r.pollMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.tick(ctx); err != nil {
				slog.Error("relay tick", "err", err)
			}
		}
	}
}

// tick fetches a batch inside a transaction so FOR UPDATE SKIP LOCKED holds
// through publish+delete. Other relay goroutines skip locked rows and claim
// their own disjoint batch — no double-publishing without relying solely on dedup.
func (r *Relay) tick(ctx context.Context) error {
	return store.WithTx(ctx, r.pool, func(q store.Querier) error {
		msgs, err := outbox.FetchPending(ctx, q, batchSize)
		if err != nil {
			return fmt.Errorf("fetch pending: %w", err)
		}
		if len(msgs) == 0 {
			return nil
		}

		var published []int64
		for _, m := range msgs {
			if err := r.pub.Publish(ctx, m.RoutingKey, m.Payload); err != nil {
				slog.Error("relay publish", "outbox_id", m.ID, "routing_key", m.RoutingKey, "err", err)
				break
			}
			published = append(published, m.ID)
		}

		if len(published) == 0 {
			return nil
		}

		if err := outbox.DeleteBatch(ctx, q, published); err != nil {
			return fmt.Errorf("delete outbox batch: %w", err)
		}

		slog.Info("relay tick", "published", len(published), "remaining", len(msgs)-len(published))
		return nil
	})
}

package relay

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/broker"
	"golang.org/x/sync/errgroup"
)

// Run starts workers parallel relay goroutines. Each goroutine creates its own
// broker.Publisher so their AMQP confirm loops are fully independent — sharing
// one publisher would serialize all workers on a single channel's confirm
// round-trip, capping throughput regardless of worker count.
// Workers claim disjoint outbox batches via FOR UPDATE SKIP LOCKED.
func Run(ctx context.Context, pool *pgxpool.Pool, newPub func() (broker.Publisher, error), workers, pollMs int) error {
	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			pub, err := newPub()
			if err != nil {
				return fmt.Errorf("relay: create publisher: %w", err)
			}
			defer pub.Close()
			return New(pool, pub, pollMs).Run(gctx)
		})
	}
	return g.Wait()
}

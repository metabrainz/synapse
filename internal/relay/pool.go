package relay

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/broker"
	"golang.org/x/sync/errgroup"
)

// Run starts n parallel relay workers. Each worker independently polls the
// outbox using FOR UPDATE SKIP LOCKED, so they claim disjoint batches and
// never double-publish. All workers stop when ctx is cancelled or any worker
// returns a non-context error.
func Run(ctx context.Context, pool *pgxpool.Pool, pub broker.Publisher, workers, pollMs int) error {
	g, ctx := errgroup.WithContext(ctx)
	for range workers {
		r := New(pool, pub, pollMs)
		g.Go(func() error { return r.Run(ctx) })
	}
	return g.Wait()
}

// Package relay moves committed outbox rows into RabbitMQ using a three-phase
// claim → publish → delete protocol that guarantees at-least-once delivery even
// across process crashes. Each relay worker holds its own AMQP channel so
// publisher-confirm round-trips are fully independent.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/broker"
	"github.com/metabrainz/synapse/internal/store/outbox"
	"golang.org/x/sync/errgroup"
)

const stuckAfter = 5 * time.Minute

// Run resets stuck outbox rows once at startup, then starts workers parallel
// relay goroutines. Each goroutine creates its own broker.Publisher so their
// AMQP confirm loops are fully independent — sharing one publisher would
// serialize all workers on a single channel's confirm round-trip.
// Workers claim disjoint outbox batches via FOR UPDATE SKIP LOCKED.
func Run(ctx context.Context, pool *pgxpool.Pool, newPub func() (broker.Publisher, error), workers, pollMs, batchSize int) error {
	if n, err := outbox.ResetStuck(ctx, pool, stuckAfter); err != nil {
		slog.Warn("relay: reset stuck rows", "err", err)
	} else if n > 0 {
		slog.Info("relay: reset stuck rows", "count", n)
	}

	g, gctx := errgroup.WithContext(ctx)
	for range workers {
		g.Go(func() error {
			pub, err := newPub()
			if err != nil {
				return fmt.Errorf("relay: create publisher: %w", err)
			}
			defer pub.Close()
			return New(pool, pub, pollMs, batchSize).Run(gctx)
		})
	}
	return g.Wait()
}

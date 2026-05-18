// Package worker runs the delivery side of the pipeline. Each channel type
// (webhook, email, …) gets its own pool of consumer goroutines. On success a
// delivery is acked; on retriable failure it is re-queued with exponential
// back-off via the RabbitMQ retry exchange; on final failure it is nacked to
// the dead-letter queue for manual inspection.
package worker

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/broker/rabbitmq"
	"github.com/metabrainz/synapse/internal/dedup"
)

// Run starts concurrency worker goroutines for channelType, each with its own
// AMQP consumer. prefetch controls how many unacked messages RabbitMQ delivers
// to each goroutine — this is the primary concurrency knob, not a semaphore.
// Blocks until ctx is cancelled or a worker fails.
func Run(
	ctx context.Context,
	channelType string,
	concurrency, prefetch int,
	amqpURL string,
	ad adapter.Adapter,
	deduper *dedup.Deduper,
	pool *pgxpool.Pool,
) error {
	g, gctx := errgroup.WithContext(ctx)

	for range concurrency {
		g.Go(func() error {
			consumer, err := rabbitmq.NewConsumer(amqpURL, channelType, prefetch)
			if err != nil {
				return fmt.Errorf("new consumer %s: %w", channelType, err)
			}
			defer consumer.Close()

			h := Handler(channelType, ad, consumer, deduper, pool)
			return consumer.Run(gctx, h)
		})
	}

	return g.Wait()
}

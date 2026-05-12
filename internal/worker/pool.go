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
// AMQP consumer and prefetch=1. Blocks until ctx is cancelled or a worker fails.
func Run(
	ctx context.Context,
	channelType string,
	concurrency int,
	amqpURL string,
	ad adapter.Adapter,
	deduper *dedup.Deduper,
	pool *pgxpool.Pool,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for range concurrency {
		g.Go(func() error {
			consumer, err := rabbitmq.NewConsumer(amqpURL, channelType, 1)
			if err != nil {
				return fmt.Errorf("new consumer %s: %w", channelType, err)
			}
			defer consumer.Close()

			h := Handler(channelType, ad, consumer, deduper, pool)
			return consumer.Run(ctx, h)
		})
	}

	return g.Wait()
}

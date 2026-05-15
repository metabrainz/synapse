package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/metabrainz/synapse/internal/broker"
	"github.com/metabrainz/synapse/internal/store/outbox"
)

const (
	batchSize      = 100
	stuckAfter     = 5 * time.Minute
	publishTimeout = 5 * time.Second
)

type Relay struct {
	pool     *pgxpool.Pool
	pub      broker.Publisher
	pollMs   int
	workerID string
}

func New(pool *pgxpool.Pool, pub broker.Publisher, pollMs int) *Relay {
	return &Relay{
		pool:     pool,
		pub:      pub,
		pollMs:   pollMs,
		workerID: uuid.NewString(),
	}
}

// Run polls the outbox on every tick until ctx is cancelled.
// On startup it resets any rows this process left stuck from a prior crash.
func (r *Relay) Run(ctx context.Context) error {
	if n, err := outbox.ResetStuck(ctx, r.pool, stuckAfter); err != nil {
		slog.Warn("relay: reset stuck rows", "err", err)
	} else if n > 0 {
		slog.Info("relay: reset stuck rows", "count", n)
	}

	interval := time.Duration(r.pollMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.tick(ctx); err != nil {
				slog.Error("relay tick", "worker", r.workerID, "err", err)
			}
		}
	}
}

// tick implements the three-phase outbox relay:
//
//  1. Claim   — mark batch PUBLISHING, release connection.
//  2. Publish — no DB connection held: AMQP publish + confirm per row.
//  3. Cleanup — delete confirmed rows, release connection.
//
// If the relay crashes between phases 2 and 3, rows remain PUBLISHING until
// ResetStuck resets them to PENDING (called on startup or by the cleanup worker).
func (r *Relay) tick(ctx context.Context) error {
	// Phase 1 — claim
	msgs, err := outbox.ClaimBatch(ctx, r.pool, r.workerID, batchSize)
	if err != nil {
		return fmt.Errorf("claim batch: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}

	// Phase 2 — publish (no DB connection held)
	batch := make([]broker.BatchMsg, len(msgs))
	for i, m := range msgs {
		batch[i] = broker.BatchMsg{ID: m.ID, RoutingKey: m.RoutingKey, Body: m.Payload}
	}
	pubCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	confirmed, err := r.pub.PublishBatch(pubCtx, batch)
	cancel()
	if err != nil {
		return fmt.Errorf("publish batch: %w", err)
	}
	if len(confirmed) == 0 {
		return nil
	}

	// Phase 3 — delete confirmed rows
	if err := outbox.DeleteClaimed(ctx, r.pool, confirmed, r.workerID); err != nil {
		return fmt.Errorf("delete claimed: %w", err)
	}

	return nil
}

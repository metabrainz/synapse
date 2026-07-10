package relay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/metabrainz/synapse/internal/broker"
	"github.com/metabrainz/synapse/internal/store/outbox"
)

const publishTimeout = 5 * time.Second

func randHex() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type Relay struct {
	pool      *pgxpool.Pool
	pub       broker.Publisher
	pollMs    int
	batchSize int
	workerID  string
}

// New creates a Relay. workerID is a random UUID that scopes outbox ownership:
// DeleteClaimed will only delete rows that this worker claimed, preventing a
// restarted worker from deleting rows owned by a still-running sibling.
func New(pool *pgxpool.Pool, pub broker.Publisher, pollMs, batchSize int) *Relay {
	return &Relay{
		pool:      pool,
		pub:       pub,
		pollMs:    pollMs,
		batchSize: batchSize,
		workerID:  randHex(),
	}
}

// Run polls the outbox on every tick until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.Tick(time.Duration(r.pollMs) * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker:
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
// ResetStuck resets them to PENDING (called once at startup in workers.go).
func (r *Relay) tick(ctx context.Context) error {
	msgs, err := outbox.ClaimBatch(ctx, r.pool, r.workerID, r.batchSize)
	if err != nil {
		return fmt.Errorf("claim batch: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}

	batch := make([]broker.BatchMsg, len(msgs))
	for i, msg := range msgs {
		batch[i] = broker.BatchMsg{ID: msg.ID, RoutingKey: msg.RoutingKey, Body: msg.Payload}
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

	if err := outbox.DeleteClaimed(ctx, r.pool, confirmed, r.workerID); err != nil {
		return fmt.Errorf("delete claimed: %w", err)
	}

	return nil
}

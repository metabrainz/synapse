package live

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	publishTimeout = 500 * time.Millisecond
	chunkSize      = 1000
)

type Publisher struct {
	rdb *redis.Client
}

func NewPublisher(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb}
}

type Target struct {
	UserID string
	Nudge  Nudge
}

// Publish is best-effort: failures are logged, never returned. The context is
// detached so a client disconnecting right after the commit doesn't drop the nudge.
func (p *Publisher) Publish(ctx context.Context, userID string, n Nudge) {
	if p == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()

	payload, _ := json.Marshal(n)
	if err := p.rdb.Publish(ctx, Channel(userID), payload).Err(); err != nil {
		slog.Warn("live: publish failed", "user", userID, "err", err)
	}
}

func (p *Publisher) PublishMany(ctx context.Context, ts []Target) {
	if p == nil {
		return
	}
	for start := 0; start < len(ts); start += chunkSize {
		chunk := ts[start:min(start+chunkSize, len(ts))]
		p.publishChunk(ctx, chunk)
	}
}

func (p *Publisher) publishChunk(ctx context.Context, chunk []Target) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()

	pipe := p.rdb.Pipeline()
	for _, t := range chunk {
		payload, _ := json.Marshal(t.Nudge)
		pipe.Publish(ctx, Channel(t.UserID), payload)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("live: publish chunk failed", "count", len(chunk), "err", err)
	}
}

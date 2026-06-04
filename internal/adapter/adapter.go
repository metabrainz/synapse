package adapter

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/redis/go-redis/v9"
)

// Adapter delivers a single WorkerMessage to its target channel.
type Adapter interface {
	Deliver(ctx context.Context, msg fanout.WorkerMessage) error
	MaxAttempts() int
}

// Starter is implemented by adapters that need async initialization at startup —
// validating credentials, registering a webhook endpoint, etc.
// Build calls Start on every registered adapter that satisfies this interface.
type Starter interface {
	Start(ctx context.Context) error
}

// RouteProvider is implemented by adapters that expose their own HTTP routes —
// a user-facing connect flow, an inbound webhook, or both.
// NewRouter calls MountRoutes once at startup for every registered adapter
// that satisfies this interface.
type RouteProvider interface {
	MountRoutes(r chi.Router, userMW func(http.Handler) http.Handler, rdb *redis.Client, channels *userchannels.Repo)
}

// RateLimiter is implemented by adapters that enforce destination-level rate limits
// (e.g. Telegram's 1 msg/user/s and 30 msg/s global caps).
//
// The worker checks this BEFORE the dedup layer so that rate-limited messages can
// be re-queued with the same attempt counter — spending a dedup slot would cause
// the message to be silently dropped when it comes back off the retry queue.
type RateLimiter interface {
	// RateLimit reports whether the message may be sent now.
	// Returns (true, 0) if allowed; (false, retryAfter) if the caller should
	// back off. A zero retryAfter means "use a short default (1 s)".
	// Implementations must fail-open on Redis errors.
	RateLimit(ctx context.Context, msg fanout.WorkerMessage) (allowed bool, retryAfter time.Duration)
}

// RetryAfter extracts a suggested retry delay from an error, if the error
// carries one (e.g. a 429 response with a Retry-After header).
// Returns (duration, true) when present and positive.
func RetryAfter(err error) (time.Duration, bool) {
	var e interface{ RetryAfter() time.Duration }
	if errors.As(err, &e) {
		if d := e.RetryAfter(); d > 0 {
			return d, true
		}
	}
	return 0, false
}

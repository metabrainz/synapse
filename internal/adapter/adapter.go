package adapter

import (
	"context"
	"net/http"

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

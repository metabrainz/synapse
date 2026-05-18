package adapter

import (
	"context"

	"github.com/metabrainz/synapse/internal/fanout"
)

// Adapter delivers a single WorkerMessage to its target channel.
type Adapter interface {
	Deliver(ctx context.Context, msg fanout.WorkerMessage) error
	MaxAttempts() int
}

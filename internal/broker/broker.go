package broker

import "context"

// BatchMsg is one unit in a PublishBatch call.
type BatchMsg struct {
	ID         int64
	RoutingKey string
	Body       []byte
}

// Publisher publishes messages to named routing keys.
// Implementations must be safe for concurrent use.
type Publisher interface {
	// PublishBatch fires all messages without waiting for confirms, then drains
	// confirms in a single pass. Returns the IDs that the broker acked; nacked
	// or unconfirmed IDs are omitted and will be retried by ResetStuck.
	PublishBatch(ctx context.Context, msgs []BatchMsg) (confirmedIDs []int64, err error)
	Close() error
}

package broker

import "context"

// Publisher publishes a message to a named routing key.
// Implementations must be safe for concurrent use.
type Publisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
	Close() error
}

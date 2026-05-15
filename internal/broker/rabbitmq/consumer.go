package rabbitmq

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler processes one message. Return nil to ack, non-nil to reject (→ DLQ).
// If the error is retryable, the handler is responsible for publishing to the
// retry exchange (via Publisher.PublishRetry) before returning nil so the
// original message is acked.
type Handler func(ctx context.Context, body []byte) error

// Consumer holds one AMQP connection dedicated to consuming from one channel-type queue.
type Consumer struct {
	url         string
	channelType string
	prefetch    int

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewConsumer(url, channelType string, prefetch int) (*Consumer, error) {
	c := &Consumer{url: url, channelType: channelType, prefetch: prefetch}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Consumer) connect() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("amqp channel: %w", err)
	}
	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		conn.Close()
		return fmt.Errorf("set qos: %w", err)
	}
	c.conn = conn
	c.ch = ch
	return nil
}

// Run consumes messages until ctx is cancelled or the connection drops.
// Prefetch limits how many unacked messages are in-flight at once — no
// application-level semaphore needed.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	queue := "deliveries." + c.channelType
	msgs, err := c.ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("consumer channel closed")
			}
			if err := handler(ctx, msg.Body); err != nil {
				msg.Reject(false) // false = don't requeue; DLX routes to dead queue
			} else {
				msg.Ack(false)
			}
		}
	}
}

// PublishRetry sends a message to the retry exchange with a per-message TTL (ms).
// The retry queue's DLX routes it back to the main queue once the TTL expires.
func (c *Consumer) PublishRetry(ctx context.Context, channelType string, body []byte, ttlMs int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ch.PublishWithContext(ctx, exchangeRetry, channelType, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Expiration:   fmt.Sprintf("%d", ttlMs),
	})
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ConsumeQueue is a simple blocking consume loop for an arbitrary named queue.
// Returns when ctx is cancelled or the AMQP channel closes.
func ConsumeQueue(ctx context.Context, url, queue string, prefetch int, handler Handler) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("amqp channel: %w", err)
	}
	defer ch.Close()

	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("amqp channel closed")
			}
			if err := handler(ctx, msg.Body); err != nil {
				msg.Reject(false)
			} else {
				msg.Ack(false)
			}
		}
	}
}

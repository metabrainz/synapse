package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

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

	mu          sync.Mutex
	conn        *amqp.Connection
	amqpChannel *amqp.Channel
}

func NewConsumer(url, channelType string, prefetch int) (*Consumer, error) {
	consumer := &Consumer{url: url, channelType: channelType, prefetch: prefetch}
	if err := consumer.connect(); err != nil {
		return nil, err
	}
	return consumer, nil
}

func (c *Consumer) connect() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	amqpChannel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("amqp channel: %w", err)
	}
	if err := amqpChannel.Qos(c.prefetch, 0, false); err != nil {
		conn.Close()
		return fmt.Errorf("set qos: %w", err)
	}
	c.conn = conn
	c.amqpChannel = amqpChannel
	return nil
}

// Run consumes messages until ctx is cancelled or the connection drops.
// Prefetch limits how many unacked messages are in-flight at once — no
// application-level semaphore needed.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	queue := "deliveries." + c.channelType
	msgs, err := c.amqpChannel.Consume(queue, "", false, false, false, false, nil)
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
	return c.amqpChannel.PublishWithContext(ctx, exchangeRetry, channelType, false, false, amqp.Publishing{
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

// BatchHandler processes a batch of raw message bodies. Return nil to ack all
// messages in the batch; return an error to nack all (RabbitMQ will redeliver).
type BatchHandler func(ctx context.Context, bodies [][]byte) error

// ConsumeBatchQueue collects up to batchSize messages per iteration and
// processes them together via handler. It blocks waiting for the first message,
// then drains additional messages for up to drainMs milliseconds before
// processing — keeping latency low on a quiet queue while maximising batch
// size under load. Returns when ctx is cancelled or the connection drops.
func ConsumeBatchQueue(ctx context.Context, url, queue string, batchSize, drainMs int, handler BatchHandler) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("amqp channel: %w", err)
	}
	defer amqpChannel.Close()

	if err := amqpChannel.Qos(batchSize, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	messages, err := amqpChannel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}

	drainDuration := time.Duration(drainMs) * time.Millisecond

	for {
		// Block until the first message arrives or ctx is cancelled.
		var first amqp.Delivery
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-messages:
			if !ok {
				return fmt.Errorf("amqp channel closed")
			}
			first = message
		}

		batch := []amqp.Delivery{first}
		timer := time.NewTimer(drainDuration)

		// Collect messages until the batch is full or the drain duration expires or the context is cancelled.
	collect:
		for len(batch) < batchSize {
			select {
			case message, ok := <-messages:
				if !ok {
					timer.Stop()
					break collect
				}
				batch = append(batch, message)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				timer.Stop()
				for _, delivery := range batch {
					delivery.Nack(false, true)
				}
				return ctx.Err()
			}
		}
		timer.Stop()

		batchBodies := make([][]byte, len(batch))
		for i, message := range batch {
			batchBodies[i] = message.Body
		}

		if err := handler(ctx, batchBodies); err != nil {
			for _, delivery := range batch {
				delivery.Nack(false, true)
			}
		} else {
			for _, delivery := range batch {
				delivery.Ack(false)
			}
		}
	}
}

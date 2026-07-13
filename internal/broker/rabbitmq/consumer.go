package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler processes one message. Three outcomes:
//
//   - Success:          return nil          → Ack; message is done.
//   - Retriable failure: PublishRetry, then return nil → Ack original; retry copy sits in TTL queue.
//   - Exhausted / fatal: return error       → Reject(requeue=false) → DLX → DLQ.
//
// The handler owns the retry-publish step. Returning nil without publishing
// on a failure means the message is silently dropped.
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

// Run consumes messages until ctx is cancelled. Reconnects automatically on
// channel/connection close with exponential backoff capped at 30s.
func (c *Consumer) Run(ctx context.Context, handler Handler) error {
	queue := "deliveries." + c.channelType
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		msgs, err := c.amqpChannel.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			slog.Error("consumer: consume failed, reconnecting", "queue", queue, "err", err, "backoff", backoff)
			if !sleep(ctx, backoff) {
				return ctx.Err()
			}
			backoff = min(backoff*2, 30*time.Second)
			if rerr := c.reconnect(); rerr != nil {
				slog.Error("consumer: reconnect failed", "queue", queue, "err", rerr)
				continue
			}
			continue
		}
		backoff = time.Second

		if err := c.consumeLoop(ctx, queue, msgs, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Warn("consumer: connection lost, reconnecting", "queue", queue, "err", err, "backoff", backoff)
			if !sleep(ctx, backoff) {
				return ctx.Err()
			}
			backoff = min(backoff*2, 30*time.Second)
			if rerr := c.reconnect(); rerr != nil {
				slog.Error("consumer: reconnect failed", "queue", queue, "err", rerr)
			}
		}
	}
}

func (c *Consumer) consumeLoop(ctx context.Context, queue string, msgs <-chan amqp.Delivery, handler Handler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("consumer channel closed")
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("worker: handler panic", "queue", queue, "panic", r)
						msg.Reject(false)
					}
				}()
				if err := handler(ctx, msg.Body); err != nil {
					slog.Error("worker: handler failed, rejecting to DLQ", "queue", queue, "err", err)
					msg.Reject(false)
				} else {
					msg.Ack(false)
				}
			}()
		}
	}
}

func (c *Consumer) reconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
	return c.connect()
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
// processes them together via handler. Reconnects automatically on connection
// loss with exponential backoff capped at 30s. Returns only when ctx is cancelled.
func ConsumeBatchQueue(ctx context.Context, url, queue string, batchSize, drainMs int, handler BatchHandler) error {
	backoff := time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := consumeBatchOnce(ctx, url, queue, batchSize, drainMs, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("batch consumer: connection lost, reconnecting", "queue", queue, "err", err, "backoff", backoff)
		if !sleep(ctx, backoff) {
			return ctx.Err()
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func consumeBatchOnce(ctx context.Context, url, queue string, batchSize, drainMs int, handler BatchHandler) error {
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

// sleep returns false if ctx was cancelled before duration elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

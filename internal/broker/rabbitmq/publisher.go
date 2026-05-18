package rabbitmq

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/metabrainz/synapse/internal/broker"
)

// confirmBufSize must exceed the largest batch size to prevent the broker's
// internal goroutine from blocking while we're still in Phase A publishing.
const confirmBufSize = 512

// Publisher holds one AMQP connection and channel in confirm mode.
// PublishBatch gives at-least-once delivery: the relay will not delete an
// outbox row until the broker has confirmed it received the message.
type Publisher struct {
	url      string
	mu       sync.Mutex
	conn     *amqp.Connection
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
}

func New(url string) (*Publisher, error) {
	p := &Publisher{url: url}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) connect() error {
	// Release the previous connection (already broken, but free OS resources).
	if p.conn != nil {
		p.conn.Close()
	}

	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("amqp channel: %w", err)
	}
	// Enable publisher confirms. The broker will now ack every published
	// message. Without this, publishing is fire-and-forget at the protocol level.
	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.conn = conn
	p.ch = ch
	p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, confirmBufSize))
	return nil
}

// PublishBatch publishes all messages first, then drains publisher confirms
// in a second phase to avoid a confirm RTT per message.
//
// Semantics:
//   - Only ACKed message IDs are returned.
//   - NACKed or unknown messages remain retryable in the outbox.
//   - Caller must delete ONLY the returned IDs.
//   - Caller must recover stuck PUBLISHING rows separately.
//
// Important:
//   - NotifyPublish emits one Confirmation per published message.
//   - Delivery tags are monotonically increasing per channel.
func (p *Publisher) PublishBatch(ctx context.Context, msgs []broker.BatchMsg) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	startTag := p.ch.GetNextPublishSeqNo()

	// Phase A — publish everything without waiting for confirms.
	for _, m := range msgs {
		pub := amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         m.Body,
		}
		if err := p.ch.PublishWithContext(ctx, exchange, m.RoutingKey, false, false, pub); err != nil {
			// Publish failure does NOT guarantee broker absence.
			// Message may already have been received before connection loss.
			//
			// We reconnect and return error so caller retries only
			// non-confirmed rows later.
			if rerr := p.connect(); rerr != nil {
				return nil, fmt.Errorf("publish failed and reconnect failed: %w", rerr)
			}
			return nil, fmt.Errorf("publish failed, batch aborted: %w", err)
		}
	}

	// Phase B — drain confirms.
	n := len(msgs)
	received := make([]bool, n)
	acked := make([]bool, n)
	receivedCount := 0

	for receivedCount < n {
		select {
		case c, ok := <-p.confirms:
			if !ok {
				return nil, fmt.Errorf("confirm channel closed")
			}
			idx := int(c.DeliveryTag - startTag)
			if idx < 0 || idx >= n {
				continue
			}
			if received[idx] {
				continue
			}
			received[idx] = true
			acked[idx] = c.Ack
			receivedCount++
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	confirmedIDs := make([]int64, 0, n)
	for i, m := range msgs {
		if acked[i] {
			confirmedIDs = append(confirmedIDs, m.ID)
		}
	}
	return confirmedIDs, nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

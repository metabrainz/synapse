package rabbitmq

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchange      = "deliveries"
	exchangeRetry = "deliveries.retry"
	exchangeDead  = "deliveries.dead"
)

// Publisher holds one AMQP connection and channel in confirm mode.
// Every Publish call blocks until the broker sends Basic.Ack (or returns an
// error), giving at-least-once delivery: the relay will not delete an outbox
// row until the broker has confirmed it received the message.
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
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		conn.Close()
		return fmt.Errorf("declare exchange: %w", err)
	}
	// Enable publisher confirms. The broker will now ack every published
	// message. Without this, Publish is fire-and-forget at the protocol level.
	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.conn = conn
	p.ch = ch
	// Buffer of 1: we hold mu for the full publish+confirm cycle so only one
	// message is ever in-flight at a time.
	p.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	return nil
}

// Publish sends one message and waits for the broker to confirm it.
// Returns an error if the broker nacks, the connection drops, or ctx is
// cancelled — in all cases the caller (relay) must not delete the outbox row.
func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}

	if err := p.ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg); err != nil {
		// Channel is broken — reconnect and retry once.
		// connect() replaces p.confirms with a fresh channel, so the
		// confirm we wait for below belongs to this second publish, not
		// the failed one.
		if rerr := p.connect(); rerr != nil {
			return fmt.Errorf("publish failed and reconnect failed: %w", rerr)
		}
		if err = p.ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg); err != nil {
			return fmt.Errorf("publish after reconnect: %w", err)
		}
	}

	// Wait for broker acknowledgement.
	select {
	case confirm, ok := <-p.confirms:
		if !ok {
			return fmt.Errorf("confirm channel closed (connection dropped)")
		}
		if !confirm.Ack {
			return fmt.Errorf("broker nacked message (delivery tag %d)", confirm.DeliveryTag)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

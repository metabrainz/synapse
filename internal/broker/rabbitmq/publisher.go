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

// Publisher holds one AMQP connection and channel.
// On publish failure it attempts one reconnect before returning the error.
type Publisher struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func New(url string) (*Publisher, error) {
	p := &Publisher{url: url}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) connect() error {
	// Close the previous connection before replacing it. Ignore the error —
	// it's already broken, but we still need to release OS-level resources.
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
	p.conn = conn
	p.ch = ch
	return nil
}

func (p *Publisher) Publish(_ context.Context, routingKey string, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}
	err := p.ch.Publish(exchange, routingKey, false, false, msg)
	if err == nil {
		return nil
	}

	// One reconnect attempt on failure.
	if rerr := p.connect(); rerr != nil {
		return fmt.Errorf("publish failed and reconnect failed: %w", rerr)
	}
	if err = p.ch.Publish(exchange, routingKey, false, false, msg); err != nil {
		return fmt.Errorf("publish after reconnect: %w", err)
	}
	return nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// V1 channel types. Adding a new type means adding it here and writing an adapter.
var ChannelTypes = []string{"webhook", "email"}

// Setup dials AMQP, declares full topology, then closes the connection.
// Call once at service startup before starting the relay or worker.
func Setup(url string) error {
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

	return DeclareTopology(ch, ChannelTypes)
}

// DeclareTopology declares all exchanges and per-channel-type queue sets.
// Idempotent — safe to call on every startup.
//
// Exchange layout:
//
//	deliveries       (topic) — main dispatch
//	deliveries.retry (topic) — TTL-based backoff holding area
//	deliveries.dead  (topic) — permanent failures
//
// Per channel type (e.g. "webhook", "email"):
//
//	deliveries.{type}        — main queue, DLX → deliveries.dead
//	deliveries.{type}.retry  — retry queue, DLX → deliveries (routes back on TTL expiry)
//	deliveries.dead.{type}   — dead-letter queue (manual inspection)
func DeclareTopology(ch *amqp.Channel, channelTypes []string) error {
	for _, ex := range []string{exchange, exchangeRetry, exchangeDead} {
		if err := ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", ex, err)
		}
	}
	for _, ct := range channelTypes {
		if err := declareQueueSet(ch, ct); err != nil {
			return fmt.Errorf("declare queue set %s: %w", ct, err)
		}
	}
	return nil
}

func declareQueueSet(ch *amqp.Channel, channelType string) error {
	main := "deliveries." + channelType
	retry := "deliveries." + channelType + ".retry"
	dead := "deliveries.dead." + channelType

	// Main queue: on rejection → dead exchange.
	if _, err := ch.QueueDeclare(main, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    exchangeDead,
		"x-dead-letter-routing-key": channelType,
	}); err != nil {
		return fmt.Errorf("declare %s: %w", main, err)
	}
	if err := ch.QueueBind(main, channelType, exchange, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", main, err)
	}

	// Retry queue: per-message TTL set at publish time; on TTL expiry → back to main.
	if _, err := ch.QueueDeclare(retry, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    exchange,
		"x-dead-letter-routing-key": channelType,
	}); err != nil {
		return fmt.Errorf("declare %s: %w", retry, err)
	}
	if err := ch.QueueBind(retry, channelType, exchangeRetry, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", retry, err)
	}

	// Dead-letter queue: terminal failures, for manual inspection / alerting.
	if _, err := ch.QueueDeclare(dead, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", dead, err)
	}
	if err := ch.QueueBind(dead, channelType, exchangeDead, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", dead, err)
	}

	return nil
}

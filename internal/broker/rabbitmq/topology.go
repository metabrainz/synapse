package rabbitmq

import (
	"fmt"

	"github.com/metabrainz/synapse/internal/adapter"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// Delivery exchange topology — used by publisher, consumer, and topology declaration.
	exchange      = "deliveries"
	exchangeRetry = "deliveries.retry"
	exchangeDead  = "deliveries.dead"

	ExchangeIngest   = "events.ingest"
	QueueIngest      = "events.ingest"
	RoutingKeyIngest = "event"
)

// Setup dials AMQP, declares full topology, then closes the connection.
// channelTypes must be adapter.ChannelTypes() — the topology and the adapter
// registry must match so every declared queue has a consumer and vice versa.
// Call once at service startup before starting the relay or worker.
func Setup(url string, channelTypes []adapter.ChannelType) error {
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

	return DeclareTopology(ch, channelTypes)
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
func DeclareTopology(ch *amqp.Channel, channelTypes []adapter.ChannelType) error {
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
	if err := declareIngestTopology(ch); err != nil {
		return fmt.Errorf("declare ingest topology: %w", err)
	}
	return nil
}

func declareIngestTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeIngest, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeIngest, err)
	}
	if _, err := ch.QueueDeclare(QueueIngest, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", QueueIngest, err)
	}
	if err := ch.QueueBind(QueueIngest, RoutingKeyIngest, ExchangeIngest, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", QueueIngest, err)
	}
	return nil
}

func declareQueueSet(ch *amqp.Channel, channelType adapter.ChannelType) error {
	channelTypeStr := string(channelType)

	main := "deliveries." + channelTypeStr
	retry := "deliveries." + channelTypeStr + ".retry"
	dead := "deliveries.dead." + channelTypeStr

	// Main queue: on rejection → dead exchange.
	if _, err := ch.QueueDeclare(main, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    exchangeDead,
		"x-dead-letter-routing-key": channelTypeStr,
	}); err != nil {
		return fmt.Errorf("declare %s: %w", main, err)
	}
	if err := ch.QueueBind(main, channelTypeStr, exchange, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", main, err)
	}

	// Retry queue: per-message TTL set at publish time; on TTL expiry → back to main.
	if _, err := ch.QueueDeclare(retry, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    exchange,
		"x-dead-letter-routing-key": channelTypeStr,
	}); err != nil {
		return fmt.Errorf("declare %s: %w", retry, err)
	}
	if err := ch.QueueBind(retry, channelTypeStr, exchangeRetry, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", retry, err)
	}

	// Dead-letter queue: terminal failures, for manual inspection / alerting.
	if _, err := ch.QueueDeclare(dead, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s: %w", dead, err)
	}
	if err := ch.QueueBind(dead, channelTypeStr, exchangeDead, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", dead, err)
	}

	return nil
}

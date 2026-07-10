// Package config defines the Synapse configuration schema and loads it from a
// YAML file with environment variable overrides. All services share the same
// Config struct; each service only reads the fields it needs.
package config

import "fmt"

type Config struct {
	Postgres      PostgresConfig          `yaml:"postgres"`
	Redis         RedisConfig             `yaml:"redis"`
	RabbitMQ      RabbitMQConfig          `yaml:"rabbitmq"`
	HTTP          HTTPConfig              `yaml:"http"`
	Worker        WorkerConfig            `yaml:"worker"`
	Relay         RelayConfig             `yaml:"relay"`
	Ingest        IngestConfig            `yaml:"ingest"`
	RateLimit     RateLimitConfig         `yaml:"rate_limit"`
	OAuth         OAuthConfig             `yaml:"oauth"`
	Webhook       WebhookConfig           `yaml:"webhook"`
	Telegram      TelegramConfig          `yaml:"telegram"`
	Observability ObservabilityConfig     `yaml:"observability"`
	Tenants       map[string]TenantConfig `yaml:"tenants"`
}

type ObservabilityConfig struct {
	// SentryDSN is the Sentry project DSN. Disabled when empty.
	SentryDSN        string  `yaml:"sentry_dsn"`
	Environment      string  `yaml:"environment"`
	Release          string  `yaml:"release"`
	TracesSampleRate float64 `yaml:"traces_sample_rate"`
}

// TenantConfig holds per-tenant secrets loaded from the environment or config file.
type TenantConfig struct {
	APIKey string `yaml:"api_key"`
}

type OAuthConfig struct {
	ClientID         string `yaml:"client_id"`
	ClientSecret     string `yaml:"client_secret"`
	IntrospectionURL string `yaml:"introspection_url"`
}

type WebhookConfig struct {
	// AllowPrivateURLs disables SSRF protection for webhook delivery.
	// Never set this in production — for local development and testing only.
	AllowPrivateURLs bool `yaml:"allow_private_urls"`
}

type TelegramConfig struct {
	BotToken      string `yaml:"bot_token"`
	WebhookURL    string `yaml:"webhook_url"`
	WebhookSecret string `yaml:"webhook_secret"`
}

type PostgresConfig struct {
	Host      string `yaml:"host"`
	Port      int    `yaml:"port"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	DBName    string `yaml:"dbname"`
	SSLMode   string `yaml:"sslmode"`
	PgBouncer bool   `yaml:"pgbouncer"`
	// DirectDSN is a direct Postgres connection string (bypassing PgBouncer) used
	// exclusively for LISTEN/NOTIFY. PgBouncer transaction mode drops session-level
	// commands like LISTEN, so the subscription cache needs a persistent raw connection.
	// Leave empty when not using PgBouncer — NewCache derives it from the pool config.
	DirectDSN string `yaml:"direct_dsn"`
}

// DSN returns the key=value format used by pgx pool.
func (c PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
}

// URL returns the postgres:// URL format required by golang-migrate.
func (c PostgresConfig) URL() string {
	return fmt.Sprintf("pgx5://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.DBName, c.SSLMode)
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type RabbitMQConfig struct {
	URL string `yaml:"url"`
}

type HTTPConfig struct {
	Port int `yaml:"port"`
	// DBConns is the maximum number of Postgres connections the API pool may hold.
	// Each concurrent HTTP request can use one connection, so size this to the
	// expected concurrent request peak, not the total request rate.
	DBConns int32 `yaml:"db_conns"`
}

type WorkerConfig struct {
	// DBPool is the maximum number of Postgres connections shared across all
	// worker goroutines for writing delivery status updates.
	DBPool   int                            `yaml:"db_pool"`
	Channels map[string]ChannelWorkerConfig `yaml:"channels"`
}

// ChannelWorkerConfig holds per-channel-type tuning knobs.
// Adding a new channel type requires only a new entry in the map — no struct changes.
type ChannelWorkerConfig struct {
	// Concurrency is the number of goroutines consuming from this channel's queue.
	Concurrency int `yaml:"concurrency"`
	// Prefetch is the RabbitMQ QoS prefetch count per goroutine — how many
	// unacked messages RabbitMQ will deliver before waiting for acks.
	Prefetch int `yaml:"prefetch"`
}

type RelayConfig struct {
	// OutboxPollMs is how often (in milliseconds) each relay worker checks for
	// new PENDING outbox rows. Lower values reduce latency; higher values reduce DB load.
	OutboxPollMs int `yaml:"outbox_poll_ms"`
	Workers      int `yaml:"workers"`
	// BatchSize is the maximum number of outbox rows claimed per relay tick.
	// Larger batches amortise AMQP confirm overhead but increase per-batch latency.
	BatchSize int `yaml:"batch_size"`
}

type IngestConfig struct {
	Workers int `yaml:"workers"`
	// BatchSize caps how many AMQP messages are processed in one DB transaction.
	// Larger batches cut per-event DB overhead but increase memory use and the
	// blast radius if a transaction fails (all messages are nacked and redelivered).
	BatchSize int `yaml:"batch_size"`
	// DrainMs is how long each worker waits to accumulate a full batch after the
	// first message arrives. Higher values improve batch fill under load but add
	// latency for isolated messages on a quiet queue.
	DrainMs int `yaml:"drain_ms"`
}

type RateLimitConfig struct {
	// Burst is the maximum number of tokens in the bucket — controls peak burst capacity.
	Burst int `yaml:"burst"`
	// RatePerSec is the token refill rate — controls sustained throughput.
	RatePerSec int `yaml:"rate_per_sec"`
}

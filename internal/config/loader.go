package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load reads a YAML config file and overlays environment variable overrides.
// If the file does not exist, env vars and defaults are used without error.
func Load(path string) (*Config, error) {
	cfg := defaultConfig()

	f, err := os.Open(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("open config: %w", err)
	}
	if err == nil {
		defer f.Close()
		if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
	}

	if err := applyEnv(&cfg); err != nil {
		return nil, fmt.Errorf("invalid environment: %w", err)
	}
	return &cfg, nil
}

func defaultConfig() Config {
	return Config{
		Observability: ObservabilityConfig{
			TracesSampleRate: 0.1,
		},
		HTTP: HTTPConfig{
			Port:    8080,
			DBConns: 20,
		},
		Postgres: PostgresConfig{
			Host:    "localhost",
			Port:    5432,
			SSLMode: "disable",
		},
		Redis: RedisConfig{
			Addr: "localhost:6379",
		},
		RabbitMQ: RabbitMQConfig{
			URL: "amqp://guest:guest@localhost:5672/",
		},
		Relay: RelayConfig{
			OutboxPollMs: 100,
			Workers:      4,
			BatchSize:    100,
		},
		Worker: WorkerConfig{
			DBPool: 15,
			Channels: map[string]ChannelWorkerConfig{
				"webhook": {Concurrency: 10, Prefetch: 10},
			},
		},
		Ingest: IngestConfig{
			Workers:   4,
			BatchSize: 50,
			DrainMs:   10,
		},
		RateLimit: RateLimitConfig{
			Burst:      100,
			RatePerSec: 50,
		},
	}
}

func applyEnv(cfg *Config) error {
	str := func(dst *string, key string) {
		if v := os.Getenv(key); v != "" {
			*dst = v
		}
	}
	integer := func(dst *int, key string) error {
		v := os.Getenv(key)
		if v == "" {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s=%q: expected integer", key, v)
		}
		*dst = n
		return nil
	}
	int32v := func(dst *int32, key string) error {
		v := os.Getenv(key)
		if v == "" {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s=%q: expected integer", key, v)
		}
		*dst = int32(n)
		return nil
	}
	float64v := func(dst *float64, key string) error {
		v := os.Getenv(key)
		if v == "" {
			return nil
		}
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("%s=%q: expected float", key, v)
		}
		*dst = n
		return nil
	}

	str(&cfg.Postgres.Host, "SYNAPSE_PG_HOST")
	str(&cfg.Postgres.User, "SYNAPSE_PG_USER")
	str(&cfg.Postgres.Password, "SYNAPSE_PG_PASSWORD")
	str(&cfg.Postgres.DBName, "SYNAPSE_PG_DBNAME")
	str(&cfg.Postgres.DirectDSN, "SYNAPSE_PG_DIRECT_DSN")
	str(&cfg.Redis.Addr, "SYNAPSE_REDIS_ADDR")
	str(&cfg.RabbitMQ.URL, "SYNAPSE_RABBITMQ_URL")
	str(&cfg.OAuth.ClientID, "SYNAPSE_OAUTH_CLIENT_ID")
	str(&cfg.OAuth.ClientSecret, "SYNAPSE_OAUTH_CLIENT_SECRET")
	str(&cfg.OAuth.IntrospectionURL, "SYNAPSE_OAUTH_INTROSPECTION_URL")
	str(&cfg.Telegram.BotToken, "SYNAPSE_TELEGRAM_BOT_TOKEN")
	str(&cfg.Telegram.WebhookURL, "SYNAPSE_TELEGRAM_WEBHOOK_URL")
	str(&cfg.Telegram.WebhookSecret, "SYNAPSE_TELEGRAM_WEBHOOK_SECRET")
	str(&cfg.MailService.URL, "SYNAPSE_MAIL_SERVICE_URL")
	str(&cfg.Observability.SentryDSN, "SYNAPSE_OBSERVABILITY_SENTRY_DSN")
	str(&cfg.Observability.Environment, "SYNAPSE_OBSERVABILITY_ENVIRONMENT")
	str(&cfg.Observability.Release, "SYNAPSE_OBSERVABILITY_RELEASE")

	if v := os.Getenv("SYNAPSE_PG_PGBOUNCER"); v == "true" || v == "1" {
		cfg.Postgres.PgBouncer = true
	}

	// Tenant API keys: SYNAPSE_TENANTS_{TENANT_ID}_API_KEY
	// e.g. SYNAPSE_TENANTS_LISTENBRAINZ_API_KEY
	if cfg.Tenants == nil {
		cfg.Tenants = make(map[string]TenantConfig)
	}
	for _, env := range os.Environ() {
		const prefix = "SYNAPSE_TENANTS_"
		const suffix = "_API_KEY"
		if !strings.HasPrefix(env, prefix) {
			continue
		}
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := kv[0]
		if !strings.HasSuffix(key, suffix) {
			continue
		}
		tenantID := strings.ToLower(key[len(prefix) : len(key)-len(suffix)])
		tc := cfg.Tenants[tenantID]
		tc.APIKey = kv[1]
		cfg.Tenants[tenantID] = tc
	}

	if err := errors.Join(
		integer(&cfg.HTTP.Port, "SYNAPSE_HTTP_PORT"),
		integer(&cfg.Postgres.Port, "SYNAPSE_PG_PORT"),
		int32v(&cfg.HTTP.DBConns, "SYNAPSE_HTTP_DB_CONNS"),
		integer(&cfg.Relay.Workers, "SYNAPSE_RELAY_WORKERS"),
		integer(&cfg.Relay.OutboxPollMs, "SYNAPSE_RELAY_POLL_MS"),
		integer(&cfg.Relay.BatchSize, "SYNAPSE_RELAY_BATCH_SIZE"),
		integer(&cfg.Worker.DBPool, "SYNAPSE_WORKER_DB_POOL"),
		integer(&cfg.Ingest.Workers, "SYNAPSE_INGEST_WORKERS"),
		integer(&cfg.Ingest.BatchSize, "SYNAPSE_INGEST_BATCH_SIZE"),
		integer(&cfg.Ingest.DrainMs, "SYNAPSE_INGEST_DRAIN_MS"),
		integer(&cfg.RateLimit.Burst, "SYNAPSE_RATELIMIT_BURST"),
		integer(&cfg.RateLimit.RatePerSec, "SYNAPSE_RATELIMIT_RATE_PER_SEC"),
		float64v(&cfg.Observability.TracesSampleRate, "SYNAPSE_OBSERVABILITY_TRACES_SAMPLE_RATE"),
	); err != nil {
		return err
	}

	for key, ch := range cfg.Worker.Channels {
		envKey := strings.ToUpper(key)
		if err := errors.Join(
			integer(&ch.Concurrency, "SYNAPSE_WORKER_"+envKey+"_CONCURRENCY"),
			integer(&ch.Prefetch, "SYNAPSE_WORKER_"+envKey+"_PREFETCH"),
		); err != nil {
			return err
		}
		cfg.Worker.Channels[key] = ch
	}

	return nil
}

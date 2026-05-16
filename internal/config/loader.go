package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"

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
		},
		Worker: WorkerConfig{
			WebhookConcurrency: 10,
			EmailConcurrency:   5,
			DBPool:             15,
		},
		Ingest: IngestConfig{
			Workers:   4,
			BatchSize: 50,
			DrainMs:   10,
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

	str(&cfg.HTTP.AdminKey, "SYNAPSE_ADMIN_KEY")
	str(&cfg.Postgres.Host, "SYNAPSE_PG_HOST")
	str(&cfg.Postgres.User, "SYNAPSE_PG_USER")
	str(&cfg.Postgres.Password, "SYNAPSE_PG_PASSWORD")
	str(&cfg.Postgres.DBName, "SYNAPSE_PG_DBNAME")
	str(&cfg.Postgres.DirectDSN, "SYNAPSE_PG_DIRECT_DSN")
	str(&cfg.Redis.Addr, "SYNAPSE_REDIS_ADDR")
	str(&cfg.RabbitMQ.URL, "SYNAPSE_RABBITMQ_URL")

	if v := os.Getenv("SYNAPSE_PG_PGBOUNCER"); v == "true" || v == "1" {
		cfg.Postgres.PgBouncer = true
	}

	return errors.Join(
		integer(&cfg.HTTP.Port, "SYNAPSE_HTTP_PORT"),
		integer(&cfg.Postgres.Port, "SYNAPSE_PG_PORT"),
		int32v(&cfg.HTTP.DBConns, "SYNAPSE_HTTP_DB_CONNS"),
		integer(&cfg.Relay.Workers, "SYNAPSE_RELAY_WORKERS"),
		integer(&cfg.Relay.OutboxPollMs, "SYNAPSE_RELAY_POLL_MS"),
		integer(&cfg.Worker.WebhookConcurrency, "SYNAPSE_WORKER_WEBHOOK_CONCURRENCY"),
		integer(&cfg.Worker.DBPool, "SYNAPSE_WORKER_DB_POOL"),
		integer(&cfg.Ingest.Workers, "SYNAPSE_INGEST_WORKERS"),
		integer(&cfg.Ingest.BatchSize, "SYNAPSE_INGEST_BATCH_SIZE"),
		integer(&cfg.Ingest.DrainMs, "SYNAPSE_INGEST_DRAIN_MS"),
	)
}

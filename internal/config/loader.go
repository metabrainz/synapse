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
	var cfg Config

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

	applyEnv(&cfg)
	setDefaults(&cfg)
	return &cfg, nil
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("SYNAPSE_HTTP_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.HTTP.Port = n
		}
	}
	if v := os.Getenv("SYNAPSE_ADMIN_KEY"); v != "" {
		cfg.HTTP.AdminKey = v
	}
	if v := os.Getenv("SYNAPSE_PG_HOST"); v != "" {
		cfg.Postgres.Host = v
	}
	if v := os.Getenv("SYNAPSE_PG_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.Port = n
		}
	}
	if v := os.Getenv("SYNAPSE_PG_USER"); v != "" {
		cfg.Postgres.User = v
	}
	if v := os.Getenv("SYNAPSE_PG_PASSWORD"); v != "" {
		cfg.Postgres.Password = v
	}
	if v := os.Getenv("SYNAPSE_PG_DBNAME"); v != "" {
		cfg.Postgres.DBName = v
	}
	if v := os.Getenv("SYNAPSE_REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
	if v := os.Getenv("SYNAPSE_RABBITMQ_URL"); v != "" {
		cfg.RabbitMQ.URL = v
	}
}

func setDefaults(cfg *Config) {
	if cfg.HTTP.Port == 0 {
		cfg.HTTP.Port = 8080
	}
	if cfg.Postgres.Host == "" {
		cfg.Postgres.Host = "localhost"
	}
	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = 5432
	}
	if cfg.Postgres.SSLMode == "" {
		cfg.Postgres.SSLMode = "disable"
	}
	if cfg.Postgres.MaxConns == 0 {
		cfg.Postgres.MaxConns = 20
	}
	if cfg.Redis.Addr == "" {
		cfg.Redis.Addr = "localhost:6379"
	}
	if cfg.RabbitMQ.URL == "" {
		cfg.RabbitMQ.URL = "amqp://guest:guest@localhost:5672/"
	}
	if cfg.Relay.OutboxPollMs == 0 {
		cfg.Relay.OutboxPollMs = 100
	}
	if cfg.Relay.Workers == 0 {
		cfg.Relay.Workers = 4
	}
	if cfg.Worker.WebhookConcurrency == 0 {
		cfg.Worker.WebhookConcurrency = 10
	}
	if cfg.Worker.EmailConcurrency == 0 {
		cfg.Worker.EmailConcurrency = 5
	}
}

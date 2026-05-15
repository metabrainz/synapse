package config

import "fmt"

type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Redis    RedisConfig    `yaml:"redis"`
	RabbitMQ RabbitMQConfig `yaml:"rabbitmq"`
	HTTP     HTTPConfig     `yaml:"http"`
	Worker   WorkerConfig   `yaml:"worker"`
	Relay    RelayConfig    `yaml:"relay"`
	Ingest   IngestConfig   `yaml:"ingest"`
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
	Port     int    `yaml:"port"`
	AdminKey string `yaml:"admin_key"`
	DBConns  int32  `yaml:"db_conns"`
}

type WorkerConfig struct {
	WebhookConcurrency int `yaml:"webhook_concurrency"`
	EmailConcurrency   int `yaml:"email_concurrency"`
}

type RelayConfig struct {
	OutboxPollMs int `yaml:"outbox_poll_ms"`
	Workers      int `yaml:"workers"`
}

type IngestConfig struct {
	Workers int `yaml:"workers"`
}

package config

import "fmt"

type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Redis    RedisConfig    `yaml:"redis"`
	RabbitMQ RabbitMQConfig `yaml:"rabbitmq"`
	HTTP     HTTPConfig     `yaml:"http"`
	Worker   WorkerConfig   `yaml:"worker"`
	Relay    RelayConfig    `yaml:"relay"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
	MaxConns int32  `yaml:"max_conns"`
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
}

type WorkerConfig struct {
	WebhookConcurrency int `yaml:"webhook_concurrency"`
	EmailConcurrency   int `yaml:"email_concurrency"`
}

type RelayConfig struct {
	OutboxPollMs int `yaml:"outbox_poll_ms"`
	Workers      int `yaml:"workers"`
}

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

func (c PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.DBName, c.SSLMode)
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
}

type WorkerConfig struct {
	WebhookConcurrency int `yaml:"webhook_concurrency"`
	EmailConcurrency   int `yaml:"email_concurrency"`
}

type RelayConfig struct {
	OutboxPollMs int `yaml:"outbox_poll_ms"`
}

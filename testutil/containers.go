//go:build integration

package testutil

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

type CleanupFn func()

func StartPostgres(ctx context.Context) (dsn string, cleanup CleanupFn, err error) {
	c, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("synapse_test"),
		tcpostgres.WithUsername("synapse"),
		tcpostgres.WithPassword("synapse"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start postgres: %w", err)
	}
	dsn, err = c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		c.Terminate(ctx)
		return "", nil, fmt.Errorf("postgres connection string: %w", err)
	}
	return dsn, func() { c.Terminate(ctx) }, nil
}

func StartRedis(ctx context.Context) (addr string, cleanup CleanupFn, err error) {
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		return "", nil, fmt.Errorf("start redis: %w", err)
	}
	addr, err = c.Endpoint(ctx, "")
	if err != nil {
		c.Terminate(ctx)
		return "", nil, fmt.Errorf("redis endpoint: %w", err)
	}
	return addr, func() { c.Terminate(ctx) }, nil
}

func StartRabbitMQ(ctx context.Context) (amqpURL string, cleanup CleanupFn, err error) {
	c, err := tcrabbitmq.Run(ctx, "rabbitmq:3-management-alpine")
	if err != nil {
		return "", nil, fmt.Errorf("start rabbitmq: %w", err)
	}
	amqpURL, err = c.AmqpURL(ctx)
	if err != nil {
		c.Terminate(ctx)
		return "", nil, fmt.Errorf("rabbitmq url: %w", err)
	}
	return amqpURL, func() { c.Terminate(ctx) }, nil
}

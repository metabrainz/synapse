//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

type CleanupFn func()

var noop CleanupFn = func() {}

// StartPostgres returns a Postgres DSN.
// If SYNAPSE_TEST_PG_DSN is set, it is used directly (no Docker needed).
// Otherwise a container is started via testcontainers.
func StartPostgres(ctx context.Context) (dsn string, cleanup CleanupFn, err error) {
	if dsn := os.Getenv("SYNAPSE_TEST_PG_DSN"); dsn != "" {
		return dsn, noop, nil
	}

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

// StartRedis returns a Redis address.
// If SYNAPSE_TEST_REDIS_ADDR is set, it is used directly.
func StartRedis(ctx context.Context) (addr string, cleanup CleanupFn, err error) {
	if addr := os.Getenv("SYNAPSE_TEST_REDIS_ADDR"); addr != "" {
		return addr, noop, nil
	}

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

// StartRabbitMQ returns an AMQP URL.
// If SYNAPSE_TEST_RABBITMQ_URL is set, it is used directly.
func StartRabbitMQ(ctx context.Context) (amqpURL string, cleanup CleanupFn, err error) {
	if url := os.Getenv("SYNAPSE_TEST_RABBITMQ_URL"); url != "" {
		return url, noop, nil
	}

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

// NewTestPool starts a real Postgres container, runs all migrations, and returns
// a ready-to-use pool. Cleanup is registered via t.Cleanup.
func NewTestPool(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn, cleanup, err := StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(cleanup)

	// Anchor migrations path to this file's location — works regardless of
	// which package calls this helper.
	_, filename, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(filename), "..", "migrations")

	m, err := migrate.New("file://"+migrationsDir, toPgx5URL(dsn))
	if err != nil {
		t.Fatalf("init migrate: %v", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		t.Fatalf("run migrations: %v", err)
	}
	m.Close()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	return pool
}

// toPgx5URL converts a postgres:// DSN to pgx5:// for golang-migrate.
func toPgx5URL(dsn string) string {
	if len(dsn) > 11 && dsn[:11] == "postgres://" {
		return fmt.Sprintf("pgx5://%s", dsn[11:])
	}
	return dsn
}

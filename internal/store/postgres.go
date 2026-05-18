package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/config"
)

// NewPool creates and validates a pgxpool with the given max connection count.
// MinConns is capped at 5 to avoid holding open connections the service will
// never actually use (e.g. the relay only needs workers connections max).
func NewPool(ctx context.Context, cfg config.PostgresConfig, maxConns int32) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("parse pg config: %w", err)
	}
	pcfg.MaxConns = maxConns
	pcfg.MinConns = min(5, maxConns)
	pcfg.MaxConnLifetime = 30 * time.Minute // rotate connections to avoid stale TCP
	pcfg.MaxConnIdleTime = 5 * time.Minute
	pcfg.HealthCheckPeriod = 30 * time.Second

	// PgBouncer transaction mode doesn't support extended query protocol (prepared
	// statements). Switch to simple protocol so every statement is sent as a plain
	// text query that PgBouncer can route without per-session state.
	if cfg.PgBouncer {
		pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return pool, nil
}

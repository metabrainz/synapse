// Package store provides shared database helpers: the Querier interface (satisfied
// by both *pgxpool.Pool and pgx.Tx), WithTx for transactional closures, and
// helpers for interpreting PostgreSQL error codes. The sub-packages (events,
// deliveries, outbox, …) contain the table-specific query functions.
package store

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx.
// Store methods that need to run inside a transaction accept a Querier
// so the caller decides whether to pass a pool or a tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// WithTx runs fn inside a transaction. Rolls back on error, commits on success.
// fn receives a Querier so it cannot commit or rollback itself.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(Querier) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

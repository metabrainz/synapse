package cleanup

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PruneOldEvents deletes events (and their deliveries via CASCADE) older than age.
// Runs inside a single transaction so a partial failure leaves no orphans.
func PruneOldEvents(ctx context.Context, pool *pgxpool.Pool, age time.Duration) error {
	cutoff := time.Now().Add(-age)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	res, err := tx.Exec(ctx,
		`DELETE FROM events WHERE created_at < $1`, cutoff,
	)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	slog.Info("cleanup: pruned old events", "rows", res.RowsAffected(), "cutoff", cutoff.Format(time.RFC3339))
	return nil
}

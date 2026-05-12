package cleanup

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReconcileStale marks stuck deliveries as DEAD:
//   - PENDING for more than 1 hour (relay likely crashed before publishing)
//   - RETRYING for more than 3 hours (retry loop stalled)
func ReconcileStale(ctx context.Context, pool *pgxpool.Pool) error {
	res, err := pool.Exec(ctx, `
		UPDATE deliveries
		SET    status     = 'DEAD',
		       last_error = 'reconciled by cleanup job',
		       updated_at = NOW()
		WHERE (status = 'PENDING'  AND created_at < NOW() - INTERVAL '1 hour')
		   OR (status = 'RETRYING' AND updated_at < NOW() - INTERVAL '3 hours')
	`)
	if err != nil {
		return err
	}

	slog.Info("cleanup: reconciled stale deliveries", "rows", res.RowsAffected())
	return nil
}

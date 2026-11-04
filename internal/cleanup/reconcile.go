// Package cleanup provides the maintenance jobs:
//   - ReconcileStale: marks stuck deliveries (PENDING/RETRYING past their age thresholds) as DEAD.
//   - PruneExpired: deletes notifications past their expires_at.
//   - PruneOldEvents: deletes events past a retention window that no notification
//     or unfinished delivery still needs.
//
// The cleanup binary runs these on a configurable schedule (or once and exit).
package cleanup

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ReconcileStale marks stuck deliveries as DEAD:
//   - PENDING longer than pendingAge (relay likely crashed before publishing)
//   - RETRYING longer than retryAge (retry loop stalled)
func ReconcileStale(ctx context.Context, pool *pgxpool.Pool, pendingAge, retryAge time.Duration) error {
	now := time.Now()
	res, err := pool.Exec(ctx, `
		UPDATE deliveries
		SET    status     = 'DEAD',
		       last_error = 'reconciled by cleanup: delivery stuck',
		       updated_at = NOW()
		WHERE (status = 'PENDING'  AND created_at < $1)
		   OR (status = 'RETRYING' AND updated_at < $2)
	`, now.Add(-pendingAge), now.Add(-retryAge))
	if err != nil {
		return err
	}
	slog.Info("cleanup: reconciled stale deliveries", "rows", res.RowsAffected())
	return nil
}

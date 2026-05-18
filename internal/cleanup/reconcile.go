// Package cleanup provides two maintenance jobs:
//   - ReconcileStale: marks stuck deliveries (PENDING/RETRYING past their age thresholds) as DEAD.
//   - PruneOldEvents: deletes events older than a configured retention window,
//     cascading to their deliveries via the FK relationship.
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

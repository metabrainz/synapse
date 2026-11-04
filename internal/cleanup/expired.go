package cleanup

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PruneExpired deletes notifications past their expires_at in batches and
// returns how many it deleted. The API already hides expired notifications; this
// reclaims the space.
func PruneExpired(ctx context.Context, pool *pgxpool.Pool, batchSize int) (int64, error) {
	var total int64
	for {
		res, err := pool.Exec(ctx,
			`DELETE FROM notifications
			 WHERE id IN (SELECT id FROM notifications WHERE expires_at < now() ORDER BY expires_at LIMIT $1)`,
			batchSize)
		if err != nil {
			return total, err
		}
		total += res.RowsAffected()
		if res.RowsAffected() < int64(batchSize) || ctx.Err() != nil {
			break
		}
	}
	slog.Info("cleanup: pruned expired notifications", "rows", total)
	return total, ctx.Err()
}

package cleanup

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PruneOldEvents deletes events older than age. Cascade delete on the FK
// removes their deliveries automatically. A single DELETE is statement-atomic
// in Postgres — no explicit transaction needed.
func PruneOldEvents(ctx context.Context, pool *pgxpool.Pool, age time.Duration) error {
	res, err := pool.Exec(ctx,
		`DELETE FROM events WHERE created_at < $1`,
		time.Now().Add(-age),
	)
	if err != nil {
		return err
	}
	slog.Info("cleanup: pruned old events", "rows", res.RowsAffected(), "age", age)
	return nil
}

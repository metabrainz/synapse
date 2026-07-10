package cleanup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var partitionedTables = []string{"events", "deliveries"}

// PruneOldEvents deletes events older than age. On partitioned tables,
// prefer DropOldPartitions instead (O(1), zero vacuum).
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

// DropOldPartitions drops monthly partitions older than age for both events
// and deliveries. O(1), zero dead tuples. No-op on unpartitioned tables.
func DropOldPartitions(ctx context.Context, pool *pgxpool.Pool, age time.Duration) error {
	cutoff := time.Now().Add(-age)

	for _, table := range partitionedTables {
		var isPartitioned bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS(
				SELECT 1 FROM pg_partitioned_table
				WHERE partrelid = $1::regclass
			)`, table).Scan(&isPartitioned)
		if err != nil || !isPartitioned {
			continue
		}

		rows, err := pool.Query(ctx,
			`SELECT inhrelid::regclass::text
			 FROM pg_inherits
			 WHERE inhparent = $1::regclass`, table)
		if err != nil {
			slog.Error("cleanup: list partitions", "table", table, "err", err)
			continue
		}

		var toDrop []string
		for rows.Next() {
			var partName string
			if err := rows.Scan(&partName); err != nil {
				continue
			}
			// Parse partition name: "events_2025_06" → 2025-06-01
			var year, month int
			prefix := table + "_"
			if n, _ := fmt.Sscanf(partName, prefix+"%d_%d", &year, &month); n == 2 {
				partEnd := time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, time.UTC)
				if partEnd.Before(cutoff) {
					toDrop = append(toDrop, partName)
				}
			}
		}
		rows.Close()

		for _, partName := range toDrop {
			if _, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", partName)); err != nil {
				slog.Error("cleanup: drop partition", "partition", partName, "err", err)
			} else {
				slog.Info("cleanup: dropped old partition", "partition", partName)
			}
		}
	}
	return nil
}

// EnsurePartitions creates monthly partitions for the next N months.
// No-op if the tables are not partitioned.
func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, monthsAhead int) error {
	for _, table := range partitionedTables {
		var isPartitioned bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS(
				SELECT 1 FROM pg_partitioned_table
				WHERE partrelid = $1::regclass
			)`, table).Scan(&isPartitioned)
		if err != nil || !isPartitioned {
			continue
		}

		for i := 0; i <= monthsAhead; i++ {
			start := time.Now().AddDate(0, i, 0)
			startMonth := time.Date(start.Year(), start.Month(), 1, 0, 0, 0, 0, time.UTC)
			endMonth := startMonth.AddDate(0, 1, 0)
			partName := fmt.Sprintf("%s_%s", table, startMonth.Format("2006_01"))

			_, err := pool.Exec(ctx, fmt.Sprintf(
				`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
				partName, table,
				startMonth.Format("2006-01-02"),
				endMonth.Format("2006-01-02"),
			))
			if err != nil {
				slog.Error("cleanup: create partition", "table", table, "partition", partName, "err", err)
			}
		}
	}
	return nil
}

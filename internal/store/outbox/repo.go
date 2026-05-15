package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/metabrainz/synapse/internal/store"
)

type Message struct {
	ID         int64
	RoutingKey string
	Payload    json.RawMessage
	CreatedAt  time.Time
}

// Insert writes an outbox row in the same transaction as event + deliveries.
func Insert(ctx context.Context, q store.Querier, routingKey string, payload json.RawMessage) error {
	_, err := q.Exec(ctx,
		`INSERT INTO outbox (routing_key, payload) VALUES ($1, $2)`,
		routingKey, payload,
	)
	return err
}

// ClaimBatch atomically marks up to limit PENDING rows as PUBLISHING and returns
// them. The CTE avoids a separate SELECT + UPDATE round-trip and ensures only
// this worker holds those rows — others skip them via SKIP LOCKED.
func ClaimBatch(ctx context.Context, pool *pgxpool.Pool, workerID string, limit int) ([]Message, error) {
	rows, err := pool.Query(ctx, `
		WITH claimed AS (
			SELECT id FROM outbox
			WHERE status = 'PENDING'
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox
		SET status = 'PUBLISHING', locked_by = $2, locked_at = NOW()
		WHERE id IN (SELECT id FROM claimed)
		RETURNING id, routing_key, payload, created_at`,
		limit, workerID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.RoutingKey, &m.Payload, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteClaimed removes rows after successful publish. The locked_by guard
// ensures a worker never deletes rows it didn't claim — safe across restarts.
func DeleteClaimed(ctx context.Context, pool *pgxpool.Pool, ids []int64, workerID string) error {
	_, err := pool.Exec(ctx,
		`DELETE FROM outbox WHERE id = ANY($1) AND locked_by = $2`,
		ids, workerID,
	)
	return err
}

// ResetStuck returns rows stuck in PUBLISHING longer than age back to PENDING.
// Called by the cleanup worker and on relay startup to handle crash recovery.
func ResetStuck(ctx context.Context, pool *pgxpool.Pool, age time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx, `
		UPDATE outbox
		SET status = 'PENDING', locked_by = NULL, locked_at = NULL
		WHERE status = 'PUBLISHING'
		  AND locked_at < NOW() - $1::INTERVAL`,
		age.String(),
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

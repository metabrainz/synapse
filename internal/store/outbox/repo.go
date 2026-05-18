// Package outbox implements the transactional outbox pattern. Rows are inserted
// atomically with their parent events and deliveries, then claimed by the relay
// using FOR UPDATE SKIP LOCKED so multiple relay workers cannot process the same
// row. After successful AMQP publish, rows are deleted; stuck PUBLISHING rows
// are reset to PENDING at relay startup by ResetStuck.
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

// InsertBatch writes all outbox rows in one round-trip using unnest. Always
// called inside the same transaction as events.InsertBatch and
// deliveries.InsertBatch so the three tables are written atomically — a crash
// before COMMIT leaves no partial state for the relay to act on.
func InsertBatch(ctx context.Context, q store.Querier, routingKeys []string, payloads []json.RawMessage) error {
	strs := make([]string, len(payloads))
	for i, p := range payloads {
		strs[i] = string(p)
	}
	_, err := q.Exec(ctx,
		`INSERT INTO outbox (routing_key, payload)
		 SELECT unnest($1::text[]), unnest($2::text[])::jsonb`,
		routingKeys, strs,
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

// FetchPending returns up to limit PENDING rows ordered by created_at without
// locking them. Safe to call inside or outside a transaction. Use for test
// assertions and health checks; use ClaimBatch for the production relay.
func FetchPending(ctx context.Context, q store.Querier, limit int) ([]Message, error) {
	rows, err := q.Query(ctx,
		`SELECT id, routing_key, payload, created_at FROM outbox
		 WHERE status = 'PENDING'
		 ORDER BY created_at ASC
		 LIMIT $1`,
		limit,
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

// DeleteBatch removes outbox rows by ID without checking the locked_by owner.
// Use for test cleanup; use DeleteClaimed for the production relay.
func DeleteBatch(ctx context.Context, q store.Querier, ids []int64) error {
	_, err := q.Exec(ctx, `DELETE FROM outbox WHERE id = ANY($1)`, ids)
	return err
}

// ResetStuck returns rows stuck in PUBLISHING longer than age back to PENDING.
// Called once at relay startup to handle crash recovery.
func ResetStuck(ctx context.Context, pool *pgxpool.Pool, age time.Duration) (int64, error) {
	tag, err := pool.Exec(ctx, `
		UPDATE outbox
		SET status = 'PENDING', locked_by = NULL, locked_at = NULL
		WHERE status = 'PUBLISHING'
		  AND locked_at < $1`,
		time.Now().Add(-age),
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

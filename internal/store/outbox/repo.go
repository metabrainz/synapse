package outbox

import (
	"context"
	"encoding/json"
	"time"

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

// FetchPending returns up to limit rows, oldest first, with FOR UPDATE SKIP LOCKED
// so multiple relay instances each claim a disjoint batch and never double-publish.
func FetchPending(ctx context.Context, q store.Querier, limit int) ([]Message, error) {
	rows, err := q.Query(ctx,
		`SELECT id, routing_key, payload, created_at
		 FROM outbox
		 ORDER BY created_at ASC
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
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

// DeleteBatch removes rows after successful publish to RabbitMQ.
// At-least-once guarantee: if relay crashes between publish and delete,
// rows survive and get re-published on restart.
func DeleteBatch(ctx context.Context, q store.Querier, ids []int64) error {
	_, err := q.Exec(ctx,
		`DELETE FROM outbox WHERE id = ANY($1)`, ids,
	)
	return err
}

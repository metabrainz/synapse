package events

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metabrainz/synapse/internal/store"
)

type Event struct {
	ID             int64
	TenantID       string
	UserID         string
	EventType      string
	Payload        json.RawMessage
	IdempotencyKey *string
	CreatedAt      time.Time
}

// Insert writes an event inside a transaction. Always called via store.WithTx.
func Insert(ctx context.Context, q store.Querier, e Event) (int64, error) {
	var id int64
	err := q.QueryRow(ctx,
		`INSERT INTO events (tenant_id, user_id, event_type, payload, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		e.TenantID, e.UserID, e.EventType, e.Payload, e.IdempotencyKey,
	).Scan(&id)
	return id, err
}

func GetByID(ctx context.Context, q store.Querier, id int64) (*Event, error) {
	var e Event
	err := q.QueryRow(ctx,
		`SELECT id, tenant_id, user_id, event_type, payload, idempotency_key, created_at
		 FROM events WHERE id = $1`, id,
	).Scan(&e.ID, &e.TenantID, &e.UserID, &e.EventType, &e.Payload, &e.IdempotencyKey, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &e, err
}

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

// InsertBatch inserts multiple events in one round-trip and returns their IDs
// in the same order as the input slice. Uses unnest to avoid N individual inserts.
func InsertBatch(ctx context.Context, q store.Querier, evs []Event) ([]int64, error) {
	n := len(evs)
	tenantIDs := make([]string, n)
	userIDs := make([]string, n)
	eventTypes := make([]string, n)
	payloads := make([]string, n)
	ikeys := make([]*string, n)

	for i, e := range evs {
		tenantIDs[i] = e.TenantID
		userIDs[i] = e.UserID
		eventTypes[i] = e.EventType
		payloads[i] = string(e.Payload)
		ikeys[i] = e.IdempotencyKey
	}

	rows, err := q.Query(ctx,
		`INSERT INTO events (tenant_id, user_id, event_type, payload, idempotency_key)
		 SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::text[]),
		        unnest($4::text[])::jsonb, unnest($5::text[])
		 RETURNING id`,
		tenantIDs, userIDs, eventTypes, payloads, ikeys,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]int64, 0, n)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
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

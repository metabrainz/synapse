// Package events manages the events table. InsertBatch uses unnest to write
// multiple rows in one round-trip and returns IDs in the same order as the
// input slice, so callers can wire event IDs into subsequent delivery inserts
// without a secondary query.
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
// Returns the event with ID and CreatedAt populated from the DB.
func Insert(ctx context.Context, q store.Querier, event Event) (Event, error) {
	err := q.QueryRow(ctx,
		`INSERT INTO events (tenant_id, user_id, event_type, payload, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5) RETURNING id, created_at`,
		event.TenantID, event.UserID, event.EventType, event.Payload, event.IdempotencyKey,
	).Scan(&event.ID, &event.CreatedAt)
	return event, err
}

// InsertBatch inserts n events in one round-trip by expanding parallel typed
// arrays with unnest. PostgreSQL preserves array element order within a single
// unnest, so the returned IDs are positionally aligned with the input slice —
// callers can zip evs[i] with ids[i] without any secondary sort.
func InsertBatch(ctx context.Context, q store.Querier, evs []Event) ([]int64, error) {
	n := len(evs)
	tenantIDs := make([]string, n)
	userIDs := make([]string, n)
	eventTypes := make([]string, n)
	payloads := make([]string, n)
	ikeys := make([]*string, n)

	for i, event := range evs {
		tenantIDs[i] = event.TenantID
		userIDs[i] = event.UserID
		eventTypes[i] = event.EventType
		payloads[i] = string(event.Payload)
		ikeys[i] = event.IdempotencyKey
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
	var event Event
	err := q.QueryRow(ctx,
		`SELECT id, tenant_id, user_id, event_type, payload, idempotency_key, created_at
		 FROM events WHERE id = $1`, id,
	).Scan(&event.ID, &event.TenantID, &event.UserID, &event.EventType, &event.Payload, &event.IdempotencyKey, &event.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &event, err
}

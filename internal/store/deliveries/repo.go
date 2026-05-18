// Package deliveries tracks the status of each (event, channel) delivery attempt.
// InsertBatch returns IDs positionally aligned with its input so the fanout can
// embed delivery IDs into outbox payloads without a second query.
package deliveries

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metabrainz/synapse/internal/store"
)

const (
	StatusPending   = "PENDING"
	StatusRetrying  = "RETRYING"
	StatusDelivered = "DELIVERED"
	StatusDead      = "DEAD"
)

type Delivery struct {
	ID          int64      `json:"id"`
	EventID     int64      `json:"event_id"`
	ChannelID   int64      `json:"channel_id"`
	ChannelType string     `json:"channel_type"`
	Status      string     `json:"status"`
	Attempt     int        `json:"attempt"`
	MaxAttempts int        `json:"max_attempts"`
	LastError   *string    `json:"last_error,omitempty"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// InsertBatch inserts n deliveries in one round-trip using unnest. Returned IDs
// are positionally aligned with the input slice — callers can zip ds[i] with
// ids[i] to embed the database-assigned ID into outbox payloads immediately
// after this call, without a second query.
func InsertBatch(ctx context.Context, q store.Querier, ds []Delivery) ([]int64, error) {
	n := len(ds)
	eventIDs := make([]int64, n)
	channelIDs := make([]int64, n)
	channelTypes := make([]string, n)
	maxAttempts := make([]int32, n)

	for i, d := range ds {
		eventIDs[i] = d.EventID
		channelIDs[i] = d.ChannelID
		channelTypes[i] = d.ChannelType
		maxAttempts[i] = int32(d.MaxAttempts)
	}

	rows, err := q.Query(ctx,
		`INSERT INTO deliveries (event_id, channel_id, channel_type, max_attempts)
		 SELECT unnest($1::bigint[]), unnest($2::bigint[]), unnest($3::text[]), unnest($4::int[])
		 RETURNING id`,
		eventIDs, channelIDs, channelTypes, maxAttempts,
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

func UpdateStatus(ctx context.Context, q store.Querier, id int64, status string, attempt int, lastErr *string) error {
	_, err := q.Exec(ctx,
		`UPDATE deliveries
		 SET status      = $2,
		     attempt     = $3,
		     last_error  = $4,
		     delivered_at = CASE WHEN $2 = 'DELIVERED' THEN NOW() ELSE NULL END,
		     updated_at  = NOW()
		 WHERE id = $1`,
		id, status, attempt, lastErr,
	)
	return err
}

func ListByEvent(ctx context.Context, q store.Querier, eventID int64) ([]Delivery, error) {
	rows, err := q.Query(ctx,
		`SELECT id, event_id, channel_id, channel_type, status, attempt,
		        max_attempts, last_error, delivered_at, created_at, updated_at
		 FROM deliveries WHERE event_id = $1 ORDER BY id`,
		eventID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Delivery
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(
			&d.ID, &d.EventID, &d.ChannelID, &d.ChannelType, &d.Status,
			&d.Attempt, &d.MaxAttempts, &d.LastError, &d.DeliveredAt,
			&d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func GetByID(ctx context.Context, q store.Querier, id int64) (*Delivery, error) {
	var d Delivery
	err := q.QueryRow(ctx,
		`SELECT id, event_id, channel_id, channel_type, status, attempt,
		        max_attempts, last_error, delivered_at, created_at, updated_at
		 FROM deliveries WHERE id = $1`, id,
	).Scan(
		&d.ID, &d.EventID, &d.ChannelID, &d.ChannelType, &d.Status,
		&d.Attempt, &d.MaxAttempts, &d.LastError, &d.DeliveredAt,
		&d.CreatedAt, &d.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &d, err
}

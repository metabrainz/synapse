// Package deliveries tracks the status of each (event, channel) delivery attempt.
// InsertBatch returns IDs positionally aligned with its input so the fanout can
// embed delivery IDs into outbox payloads without a second query.
package deliveries

import (
	"context"
	"time"

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
	UserID      string     `json:"user_id"`
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
func InsertBatch(ctx context.Context, q store.Querier, batch []Delivery) ([]int64, error) {
	n := len(batch)
	eventIDs := make([]int64, n)
	userIDs := make([]string, n)
	channelIDs := make([]int64, n)
	channelTypes := make([]string, n)
	maxAttempts := make([]int32, n)

	for i, delivery := range batch {
		eventIDs[i] = delivery.EventID
		userIDs[i] = delivery.UserID
		channelIDs[i] = delivery.ChannelID
		channelTypes[i] = delivery.ChannelType
		maxAttempts[i] = int32(delivery.MaxAttempts)
	}

	rows, err := q.Query(ctx,
		`INSERT INTO deliveries (event_id, user_id, channel_id, channel_type, max_attempts)
		 SELECT unnest($1::bigint[]), unnest($2::text[]), unnest($3::bigint[]), unnest($4::text[]), unnest($5::int[])
		 RETURNING id`,
		eventIDs, userIDs, channelIDs, channelTypes, maxAttempts,
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

// ListByEventForTenant returns deliveries for an event owned by tenantID.
// The second return value is false when no such event exists for the tenant.
func ListByEventForTenant(ctx context.Context, q store.Querier, eventID int64, tenantID string) ([]Delivery, bool, error) {
	var exists bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM events WHERE id = $1 AND tenant_id = $2)`,
		eventID, tenantID,
	).Scan(&exists); err != nil || !exists {
		return nil, false, err
	}
	deliveries, err := ListByEvent(ctx, q, eventID)
	return deliveries, true, err
}

func ListByEvent(ctx context.Context, q store.Querier, eventID int64) ([]Delivery, error) {
	rows, err := q.Query(ctx,
		`SELECT id, event_id, user_id, channel_id, channel_type, status, attempt,
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
		var delivery Delivery
		if err := rows.Scan(
			&delivery.ID, &delivery.EventID, &delivery.UserID, &delivery.ChannelID, &delivery.ChannelType, &delivery.Status,
			&delivery.Attempt, &delivery.MaxAttempts, &delivery.LastError, &delivery.DeliveredAt,
			&delivery.CreatedAt, &delivery.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, delivery)
	}
	return out, rows.Err()
}


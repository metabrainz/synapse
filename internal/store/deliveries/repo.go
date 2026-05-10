package deliveries

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/metabrainz/synapse/internal/store"
)

type Delivery struct {
	ID          int64
	EventID     int64
	ChannelID   int64
	ChannelType string
	Status      string
	Attempt     int
	MaxAttempts int
	LastError   *string
	DeliveredAt *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func Insert(ctx context.Context, q store.Querier, d Delivery) (int64, error) {
	var id int64
	err := q.QueryRow(ctx,
		`INSERT INTO deliveries (event_id, channel_id, channel_type, max_attempts)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		d.EventID, d.ChannelID, d.ChannelType, d.MaxAttempts,
	).Scan(&id)
	return id, err
}

func UpdateStatus(ctx context.Context, q store.Querier, id int64, status string, attempt int, lastErr *string) error {
	_, err := q.Exec(ctx,
		`UPDATE deliveries
		 SET status      = $2,
		     attempt     = $3,
		     last_error  = $4,
		     delivered_at = CASE WHEN $2 = 'delivered' THEN NOW() ELSE NULL END,
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

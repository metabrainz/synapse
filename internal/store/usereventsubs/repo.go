package usereventsubs

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Subscription struct {
	UserID      string    `json:"user_id"`
	TenantID    string    `json:"tenant_id"`
	EventType   string    `json:"event_type"`
	ChannelType string    `json:"channel_type"`
	IsEnabled   bool      `json:"is_enabled"`
	CreatedAt   time.Time `json:"created_at"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Upsert(ctx context.Context, s Subscription) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO user_event_subscriptions
		     (user_id, tenant_id, event_type, channel_type, is_enabled)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (user_id, tenant_id, event_type, channel_type)
		 DO UPDATE SET is_enabled = EXCLUDED.is_enabled`,
		s.UserID, s.TenantID, s.EventType, s.ChannelType, s.IsEnabled,
	)
	return err
}

func (r *Repo) SetEnabled(ctx context.Context, userID, tenantID, eventType, channelType string, enabled bool) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE user_event_subscriptions SET is_enabled = $5
		 WHERE user_id = $1 AND tenant_id = $2 AND event_type = $3 AND channel_type = $4`,
		userID, tenantID, eventType, channelType, enabled,
	)
	return err
}

func (r *Repo) Delete(ctx context.Context, userID, tenantID, eventType, channelType string) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM user_event_subscriptions
		 WHERE user_id = $1 AND tenant_id = $2 AND event_type = $3 AND channel_type = $4`,
		userID, tenantID, eventType, channelType,
	)
	return err
}

func (r *Repo) ListByUserTenant(ctx context.Context, userID, tenantID string) ([]Subscription, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT user_id, tenant_id, event_type, channel_type, is_enabled, created_at
		 FROM user_event_subscriptions
		 WHERE user_id = $1 AND tenant_id = $2 ORDER BY event_type, channel_type`,
		userID, tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Subscription
	for rows.Next() {
		var s Subscription
		if err := rows.Scan(&s.UserID, &s.TenantID, &s.EventType, &s.ChannelType,
			&s.IsEnabled, &s.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Package subscriptions manages the mapping from channels to event types.
// A subscription with event_type = '*' matches every event for that user.
// ListAllForCache loads the full subscription table for the in-memory fanout
// cache; ListActiveForEvent is the live DB fallback used without the cache.
//
// The three-gate model requires all of: admin rule (tenant_event_channel_rules),
// user assignment (user_tenant_channel_mapping), and user subscription
// (user_event_subscriptions) to be enabled before a delivery is created.
package subscriptions

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Subscription struct {
	ID        int64           `json:"id"`
	ChannelID int64           `json:"channel_id"`
	EventType string          `json:"event_type"`
	Enabled   bool            `json:"enabled"`
	Config    json.RawMessage `json:"config"`
	CreatedAt time.Time       `json:"created_at"`
}

// ActiveChannel is the minimal shape the fanout needs per matched subscription.
type ActiveChannel struct {
	ChannelID   int64
	ChannelType string
	Config      json.RawMessage
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Insert(ctx context.Context, s Subscription) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`INSERT INTO subscriptions (channel_id, event_type, config)
		 VALUES ($1, $2, $3) RETURNING id`,
		s.ChannelID, s.EventType, s.Config,
	).Scan(&id)
	return id, err
}

func (r *Repo) Delete(ctx context.Context, channelID int64, eventType string) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM subscriptions WHERE channel_id = $1 AND event_type = $2`,
		channelID, eventType,
	)
	return err
}

func (r *Repo) ListByChannel(ctx context.Context, channelID int64) ([]Subscription, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, channel_id, event_type, enabled, config, created_at
		 FROM subscriptions WHERE channel_id = $1 ORDER BY event_type`,
		channelID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Subscription
	for rows.Next() {
		var s Subscription
		if err := rows.Scan(&s.ID, &s.ChannelID, &s.EventType, &s.Enabled, &s.Config, &s.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// ListActiveForEvent returns all active channels that pass all three gates for the given event.
// Gate 1: tenant_event_channel_rules.is_allowed (admin policy)
// Gate 2: user_tenant_channel_mapping.is_enabled (user assignment)
// Gate 3: user_event_subscriptions.is_enabled (user subscription)
func (r *Repo) ListActiveForEvent(ctx context.Context, tenantID, userID, eventType string) ([]ActiveChannel, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT uc.id, uc.channel_type, uc.config
		 FROM tenant_event_channel_rules r
		 JOIN user_tenant_channel_mapping utm
		   ON utm.tenant_id    = r.tenant_id
		  AND utm.channel_type = r.channel_type
		  AND utm.user_id      = $1
		 JOIN user_channels uc
		   ON uc.id        = utm.user_channel_id
		  AND uc.is_active  = TRUE
		 JOIN user_event_subscriptions ues
		   ON ues.user_id      = $1
		  AND ues.tenant_id    = $2
		  AND ues.event_type   = $3
		  AND ues.channel_type = r.channel_type
		 WHERE r.tenant_id  = $2
		   AND r.event_type = $3
		   AND r.is_allowed      = TRUE
		   AND utm.is_enabled    = TRUE
		   AND ues.is_enabled    = TRUE`,
		userID, tenantID, eventType,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ActiveChannel
	for rows.Next() {
		var ac ActiveChannel
		if err := rows.Scan(&ac.ChannelID, &ac.ChannelType, &ac.Config); err != nil {
			return nil, err
		}
		out = append(out, ac)
	}
	return out, rows.Err()
}

// CacheEntry is a fully-qualified subscription row used to populate the fanout cache.
type CacheEntry struct {
	TenantID  string
	UserID    string
	EventType string
	ActiveChannel
}

// ListAllForCache loads every active subscription with full routing context.
// All three gates must be enabled: admin rule, user assignment, and user subscription.
func (r *Repo) ListAllForCache(ctx context.Context) ([]CacheEntry, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT utm.user_id, r.tenant_id, r.event_type,
		        uc.id, uc.channel_type, uc.config
		 FROM tenant_event_channel_rules r
		 JOIN user_tenant_channel_mapping utm
		   ON utm.tenant_id    = r.tenant_id
		  AND utm.channel_type = r.channel_type
		 JOIN user_channels uc
		   ON uc.id       = utm.user_channel_id
		  AND uc.is_active = TRUE
		 JOIN user_event_subscriptions ues
		   ON ues.user_id      = utm.user_id
		  AND ues.tenant_id    = r.tenant_id
		  AND ues.event_type   = r.event_type
		  AND ues.channel_type = r.channel_type
		 WHERE r.is_allowed   = TRUE
		   AND utm.is_enabled = TRUE
		   AND ues.is_enabled = TRUE`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CacheEntry
	for rows.Next() {
		var e CacheEntry
		if err := rows.Scan(
			&e.UserID, &e.TenantID, &e.EventType,
			&e.ChannelID, &e.ChannelType, &e.Config,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

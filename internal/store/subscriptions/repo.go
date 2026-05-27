// Package subscriptions resolves which delivery channels should receive an event.
// Gate 1 (tenant channel rules) is enforced in the application layer via the static
// registry. This package enforces Gates 2 (user tenant assignment) and 3 (user subscription).
package subscriptions

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ActiveChannel is the minimal shape the fanout needs per matched subscription.
type ActiveChannel struct {
	ChannelID   int64
	ChannelType string
	Config      json.RawMessage
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// ListActiveForEvent returns active channels that pass Gates 2 and 3 for the given event.
// Gate 2: user_tenant_channel_mapping.is_enabled (user has assigned and enabled a channel for this tenant)
// Gate 3: user_event_subscriptions.is_enabled (user has subscribed to this event type on this channel)
// Gate 1 (tenant channel rules) is applied in the fanout layer via the static registry.
func (r *Repo) ListActiveForEvent(ctx context.Context, tenantID, userID, eventType string) ([]ActiveChannel, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT uc.id, uc.channel_type, uc.config
		 FROM user_tenant_channel_mapping utm
		 JOIN user_channels uc
		   ON uc.id       = utm.user_channel_id
		  AND uc.is_active = TRUE
		 JOIN user_event_subscriptions ues
		   ON ues.user_id      = utm.user_id
		  AND ues.tenant_id    = utm.tenant_id
		  AND ues.event_type   = $3
		  AND ues.channel_type = utm.channel_type
		 WHERE utm.user_id    = $1
		   AND utm.tenant_id  = $2
		   AND utm.is_enabled = TRUE
		   AND ues.is_enabled = TRUE`,
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
// Gates 2 and 3 must be enabled. Gate 1 is filtered in the cache rebuild loop.
func (r *Repo) ListAllForCache(ctx context.Context) ([]CacheEntry, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT utm.user_id, utm.tenant_id, ues.event_type,
		        uc.id, uc.channel_type, uc.config
		 FROM user_tenant_channel_mapping utm
		 JOIN user_channels uc
		   ON uc.id       = utm.user_channel_id
		  AND uc.is_active = TRUE
		 JOIN user_event_subscriptions ues
		   ON ues.user_id      = utm.user_id
		  AND ues.tenant_id    = utm.tenant_id
		  AND ues.channel_type = utm.channel_type
		 WHERE utm.is_enabled = TRUE
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

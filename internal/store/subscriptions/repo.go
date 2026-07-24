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
// UserID is the recipient — recorded on each delivery so it survives channel deletion.
type ActiveChannel struct {
	UserID      string
	Username    string
	ChannelID   int64
	ChannelType string
	Config      json.RawMessage
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// ListActiveForRecipients resolves active channels for many recipients in one query.
// It is the DB fallback for the fanout; production resolves from the in-memory Cache.
// Like ListActiveForEvent, it matches exact event types only (wildcard '*' subscriptions
// are expanded by the cache layer, not here).
func (r *Repo) ListActiveForRecipients(ctx context.Context, tenantID, eventType string, recipients []string) ([]ActiveChannel, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT channel_mapping.user_id, u.username, channel.id, channel.channel_type, channel.config
		 FROM user_tenant_channel_mapping channel_mapping
		 JOIN users u ON u.id = channel_mapping.user_id
		 JOIN user_channels channel
		   ON channel.id        = channel_mapping.user_channel_id
		  AND channel.is_active = TRUE
		 JOIN user_event_subscriptions event_sub
		   ON event_sub.user_id      = channel_mapping.user_id
		  AND event_sub.tenant_id    = channel_mapping.tenant_id
		  AND event_sub.event_type   = $2
		  AND event_sub.channel_type = channel_mapping.channel_type
		 WHERE channel_mapping.tenant_id  = $1
		   AND channel_mapping.user_id    = ANY($3)
		   AND channel_mapping.is_enabled = TRUE
		   AND event_sub.is_enabled       = TRUE`,
		tenantID, eventType, recipients,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ActiveChannel
	for rows.Next() {
		var channel ActiveChannel
		if err := rows.Scan(&channel.UserID, &channel.Username, &channel.ChannelID, &channel.ChannelType, &channel.Config); err != nil {
			return nil, err
		}
		out = append(out, channel)
	}
	return out, rows.Err()
}

// CacheEntry is a fully-qualified subscription row used to populate the fanout cache.
// UserID comes from the embedded ActiveChannel.
type CacheEntry struct {
	TenantID  string
	EventType string
	ActiveChannel
}

// ListAllForCache loads every active subscription with full routing context.
// Gates 2 and 3 must be enabled. Gate 1 is filtered in the cache rebuild loop.
func (r *Repo) ListAllForCache(ctx context.Context) ([]CacheEntry, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT channel_mapping.user_id, u.username, channel_mapping.tenant_id, event_sub.event_type,
		        channel.id, channel.channel_type, channel.config
		 FROM user_tenant_channel_mapping channel_mapping
		 JOIN users u ON u.id = channel_mapping.user_id
		 JOIN user_channels channel
		   ON channel.id        = channel_mapping.user_channel_id
		  AND channel.is_active = TRUE
		 JOIN user_event_subscriptions event_sub
		   ON event_sub.user_id      = channel_mapping.user_id
		  AND event_sub.tenant_id    = channel_mapping.tenant_id
		  AND event_sub.channel_type = channel_mapping.channel_type
		 WHERE channel_mapping.is_enabled = TRUE
		   AND event_sub.is_enabled = TRUE`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CacheEntry
	for rows.Next() {
		var entry CacheEntry
		if err := rows.Scan(
			&entry.UserID, &entry.Username, &entry.TenantID, &entry.EventType,
			&entry.ChannelID, &entry.ChannelType, &entry.Config,
		); err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

package subscriptions

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Subscription struct {
	ID        int64
	ChannelID int64
	EventType string
	Enabled   bool
	Config    json.RawMessage
	CreatedAt time.Time
}

// ActiveChannel is the minimal shape the fanout needs per matched subscription.
type ActiveChannel struct {
	ChannelID   int64
	ChannelType string
	Config      json.RawMessage
	SubConfig   json.RawMessage
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

// ListActiveForEvent returns all active channels subscribed to the given event.
// Matches both the specific event_type and the wildcard '*'.
func (r *Repo) ListActiveForEvent(ctx context.Context, tenantID, userID, eventType string) ([]ActiveChannel, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT c.id, c.type, c.config, s.config
		 FROM subscriptions s
		 JOIN channels c ON c.id = s.channel_id
		 WHERE c.tenant_id = $1
		   AND c.user_id   = $2
		   AND c.enabled   = TRUE
		   AND c.status    = 'active'
		   AND s.enabled   = TRUE
		   AND s.event_type IN ($3, '*')`,
		tenantID, userID, eventType,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ActiveChannel
	for rows.Next() {
		var ac ActiveChannel
		if err := rows.Scan(&ac.ChannelID, &ac.ChannelType, &ac.Config, &ac.SubConfig); err != nil {
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
// Used to warm the in-memory fanout cache on startup and after LISTEN/NOTIFY.
func (r *Repo) ListAllForCache(ctx context.Context) ([]CacheEntry, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT c.tenant_id, c.user_id, s.event_type, c.id, c.type, c.config, s.config
		 FROM subscriptions s
		 JOIN channels c ON c.id = s.channel_id
		 WHERE c.enabled = TRUE AND c.status = 'active' AND s.enabled = TRUE`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CacheEntry
	for rows.Next() {
		var e CacheEntry
		if err := rows.Scan(
			&e.TenantID, &e.UserID, &e.EventType,
			&e.ChannelID, &e.ChannelType, &e.Config, &e.SubConfig,
		); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

type scanner interface{ Scan(dest ...any) error }

func scan(s scanner) (*Subscription, error) {
	var sub Subscription
	err := s.Scan(&sub.ID, &sub.ChannelID, &sub.EventType, &sub.Enabled, &sub.Config, &sub.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &sub, err
}

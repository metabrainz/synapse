// Package notifications stores a user's inbox entries. The payload stays in events.
package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/store"
)

var ErrNotFound = errors.New("notification not found")

type Entry struct {
	EventID   int64
	UserID    string
	TenantID  string
	ExpiresAt time.Time
}

// InsertBatch skips a repeated (event, user) pair instead of failing the batch.
func InsertBatch(ctx context.Context, q store.Querier, batch []Entry) error {
	n := len(batch)
	eventIDs := make([]int64, n)
	userIDs := make([]string, n)
	tenantIDs := make([]string, n)
	expiresAt := make([]time.Time, n)
	for i, nt := range batch {
		eventIDs[i] = nt.EventID
		userIDs[i] = nt.UserID
		tenantIDs[i] = nt.TenantID
		expiresAt[i] = nt.ExpiresAt
	}
	_, err := q.Exec(ctx,
		`INSERT INTO notifications (event_id, user_id, tenant_id, expires_at)
		 SELECT unnest($1::bigint[]), unnest($2::text[]), unnest($3::text[]), unnest($4::timestamptz[])
		 ON CONFLICT (event_id, user_id) DO NOTHING`,
		eventIDs, userIDs, tenantIDs, expiresAt,
	)
	return err
}

type Notification struct {
	ID        int64           `json:"id" doc:"Stable, increasing; use as the pagination cursor"`
	UserID    string          `json:"-"`
	EventID   int64           `json:"event_id" doc:"Source event ID"`
	TenantID  string          `json:"tenant_id" doc:"Tenant that produced the event"`
	EventType string          `json:"event_type" doc:"Event type, e.g. follow"`
	Payload   json.RawMessage `json:"payload" doc:"Event payload exactly as published by the tenant"`
	ReadAt    *time.Time      `json:"read_at,omitempty" doc:"When it was marked read; absent while unread"`
	CreatedAt time.Time       `json:"created_at" doc:"When the event was created"`
	ExpiresAt time.Time       `json:"expires_at" doc:"After this time the notification is no longer returned"`
}

// ListParams.TenantID limits the list to one tenant; empty means every tenant.
type ListParams struct {
	UserID     string
	TenantID   string
	BeforeID   int64
	EventType  string
	UnreadOnly bool
	Limit      int
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// inboxWhere selects a user's visible notifications for one tenant, or all when
// tenantID is empty. Expired rows are hidden even before cleanup deletes them.
func inboxWhere(userID, tenantID string) (string, []any) {
	where := `n.user_id = $1 AND n.dismissed_at IS NULL AND n.expires_at > now()`
	args := []any{userID}
	if tenantID != "" {
		where += ` AND n.tenant_id = $2`
		args = append(args, tenantID)
	}
	return where, args
}

// List returns up to p.Limit notifications, newest first, and the cursor for the
// next page (0 when there are no more).
func (r *Repo) List(ctx context.Context, p ListParams) ([]Notification, int64, error) {
	where, args := inboxWhere(p.UserID, p.TenantID)
	if p.BeforeID > 0 {
		args = append(args, p.BeforeID)
		where += fmt.Sprintf(` AND n.id < $%d`, len(args))
	}
	if p.EventType != "" {
		args = append(args, p.EventType)
		where += fmt.Sprintf(` AND e.event_type = $%d`, len(args))
	}
	if p.UnreadOnly {
		where += ` AND n.read_at IS NULL`
	}
	args = append(args, p.Limit+1)

	rows, err := r.pool.Query(ctx,
		`SELECT n.id, n.event_id, n.tenant_id, e.event_type, e.payload, n.read_at, e.created_at, n.expires_at
		 FROM notifications n
		 JOIN events e ON e.id = n.event_id
		 WHERE `+where+fmt.Sprintf(` ORDER BY n.id DESC LIMIT $%d`, len(args)),
		args...,
	)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := []Notification{}
	for rows.Next() {
		var n Notification
		if err := rows.Scan(&n.ID, &n.EventID, &n.TenantID, &n.EventType, &n.Payload, &n.ReadAt, &n.CreatedAt, &n.ExpiresAt); err != nil {
			return nil, 0, err
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	var next int64
	if len(out) > p.Limit {
		out = out[:p.Limit]
		next = out[len(out)-1].ID
	}
	return out, next, nil
}

// ListByEvent includes expired and dismissed notifications: it records who was notified.
func (r *Repo) ListByEvent(ctx context.Context, eventID int64) ([]Notification, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, event_id, user_id, tenant_id, created_at, expires_at
		 FROM notifications WHERE event_id = $1 ORDER BY id`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Notification
	for rows.Next() {
		var n Notification
		if err := rows.Scan(&n.ID, &n.EventID, &n.UserID, &n.TenantID, &n.CreatedAt, &n.ExpiresAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (r *Repo) UnreadCount(ctx context.Context, userID, tenantID string) (int64, error) {
	where, args := inboxWhere(userID, tenantID)
	var n int64
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM notifications n WHERE `+where+` AND n.read_at IS NULL`, args...).Scan(&n)
	return n, err
}

func (r *Repo) UnreadCountsByTenant(ctx context.Context, userID string) (map[string]int64, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT n.tenant_id, count(*) FROM notifications n
		 WHERE n.user_id = $1 AND n.dismissed_at IS NULL AND n.expires_at > now() AND n.read_at IS NULL
		 GROUP BY n.tenant_id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var tenant string
		var n int64
		if err := rows.Scan(&tenant, &n); err != nil {
			return nil, err
		}
		out[tenant] = n
	}
	return out, rows.Err()
}

// MarkRead is idempotent: re-marking keeps the original read_at. It returns the notification's tenant.
func (r *Repo) MarkRead(ctx context.Context, userID, tenantID string, id int64) (string, error) {
	where, args := inboxWhere(userID, tenantID)
	args = append(args, id)
	return r.updateOne(ctx,
		fmt.Sprintf(`UPDATE notifications n SET read_at = COALESCE(n.read_at, now()) WHERE n.id = $%d AND `, len(args))+where+` RETURNING n.tenant_id`, args...)
}

func (r *Repo) Dismiss(ctx context.Context, userID, tenantID string, id int64) (string, error) {
	where, args := inboxWhere(userID, tenantID)
	args = append(args, id)
	return r.updateOne(ctx,
		fmt.Sprintf(`UPDATE notifications n SET dismissed_at = now() WHERE n.id = $%d AND `, len(args))+where+` RETURNING n.tenant_id`, args...)
}

// MarkAllRead returns the number of newly read notifications per tenant.
func (r *Repo) MarkAllRead(ctx context.Context, userID, tenantID string) (map[string]int64, error) {
	where, args := inboxWhere(userID, tenantID)
	rows, err := r.pool.Query(ctx, `UPDATE notifications n SET read_at = now() WHERE n.read_at IS NULL AND `+where+` RETURNING n.tenant_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			return nil, err
		}
		out[tenant]++
	}
	return out, rows.Err()
}

func (r *Repo) updateOne(ctx context.Context, query string, args ...any) (string, error) {
	var tenant string
	err := r.pool.QueryRow(ctx, query, args...).Scan(&tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return tenant, err
}

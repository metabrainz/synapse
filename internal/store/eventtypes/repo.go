package eventtypes

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("event type not found")

type EventType struct {
	TenantID    string `json:"tenant_id"`
	EventType   string `json:"event_type"`
	Description string `json:"description"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Upsert(ctx context.Context, tenantID, eventType, description string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO event_type_definitions (tenant_id, event_type, description)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (tenant_id, event_type) DO UPDATE SET description = EXCLUDED.description`,
		tenantID, eventType, description,
	)
	return err
}

func (r *Repo) List(ctx context.Context, tenantID string) ([]EventType, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, event_type, COALESCE(description, '') FROM event_type_definitions WHERE tenant_id = $1 ORDER BY event_type`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventType
	for rows.Next() {
		var et EventType
		if err := rows.Scan(&et.TenantID, &et.EventType, &et.Description); err != nil {
			return nil, err
		}
		out = append(out, et)
	}
	return out, rows.Err()
}

func (r *Repo) Delete(ctx context.Context, tenantID, eventType string) error {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM event_type_definitions WHERE tenant_id = $1 AND event_type = $2`,
		tenantID, eventType,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Exists is used by the ingest handler to give a clear 400 instead of a FK error.
func (r *Repo) Exists(ctx context.Context, tenantID, eventType string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM event_type_definitions WHERE tenant_id = $1 AND event_type = $2)`,
		tenantID, eventType,
	).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return exists, err
}

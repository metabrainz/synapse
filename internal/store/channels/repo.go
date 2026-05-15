package channels

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/store"
)

type Channel struct {
	ID                  int64           `json:"id"`
	TenantID            string          `json:"tenant_id"`
	UserID              string          `json:"user_id"`
	Type                string          `json:"type"`
	Config              json.RawMessage `json:"config"`
	Enabled             bool            `json:"enabled"`
	Status              string          `json:"status"`
	ConsecutiveFailures int             `json:"consecutive_failures"`
	LastFailedAt        *time.Time      `json:"last_failed_at,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Insert(ctx context.Context, q store.Querier, c Channel) (int64, error) {
	var id int64
	err := q.QueryRow(ctx,
		`INSERT INTO channels (tenant_id, user_id, type, config)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		c.TenantID, c.UserID, c.Type, c.Config,
	).Scan(&id)
	return id, err
}

func (r *Repo) GetByID(ctx context.Context, id int64) (*Channel, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, user_id, type, config, enabled, status,
		        consecutive_failures, last_failed_at, created_at, updated_at
		 FROM channels WHERE id = $1`, id,
	)
	return scan(row)
}

func (r *Repo) ListByUser(ctx context.Context, tenantID, userID string) ([]Channel, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, user_id, type, config, enabled, status,
		        consecutive_failures, last_failed_at, created_at, updated_at
		 FROM channels WHERE tenant_id = $1 AND user_id = $2 ORDER BY id`,
		tenantID, userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Channel
	for rows.Next() {
		c, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

func (r *Repo) Delete(ctx context.Context, tenantID string, id int64) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM channels WHERE id = $1 AND tenant_id = $2`, id, tenantID,
	)
	return err
}

func (r *Repo) RecordFailure(ctx context.Context, id int64, threshold int) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE channels
		 SET consecutive_failures = consecutive_failures + 1,
		     last_failed_at = NOW(),
		     status = CASE WHEN consecutive_failures + 1 >= $2 THEN 'broken' ELSE status END,
		     updated_at = NOW()
		 WHERE id = $1`,
		id, threshold,
	)
	return err
}

func (r *Repo) ResetFailures(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE channels
		 SET consecutive_failures = 0, status = 'active', updated_at = NOW()
		 WHERE id = $1`, id,
	)
	return err
}

type scanner interface{ Scan(dest ...any) error }

func scan(s scanner) (*Channel, error) {
	var c Channel
	err := s.Scan(
		&c.ID, &c.TenantID, &c.UserID, &c.Type, &c.Config,
		&c.Enabled, &c.Status, &c.ConsecutiveFailures,
		&c.LastFailedAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &c, err
}

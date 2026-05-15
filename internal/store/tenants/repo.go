package tenants

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrDuplicate = errors.New("duplicate")

type Tenant struct {
	ID        string    `json:"id"`
	APIKey    string    `json:"api_key"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Insert(ctx context.Context, t Tenant) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO tenants (id, api_key, name) VALUES ($1, $2, $3)`,
		t.ID, t.APIKey, t.Name,
	)
	return pgErr(err)
}

func (r *Repo) GetByAPIKey(ctx context.Context, apiKey string) (*Tenant, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, api_key, name, created_at FROM tenants WHERE api_key = $1`,
		apiKey,
	)
	return scan(row)
}

func (r *Repo) List(ctx context.Context) ([]Tenant, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, api_key, name, created_at FROM tenants ORDER BY id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Tenant
	for rows.Next() {
		t, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

func (r *Repo) RotateAPIKey(ctx context.Context, id, newKey string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE tenants SET api_key = $1 WHERE id = $2`,
		newKey, id,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scan(s scanner) (*Tenant, error) {
	var t Tenant
	err := s.Scan(&t.ID, &t.APIKey, &t.Name, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &t, err
}

func pgErr(err error) error {
	if err == nil {
		return nil
	}
	var e *pgconn.PgError
	if errors.As(err, &e) && e.Code == "23505" {
		return fmt.Errorf("%w: %s", ErrDuplicate, e.Detail)
	}
	return err
}

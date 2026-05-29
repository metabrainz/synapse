package users

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Upsert(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`,
		id,
	)
	return err
}

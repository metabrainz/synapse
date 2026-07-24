package users

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type User struct {
	ID        string    `json:"id"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Upsert(ctx context.Context, id, username string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO users (id, username) VALUES ($1, $2)
		 ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username
		 WHERE users.username IS DISTINCT FROM EXCLUDED.username`,
		id, username,
	)
	return err
}

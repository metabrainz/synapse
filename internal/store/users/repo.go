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

// TODO: implement Delete(ctx, id) — called when MetaBrainz notifies Synapse of account deletion.
// Should cascade-delete user_channels, user_tenant_channel_mapping, and user_event_subscriptions
// via ON DELETE CASCADE (already set in migrations). A DELETE /v1/users/{id} admin endpoint
// should call this.

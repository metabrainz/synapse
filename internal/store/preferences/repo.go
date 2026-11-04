package preferences

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Preferences struct {
	CrossTenantNotifications bool `json:"cross_tenant_notifications"`
}

// Update changes only the fields that are set.
type Update struct {
	CrossTenantNotifications *bool
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// Get returns the defaults for a user with no row.
func (r *Repo) Get(ctx context.Context, userID string) (Preferences, error) {
	var p Preferences
	err := r.pool.QueryRow(ctx,
		`SELECT cross_tenant_notifications FROM user_preferences WHERE user_id = $1`, userID,
	).Scan(&p.CrossTenantNotifications)
	if errors.Is(err, pgx.ErrNoRows) {
		return Preferences{}, nil
	}
	return p, err
}

// Update creates the row on the first change and returns the resulting preferences.
func (r *Repo) Update(ctx context.Context, userID string, u Update) (Preferences, error) {
	if u.CrossTenantNotifications == nil {
		return r.Get(ctx, userID)
	}
	var p Preferences
	err := r.pool.QueryRow(ctx,
		`INSERT INTO user_preferences (user_id, cross_tenant_notifications)
		 VALUES ($1, $2)
		 ON CONFLICT (user_id) DO UPDATE SET cross_tenant_notifications = EXCLUDED.cross_tenant_notifications
		 RETURNING cross_tenant_notifications`,
		userID, *u.CrossTenantNotifications,
	).Scan(&p.CrossTenantNotifications)
	return p, err
}

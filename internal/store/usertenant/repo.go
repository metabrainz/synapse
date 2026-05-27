package usertenant

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Mapping struct {
	UserID        string `json:"user_id"`
	TenantID      string `json:"tenant_id"`
	ChannelType   string `json:"channel_type"`
	UserChannelID int64  `json:"user_channel_id"`
	IsEnabled     bool   `json:"is_enabled"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

// Upsert sets (or replaces) which user_channel this user uses for a given tenant + channel type.
// The PK (user_id, tenant_id, channel_type) enforces the radio-button constraint.
func (r *Repo) Upsert(ctx context.Context, m Mapping) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO user_tenant_channel_mapping
		     (user_id, tenant_id, channel_type, user_channel_id, is_enabled)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (user_id, tenant_id, channel_type)
		 DO UPDATE SET user_channel_id = EXCLUDED.user_channel_id,
		               is_enabled       = EXCLUDED.is_enabled`,
		m.UserID, m.TenantID, m.ChannelType, m.UserChannelID, m.IsEnabled,
	)
	return err
}

func (r *Repo) SetEnabled(ctx context.Context, userID, tenantID, channelType string, enabled bool) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE user_tenant_channel_mapping
		 SET is_enabled = $4
		 WHERE user_id = $1 AND tenant_id = $2 AND channel_type = $3`,
		userID, tenantID, channelType, enabled,
	)
	return err
}

func (r *Repo) Delete(ctx context.Context, userID, tenantID, channelType string) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM user_tenant_channel_mapping
		 WHERE user_id = $1 AND tenant_id = $2 AND channel_type = $3`,
		userID, tenantID, channelType,
	)
	return err
}

func (r *Repo) ListByUser(ctx context.Context, userID, tenantID string) ([]Mapping, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT user_id, tenant_id, channel_type, user_channel_id, is_enabled
		 FROM user_tenant_channel_mapping
		 WHERE user_id = $1 AND tenant_id = $2`,
		userID, tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mapping
	for rows.Next() {
		var mapping Mapping
		if err := rows.Scan(&mapping.UserID, &mapping.TenantID, &mapping.ChannelType,
			&mapping.UserChannelID, &mapping.IsEnabled); err != nil {
			return nil, err
		}
		out = append(out, mapping)
	}
	return out, rows.Err()
}

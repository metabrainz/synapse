package tenantrules

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Rule struct {
	TenantID    string `json:"tenant_id"`
	EventType   string `json:"event_type"`
	ChannelType string `json:"channel_type"`
	IsAllowed   bool   `json:"is_allowed"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Upsert(ctx context.Context, rule Rule) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO tenant_event_channel_rules
		     (tenant_id, event_type, channel_type, is_allowed)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (tenant_id, event_type, channel_type)
		 DO UPDATE SET is_allowed = EXCLUDED.is_allowed`,
		rule.TenantID, rule.EventType, rule.ChannelType, rule.IsAllowed,
	)
	return err
}

func (r *Repo) Delete(ctx context.Context, tenantID, eventType, channelType string) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM tenant_event_channel_rules
		 WHERE tenant_id = $1 AND event_type = $2 AND channel_type = $3`,
		tenantID, eventType, channelType,
	)
	return err
}

func (r *Repo) ListByTenant(ctx context.Context, tenantID string) ([]Rule, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT tenant_id, event_type, channel_type, is_allowed
		 FROM tenant_event_channel_rules WHERE tenant_id = $1 ORDER BY event_type, channel_type`,
		tenantID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Rule
	for rows.Next() {
		var rule Rule
		if err := rows.Scan(&rule.TenantID, &rule.EventType,
			&rule.ChannelType, &rule.IsAllowed); err != nil {
			return nil, err
		}
		out = append(out, rule)
	}
	return out, rows.Err()
}

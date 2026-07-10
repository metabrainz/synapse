package userchannels

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("user channel not found")

type UserChannel struct {
	ID          int64           `json:"id"`
	UserID      string          `json:"user_id"`
	ChannelType string          `json:"channel_type"`
	Label       string          `json:"label"`
	Config      json.RawMessage `json:"config"`
	IsActive    bool            `json:"is_active"`
	CreatedAt   time.Time       `json:"created_at"`
}

type Repo struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Insert(ctx context.Context, channel UserChannel) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`INSERT INTO user_channels (user_id, channel_type, label, config)
		 VALUES ($1, $2, $3, $4) RETURNING id`,
		channel.UserID, channel.ChannelType, channel.Label, channel.Config,
	).Scan(&id)
	return id, err
}

func (r *Repo) ListByUser(ctx context.Context, userID string) ([]UserChannel, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, user_id, channel_type, label, config, is_active, created_at
		 FROM user_channels WHERE user_id = $1 ORDER BY created_at`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UserChannel
	for rows.Next() {
		var channel UserChannel
		if err := rows.Scan(&channel.ID, &channel.UserID, &channel.ChannelType, &channel.Label,
			&channel.Config, &channel.IsActive, &channel.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, channel)
	}
	return out, rows.Err()
}

func (r *Repo) UpdateConfig(ctx context.Context, userID string, id int64, config json.RawMessage) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE user_channels SET config = $1 WHERE id = $2 AND user_id = $3`,
		config, id, userID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repo) Delete(ctx context.Context, userID string, id int64) error {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM user_channels WHERE id = $1 AND user_id = $2`,
		id, userID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repo) GetByID(ctx context.Context, id int64) (*UserChannel, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, user_id, channel_type, label, config, is_active, created_at
		 FROM user_channels WHERE id = $1`, id,
	)
	var channel UserChannel
	err := row.Scan(&channel.ID, &channel.UserID, &channel.ChannelType, &channel.Label,
		&channel.Config, &channel.IsActive, &channel.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &channel, err
}

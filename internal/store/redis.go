package store

import (
	"context"
	"fmt"

	"github.com/metabrainz/synapse/internal/config"
	"github.com/redis/go-redis/v9"
)

func NewRedis(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return rdb, nil
}

package store

import (
	"context"
	"fmt"

	"github.com/metabrainz/synapse/internal/config"
	"github.com/redis/go-redis/v9"
)

// Redis is the shared client. It carries the environment's Namespace, set once
// from config, so every component that gets the client names its keys and
// channels with rdb.Name and none has to be told the namespace separately.
type Redis struct {
	*redis.Client
	Namespace
}

// NewRedisClient builds the client without connecting.
func NewRedisClient(cfg config.RedisConfig) *Redis {
	return WrapRedis(redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}), Namespace(cfg.Namespace))
}

// WrapRedis puts a namespace on an existing client, e.g. one derived with
// different timeouts or one made for a test.
func WrapRedis(c *redis.Client, ns Namespace) *Redis {
	return &Redis{Client: c, Namespace: ns}
}

func NewRedis(ctx context.Context, cfg config.RedisConfig) (*Redis, error) {
	rdb := NewRedisClient(cfg)

	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return rdb, nil
}

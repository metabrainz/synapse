package telegram

// Telegram Bot API rate limits (as of 2026):
//   - 1 message per second per individual chat (user, group, or channel).
//   - 30 messages per second across all chats combined (global bot limit).
//
// Exceeding either limit returns HTTP 429 with a `parameters.retry_after`
// field in the JSON body indicating the minimum seconds to wait.
//
// Reference: https://core.telegram.org/bots/faq#my-bot-is-hitting-limits-how-do-i-avoid-this
//
// These limiters are Redis token buckets shared across all worker instances.
// Both fail-open on Redis errors: a delivery attempt proceeds rather than
// being silently dropped when the limiter cannot be consulted.

import (
	"context"
	"encoding/json"
	"time"

	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

const (
	perChatRate = 1
	globalRate  = 30
)

type rateLimiter struct {
	perChat *ratelimit.Limiter // burst=1,  rate=1/s  — key: synapse:rl:tg:chat:{chatID}
	global  *ratelimit.Limiter // burst=30, rate=30/s — key: synapse:rl:tg:global
}

func newRateLimiter(rdb *redis.Client) *rateLimiter {
	return &rateLimiter{
		perChat: ratelimit.New(rdb, perChatRate, perChatRate),
		global:  ratelimit.New(rdb, globalRate, globalRate),
	}
}

// RateLimit implements adapter.RateLimiter.
// It checks the per-chat bucket first (cheaper rejection), then the global bucket.
// Returns (false, 1s) when either limit is exceeded. Fails open on Redis errors.
func (r *rateLimiter) RateLimit(ctx context.Context, msg fanout.WorkerMessage) (bool, time.Duration) {
	var cfg channelConfig
	// If we can't parse the chat ID we can't check the per-chat limit;
	// skip that check and fall through to the global limit only.
	_ = json.Unmarshal(msg.ChannelConfig, &cfg)

	if cfg.ChatID != "" {
		ok, err := r.perChat.AllowKey(ctx, "synapse:rl:tg:chat:"+cfg.ChatID)
		if err == nil && !ok {
			return false, time.Second
		}
	}

	ok, err := r.global.AllowKey(ctx, "synapse:rl:tg:global")
	if err == nil && !ok {
		return false, time.Second
	}

	return true, 0
}

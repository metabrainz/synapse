// Package ratelimit implements a per-tenant token bucket rate limiter backed by
// Redis. The bucket state (token count + last-refill timestamp) is stored in a
// Redis hash and updated atomically by a Lua script, ensuring correctness even
// under concurrent API requests hitting different API server instances.
package ratelimit

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript implements a token bucket in Redis.
// KEYS[1] = bucket key (synapse:rl:{tenant_id})
// ARGV[1] = burst capacity (max tokens)
// ARGV[2] = refill rate (tokens per second)
// ARGV[3] = now (unix nanoseconds as string)
// Returns 1 if allowed, 0 if rate limited.
var tokenBucketScript = redis.NewScript(`
local key    = KEYS[1]
local burst  = tonumber(ARGV[1])
local rate   = tonumber(ARGV[2])
local now_ns = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local ts_ns  = tonumber(data[2])

if tokens == nil then
  tokens = burst
  ts_ns  = now_ns
end

-- Refill based on elapsed time
local elapsed_s = (now_ns - ts_ns) / 1e9
tokens = math.min(burst, tokens + elapsed_s * rate)
ts_ns  = now_ns

if tokens < 1 then
  redis.call("HMSET", key, "tokens", tokens, "ts", ts_ns)
  redis.call("EXPIRE", key, 3600)
  return 0
end

tokens = tokens - 1
redis.call("HMSET", key, "tokens", tokens, "ts", ts_ns)
redis.call("EXPIRE", key, 3600)
return 1
`)

type Limiter struct {
	rdb   *redis.Client
	burst int
	rate  int // tokens per second
}

// New creates a token bucket limiter. burst is max concurrent tokens; ratePerSec is sustained fill rate.
func New(rdb *redis.Client, burst, ratePerSec int) *Limiter {
	return &Limiter{rdb: rdb, burst: burst, rate: ratePerSec}
}

// AllowKey runs the token bucket against an arbitrary Redis key.
// Use this when the caller manages its own key namespace (e.g. adapter-level limiters).
func (l *Limiter) AllowKey(ctx context.Context, key string) (bool, error) {
	nowNs := time.Now().UnixNano()
	result, err := tokenBucketScript.Run(ctx, l.rdb, []string{key},
		l.burst, l.rate, nowNs,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (l *Limiter) Allow(ctx context.Context, tenantID string) (bool, error) {
	return l.AllowKey(ctx, fmt.Sprintf("synapse:rl:%s", tenantID))
}

// Middleware extracts the tenant ID from the request context (set by auth middleware)
// and enforces the token bucket. On rate limit, responds 429. On Redis error, fails open.
func (l *Limiter) Middleware(tenantIDFromCtx func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantID := tenantIDFromCtx(r)
			if tenantID == "" {
				next.ServeHTTP(w, r)
				return
			}
			allowed, err := l.Allow(r.Context(), tenantID)
			if err != nil {
				// Fail open: Redis unavailability shouldn't block legitimate traffic.
				next.ServeHTTP(w, r)
				return
			}
			if !allowed {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate limit exceeded"}`))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

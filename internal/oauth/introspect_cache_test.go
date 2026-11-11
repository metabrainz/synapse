//go:build integration

package oauth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/testutil"
)

func TestMBIntrospector_CacheCappedAt15Minutes(t *testing.T) {
	ctx := context.Background()
	addr, cleanup, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(cleanup)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"active":     true,
			"sub":        "42",
			"username":   "testuser",
			"expires_at": time.Now().Add(time.Hour).Unix(),
		})
	}))
	defer ts.Close()

	i := oauth.NewMBIntrospector("client-id", "client-secret", ts.URL, rdb)
	if _, err := i.Introspect(ctx, "valid-token"); err != nil {
		t.Fatalf("introspect: %v", err)
	}
	keys, err := rdb.Keys(ctx, "oauth:token:*").Result()
	if err != nil || len(keys) != 1 {
		t.Fatalf("want one cached token, got %v (%v)", keys, err)
	}
	ttl, err := rdb.TTL(ctx, keys[0]).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl <= 0 || ttl > 15*time.Minute {
		t.Fatalf("a token valid for 1h must be cached at most 15m, got %s", ttl)
	}
}

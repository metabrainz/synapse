//go:build integration

package ratelimit_test

import (
	"context"
	"testing"

	"github.com/metabrainz/synapse/internal/ratelimit"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/testutil"
	"github.com/redis/go-redis/v9"
)

func TestBucketsStayInsideTheirNamespace(t *testing.T) {
	ctx := context.Background()
	addr, cleanup, err := testutil.StartRedis(ctx)
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(cleanup)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })

	// burst 1: a second request in the same bucket is refused.
	a := ratelimit.New(store.WrapRedis(rdb, "a"), 1, 1)
	b := ratelimit.New(store.WrapRedis(rdb, "b"), 1, 1)
	for name, l := range map[string]*ratelimit.Limiter{"a": a, "b": b} {
		if ok, err := l.Allow(ctx, "tenant"); err != nil || !ok {
			t.Fatalf("%s: first request = %v, %v; want allowed", name, ok, err)
		}
	}
	if ok, _ := a.Allow(ctx, "tenant"); ok {
		t.Fatal("namespace a: second request allowed, so the bucket was not used")
	}

	for _, key := range []string{"a:synapse:rl:tenant", "b:synapse:rl:tenant"} {
		if n, _ := rdb.Exists(ctx, key).Result(); n != 1 {
			t.Errorf("key %s not found", key)
		}
	}
	if n, _ := rdb.Exists(ctx, "synapse:rl:tenant").Result(); n != 0 {
		t.Error("un-namespaced key was written")
	}
}

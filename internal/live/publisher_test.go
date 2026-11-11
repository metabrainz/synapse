//go:build integration

package live_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/metabrainz/synapse/internal/live"
	"github.com/metabrainz/synapse/testutil"
)

func startRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr, cleanup, err := testutil.StartRedis(context.Background())
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(cleanup)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func deadRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func subscribe(t *testing.T, rdb *redis.Client, user string) <-chan *redis.Message {
	t.Helper()
	sub := rdb.Subscribe(context.Background(), live.Channel(user))
	t.Cleanup(func() { sub.Close() })
	if _, err := sub.Receive(context.Background()); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	return sub.Channel()
}

func expect(t *testing.T, ch <-chan *redis.Message, want string) {
	t.Helper()
	select {
	case m := <-ch:
		if m.Payload != want {
			t.Fatalf("payload = %s, want %s", m.Payload, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("no message, want %s", want)
	}
}

func TestPublishDelivers(t *testing.T) {
	rdb := startRedis(t)
	ch := subscribe(t, rdb, "alice")

	live.NewPublisher(rdb).Publish(context.Background(), "alice", live.Nudge{Kind: live.KindCreated, Tenant: "listenbrainz"})

	expect(t, ch, `{"kind":"created","tenant":"listenbrainz"}`)
}

func TestPublishSurvivesCanceledContext(t *testing.T) {
	rdb := startRedis(t)
	ch := subscribe(t, rdb, "alice")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	live.NewPublisher(rdb).Publish(ctx, "alice", live.Nudge{Kind: live.KindChanged, Tenant: "t"})

	expect(t, ch, `{"kind":"changed","tenant":"t"}`)
}

func TestPublishDeadRedis(t *testing.T) {
	n := live.Nudge{Kind: live.KindCreated, Tenant: "t"}
	p := live.NewPublisher(deadRedis(t))
	start := time.Now()
	p.Publish(context.Background(), "alice", n)
	p.PublishMany(context.Background(), []live.Target{{UserID: "alice", Nudge: n}})
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("dead redis took %s", d)
	}
}

func TestPublishUnresponsiveRedis(t *testing.T) {
	rdb := startRedis(t)
	p := live.NewPublisher(rdb)
	if err := rdb.Do(context.Background(), "CLIENT", "PAUSE", 3000, "ALL").Err(); err != nil {
		t.Fatalf("client pause: %v", err)
	}
	start := time.Now()
	p.Publish(context.Background(), "alice", live.Nudge{Kind: live.KindCreated, Tenant: "t"})
	if d := time.Since(start); d > time.Second {
		t.Fatalf("unresponsive redis took %s", d)
	}
}

func TestPublishManyStopsAtFirstFailedChunk(t *testing.T) {
	rdb := startRedis(t)
	ts := make([]live.Target, 5000)
	for i := range ts {
		ts[i] = live.Target{UserID: fmt.Sprint(i), Nudge: live.Nudge{Kind: live.KindCreated, Tenant: "t"}}
	}
	if err := rdb.Do(context.Background(), "CLIENT", "PAUSE", 3000, "ALL").Err(); err != nil {
		t.Fatalf("client pause: %v", err)
	}
	start := time.Now()
	live.NewPublisher(rdb).PublishMany(context.Background(), ts)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("unresponsive redis took %s", d)
	}
}

func TestPublishManyPipelines(t *testing.T) {
	rdb := startRedis(t)
	p := live.NewPublisher(rdb)
	p.PublishMany(context.Background(), nil)

	const total = 10000
	picked := []int{0, 4999, 9999}
	chans := map[int]<-chan *redis.Message{}
	for _, i := range picked {
		chans[i] = subscribe(t, rdb, fmt.Sprintf("u%d", i))
	}

	targets := make([]live.Target, total)
	for i := range targets {
		targets[i] = live.Target{UserID: fmt.Sprintf("u%d", i), Nudge: live.Nudge{Kind: live.KindCreated, Tenant: fmt.Sprintf("t%d", i)}}
	}
	start := time.Now()
	p.PublishMany(context.Background(), targets)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("PublishMany took %s", d)
	}

	for _, i := range picked {
		expect(t, chans[i], fmt.Sprintf(`{"kind":"created","tenant":"t%d"}`, i))
		select {
		case m := <-chans[i]:
			t.Fatalf("u%d got extra message %s", i, m.Payload)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

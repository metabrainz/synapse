//go:build integration

package live_test

import (
	"context"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/live"
	"github.com/metabrainz/synapse/internal/store"
)

func TestPublishGoesToTheNamespacedChannel(t *testing.T) {
	rdb := startRedis(t)
	plain := subscribe(t, rdb, "alice") // the un-namespaced channel

	sub := rdb.Subscribe(context.Background(), live.Channel("test", "alice"))
	t.Cleanup(func() { sub.Close() })
	if _, err := sub.Receive(context.Background()); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	live.NewPublisher(store.WrapRedis(rdb.Client, "test")).Publish(context.Background(), "alice", live.Nudge{Kind: live.KindChanged, Tenant: "t"})

	expect(t, sub.Channel(), `{"kind":"changed","tenant":"t"}`)
	select {
	case m := <-plain:
		t.Fatalf("un-namespaced subscriber got %s", m.Payload)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestHubOnlyHearsItsOwnNamespace(t *testing.T) {
	rdb := startRedis(t)
	hub := live.NewHub(store.WrapRedis(rdb.Client, "a"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hub.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	nudges, unregister := register(t, hub, "alice")
	defer unregister()

	nudge := live.Nudge{Kind: live.KindCreated, Tenant: "t"}
	for _, ns := range []store.Namespace{"b", ""} {
		live.NewPublisher(store.WrapRedis(rdb.Client, ns)).Publish(context.Background(), "alice", nudge)
	}
	select {
	case n := <-nudges:
		t.Fatalf("got a nudge from another namespace: %+v", n)
	case <-time.After(300 * time.Millisecond):
	}

	live.NewPublisher(store.WrapRedis(rdb.Client, "a")).Publish(context.Background(), "alice", nudge)
	select {
	case n := <-nudges:
		if n != nudge {
			t.Fatalf("nudge = %+v, want %+v", n, nudge)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no nudge from the hub's own namespace")
	}
}

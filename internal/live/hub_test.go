//go:build integration

package live_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/metabrainz/synapse/internal/live"
)

func startHub(t *testing.T) (*redis.Client, *live.Hub, *live.Publisher) {
	t.Helper()
	rdb := startRedis(t)
	hub := live.NewHub(rdb)
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
	return rdb, hub, live.NewPublisher(rdb)
}

func register(t *testing.T, hub *live.Hub, user string) (<-chan live.Nudge, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, unregister, err := hub.Register(ctx, user)
	if err != nil {
		t.Fatalf("register %s: %v", user, err)
	}
	return ch, unregister
}

func numSub(t *testing.T, rdb *redis.Client, user string) int64 {
	t.Helper()
	res, err := rdb.PubSubNumSub(context.Background(), live.Channel(user)).Result()
	if err != nil {
		t.Fatalf("numsub: %v", err)
	}
	return res[live.Channel(user)]
}

func waitNumSub(t *testing.T, rdb *redis.Client, user string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := numSub(t, rdb, user)
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("numsub %s = %d, want %d", user, got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func recv(t *testing.T, ch <-chan live.Nudge, want live.Nudge) {
	t.Helper()
	select {
	case n, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed, want %+v", want)
		}
		if n != want {
			t.Fatalf("got %+v, want %+v", n, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("no nudge, want %+v", want)
	}
}

func waitClosed(t *testing.T, ch <-chan live.Nudge, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel not closed")
		}
	}
}

func TestHubFanOut(t *testing.T) {
	_, hub, pub := startHub(t)
	a1, _ := register(t, hub, "alice")
	a2, _ := register(t, hub, "alice")
	b, _ := register(t, hub, "bob")

	n := live.Nudge{Kind: live.KindCreated, Tenant: "t"}
	pub.Publish(context.Background(), "alice", n)

	recv(t, a1, n)
	recv(t, a2, n)
	select {
	case got := <-b:
		t.Fatalf("bob got %+v", got)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHubUnsubscribesOnLastOnly(t *testing.T) {
	rdb, hub, pub := startHub(t)
	_, unreg1 := register(t, hub, "alice")
	a2, unreg2 := register(t, hub, "alice")
	waitNumSub(t, rdb, "alice", 1)

	unreg1()
	time.Sleep(100 * time.Millisecond)
	if got := numSub(t, rdb, "alice"); got != 1 {
		t.Fatalf("numsub after first unregister = %d, want 1", got)
	}
	n := live.Nudge{Kind: live.KindChanged, Tenant: "t"}
	pub.Publish(context.Background(), "alice", n)
	recv(t, a2, n)

	unreg2()
	waitNumSub(t, rdb, "alice", 0)
}

func TestHubFullBufferDoesNotBlock(t *testing.T) {
	_, hub, pub := startHub(t)
	register(t, hub, "alice")
	reader, _ := register(t, hub, "alice")

	last := live.Nudge{Kind: live.KindCreated, Tenant: "t19"}
	got := make(chan struct{})
	go func() {
		for n := range reader {
			if n == last {
				close(got)
				return
			}
		}
	}()
	for i := range 20 {
		pub.Publish(context.Background(), "alice", live.Nudge{Kind: live.KindCreated, Tenant: fmt.Sprintf("t%d", i)})
	}
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("second stream did not receive the 20th nudge")
	}
}

func TestHubRecoversAfterConnectionKill(t *testing.T) {
	rdb, hub, pub := startHub(t)
	a, _ := register(t, hub, "alice")

	if err := rdb.ClientKillByFilter(context.Background(), "TYPE", "pubsub").Err(); err != nil {
		t.Fatalf("client kill: %v", err)
	}
	waitClosed(t, a, 2*time.Second)

	n := live.Nudge{Kind: live.KindCreated, Tenant: "t"}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		ch, unregister, err := hub.Register(ctx, "alice")
		cancel()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		pub.Publish(context.Background(), "alice", n)
		select {
		case got, ok := <-ch:
			if ok && got == n {
				unregister()
				return
			}
		case <-time.After(300 * time.Millisecond):
		}
		unregister()
	}
	t.Fatal("hub did not recover after connection kill")
}

func TestHubCloseClosesAndRejects(t *testing.T) {
	_, hub, _ := startHub(t)
	a, _ := register(t, hub, "alice")

	hub.Close()

	waitClosed(t, a, time.Second)
	if _, _, err := hub.Register(context.Background(), "alice"); !errors.Is(err, live.ErrHubClosed) {
		t.Fatalf("register after close err = %v, want ErrHubClosed", err)
	}
}

func TestHubUnregisterAfterClose(t *testing.T) {
	_, hub, _ := startHub(t)
	_, unregister := register(t, hub, "alice")

	hub.Close()
	unregister()
	unregister()
}

func TestHubRegisterReturnsAfterSubscribeConfirmed(t *testing.T) {
	rdb, hub, pub := startHub(t)
	c, _ := register(t, hub, "carol")

	if got := numSub(t, rdb, "carol"); got != 1 {
		t.Fatalf("numsub right after register = %d, want 1", got)
	}
	n := live.Nudge{Kind: live.KindCreated, Tenant: "t"}
	pub.Publish(context.Background(), "carol", n)
	recv(t, c, n)
}

func TestHubRegisterWaitsForConfirmation(t *testing.T) {
	hub := live.NewHub(startRedis(t)) // not running, so no confirmation is ever read
	t.Cleanup(hub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, _, err := hub.Register(ctx, "carol"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("register err = %v, want DeadlineExceeded", err)
	}
}

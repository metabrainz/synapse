//go:build integration

package live_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/metabrainz/synapse/internal/live"
)

func TestTicketSingleUse(t *testing.T) {
	ctx := context.Background()
	tickets := live.NewTickets(startRedis(t))

	tk, err := tickets.Mint(ctx, "alice")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if got, err := tickets.Redeem(ctx, tk); err != nil || got != "alice" {
		t.Fatalf("first redeem = %q, %v", got, err)
	}
	if _, err := tickets.Redeem(ctx, tk); !errors.Is(err, live.ErrInvalidTicket) {
		t.Fatalf("second redeem err = %v, want ErrInvalidTicket", err)
	}
}

func TestTicketTTL(t *testing.T) {
	ctx := context.Background()
	rdb := startRedis(t)

	tk, err := live.NewTickets(rdb).Mint(ctx, "alice")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	ttl, err := rdb.TTL(ctx, "synapse:stream-ticket:"+tk).Result()
	if err != nil {
		t.Fatalf("ttl: %v", err)
	}
	if ttl < 29*time.Second || ttl > 30*time.Second {
		t.Fatalf("ttl = %s, want 29s..30s", ttl)
	}
}

func TestTicketInvalidInputs(t *testing.T) {
	tickets := live.NewTickets(startRedis(t))
	for _, tk := range []string{"", "nope", strings.Repeat("x", 10000)} {
		if _, err := tickets.Redeem(context.Background(), tk); !errors.Is(err, live.ErrInvalidTicket) {
			t.Errorf("Redeem(len %d) err = %v, want ErrInvalidTicket", len(tk), err)
		}
	}
}

func TestTicketsUnique(t *testing.T) {
	tickets := live.NewTickets(startRedis(t))
	seen := map[string]bool{}
	for range 1000 {
		tk, err := tickets.Mint(context.Background(), "alice")
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		if len(tk) < 26 {
			t.Fatalf("ticket %q too short", tk)
		}
		if seen[tk] {
			t.Fatalf("duplicate ticket %q", tk)
		}
		seen[tk] = true
	}
}

func TestRedeemRedisDown(t *testing.T) {
	_, err := live.NewTickets(deadRedis(t)).Redeem(context.Background(), "abc")
	if err == nil || errors.Is(err, live.ErrInvalidTicket) {
		t.Fatalf("err = %v, want non-nil and not ErrInvalidTicket", err)
	}
}

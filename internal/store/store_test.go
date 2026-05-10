//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/channels"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/events"
	"github.com/metabrainz/synapse/internal/store/outbox"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
	"github.com/metabrainz/synapse/internal/store/tenants"
	"github.com/metabrainz/synapse/testutil"
)

// TestStoreEndToEnd walks the full critical path:
// tenant → channel → subscription → event + delivery + outbox in one transaction.
func TestStoreEndToEnd(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewTestPool(ctx, t)

	tenantRepo := tenants.New(pool)
	channelRepo := channels.New(pool)
	subRepo := subscriptions.New(pool)

	// 1. Create tenant
	err := tenantRepo.Insert(ctx, tenants.Tenant{
		ID:     "listenbrainz",
		APIKey: "test-api-key",
		Name:   "ListenBrainz",
	})
	if err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	// 2. Seed event type definition (required by FK on events)
	_, err = pool.Exec(ctx,
		`INSERT INTO event_type_definitions (tenant_id, event_type, description)
		 VALUES ('listenbrainz', 'listen', 'User submitted a listen')`,
	)
	if err != nil {
		t.Fatalf("insert event type: %v", err)
	}

	// 3. Create a webhook channel for user
	chConfig, _ := json.Marshal(map[string]string{"url": "https://example.com/hook"})
	var channelID int64
	err = store.WithTx(ctx, pool, func(q store.Querier) error {
		channelID, err = channelRepo.Insert(ctx, q, channels.Channel{
			TenantID: "listenbrainz",
			UserID:   "user-1",
			Type:     "webhook",
			Config:   chConfig,
		})
		return err
	})
	if err != nil {
		t.Fatalf("insert channel: %v", err)
	}

	// 4. Subscribe the channel to the listen event type
	_, err = subRepo.Insert(ctx, subscriptions.Subscription{
		ChannelID: channelID,
		EventType: "listen",
		Config:    json.RawMessage(`{"delivery_mode":"immediate"}`),
	})
	if err != nil {
		t.Fatalf("insert subscription: %v", err)
	}

	// 5. Verify subscription lookup works (this is what fanout calls)
	active, err := subRepo.ListActiveForEvent(ctx, "listenbrainz", "user-1", "listen")
	if err != nil {
		t.Fatalf("list active: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active channel, got %d", len(active))
	}

	// 6. Fan-out: write event + delivery + outbox in one transaction
	payload, _ := json.Marshal(map[string]any{"track": "Pyramid Song", "artist": "Radiohead"})
	var eventID, deliveryID int64

	err = store.WithTx(ctx, pool, func(q store.Querier) error {
		eventID, err = events.Insert(ctx, q, events.Event{
			TenantID:  "listenbrainz",
			UserID:    "user-1",
			EventType: "listen",
			Payload:   payload,
		})
		if err != nil {
			return err
		}

		deliveryID, err = deliveries.Insert(ctx, q, deliveries.Delivery{
			EventID:     eventID,
			ChannelID:   channelID,
			ChannelType: "webhook",
			MaxAttempts: 5,
		})
		if err != nil {
			return err
		}

		msg, _ := json.Marshal(map[string]any{"delivery_id": deliveryID, "event_id": eventID})
		return outbox.Insert(ctx, q, "webhook", msg)
	})
	if err != nil {
		t.Fatalf("fan-out transaction: %v", err)
	}

	// 7. Verify outbox has the message waiting for the relay
	msgs, err := outbox.FetchPending(ctx, pool, 10)
	if err != nil {
		t.Fatalf("fetch outbox: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 outbox message, got %d", len(msgs))
	}
	if msgs[0].RoutingKey != "webhook" {
		t.Errorf("expected routing key webhook, got %s", msgs[0].RoutingKey)
	}

	// 8. Simulate relay: delete after publish
	if err := outbox.DeleteBatch(ctx, pool, []int64{msgs[0].ID}); err != nil {
		t.Fatalf("delete outbox: %v", err)
	}

	// 9. Verify delivery status update works
	if err := deliveries.UpdateStatus(ctx, pool, deliveryID, "delivered", 1, nil); err != nil {
		t.Fatalf("update delivery status: %v", err)
	}

	d, err := deliveries.GetByID(ctx, pool, deliveryID)
	if err != nil || d == nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.Status != "delivered" {
		t.Errorf("expected status delivered, got %s", d.Status)
	}
}

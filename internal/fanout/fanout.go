package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/events"
	"github.com/metabrainz/synapse/internal/store/outbox"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
)

// Lookup resolves active channels for a given event. Satisfied by both
// *subscriptions.Repo (direct DB) and *Cache (in-memory, preferred in production).
type Lookup interface {
	ListActiveForEvent(ctx context.Context, tenantID, userID, eventType string) ([]subscriptions.ActiveChannel, error)
}

// WorkerMessage is the payload written to the outbox and published to RabbitMQ.
// Fields are snapshotted at fan-out time — if a user updates their webhook URL
// after an event is queued, in-flight messages use the old config. Intentional.
type WorkerMessage struct {
	DeliveryID    int64           `json:"delivery_id"`
	ChannelID     int64           `json:"channel_id"`
	ChannelType   string          `json:"channel_type"`
	ChannelConfig json.RawMessage `json:"channel_config"`
	SubConfig     json.RawMessage `json:"sub_config"`
	EventID       int64           `json:"event_id"`
	EventType     string          `json:"event_type"`
	TenantID      string          `json:"tenant_id"`
	UserID        string          `json:"user_id"`
	Payload       json.RawMessage `json:"payload"`
	Attempt       int             `json:"attempt"`
	MaxAttempts   int             `json:"max_attempts"`
	CreatedAt     time.Time       `json:"created_at"`
}

var maxAttemptsByType = map[string]int{
	"webhook": 5,
	"email":   2,
}

func maxAttempts(channelType string) int {
	if n, ok := maxAttemptsByType[channelType]; ok {
		return n
	}
	return 3
}

type Fanout struct {
	subs Lookup
}

func New(subs Lookup) *Fanout {
	return &Fanout{subs: subs}
}

// Preview returns the number of channels that would receive this event
// without writing anything — used by the dry_run ingest path.
func (f *Fanout) Preview(ctx context.Context, tenantID, userID, eventType string) (int, error) {
	channels, err := f.subs.ListActiveForEvent(ctx, tenantID, userID, eventType)
	if err != nil {
		return 0, fmt.Errorf("list subscriptions: %w", err)
	}
	return len(channels), nil
}

// Fan creates one delivery + one outbox row per active subscription matching
// the event. Returns the number of deliveries created.
// Must be called inside a store.WithTx callback — all writes are atomic with the event insert.
func (f *Fanout) Fan(ctx context.Context, q store.Querier, ev events.Event) (int, error) {
	channels, err := f.subs.ListActiveForEvent(ctx, ev.TenantID, ev.UserID, ev.EventType)
	if err != nil {
		return 0, fmt.Errorf("list subscriptions: %w", err)
	}

	n := len(channels)
	if n == 0 {
		return 0, nil
	}

	// Phase 1 — batch insert all deliveries, get IDs back in one round-trip.
	ds := make([]deliveries.Delivery, n)
	for i, ch := range channels {
		ds[i] = deliveries.Delivery{
			EventID:     ev.ID,
			ChannelID:   ch.ChannelID,
			ChannelType: ch.ChannelType,
			MaxAttempts: maxAttempts(ch.ChannelType),
		}
	}
	ids, err := deliveries.InsertBatch(ctx, q, ds)
	if err != nil {
		return 0, fmt.Errorf("batch insert deliveries: %w", err)
	}

	// Phase 2 — marshal worker messages (no DB), then batch insert outbox.
	routingKeys := make([]string, n)
	payloads := make([]json.RawMessage, n)
	for i, ch := range channels {
		routingKeys[i] = ch.ChannelType
		raw, err := json.Marshal(WorkerMessage{
			DeliveryID:    ids[i],
			ChannelID:     ch.ChannelID,
			ChannelType:   ch.ChannelType,
			ChannelConfig: ch.Config,
			SubConfig:     ch.SubConfig,
			EventID:       ev.ID,
			EventType:     ev.EventType,
			TenantID:      ev.TenantID,
			UserID:        ev.UserID,
			Payload:       ev.Payload,
			Attempt:       0,
			MaxAttempts:   ds[i].MaxAttempts,
			CreatedAt:     ev.CreatedAt,
		})
		if err != nil {
			return 0, fmt.Errorf("marshal worker message (channel %d): %w", ch.ChannelID, err)
		}
		payloads[i] = raw
	}
	if err := outbox.InsertBatch(ctx, q, routingKeys, payloads); err != nil {
		return 0, fmt.Errorf("batch insert outbox: %w", err)
	}

	return n, nil
}

// FanBatch fans out a slice of events in two DB round-trips total — one
// InsertBatch for all deliveries, one InsertBatch for all outbox rows —
// regardless of event count or subscription count. Must be called inside
// a store.WithTx callback.
func (f *Fanout) FanBatch(ctx context.Context, q store.Querier, evs []events.Event) (int, error) {
	type item struct {
		ev  events.Event
		ch  subscriptions.ActiveChannel
		max int
	}

	var items []item
	for _, ev := range evs {
		channels, err := f.subs.ListActiveForEvent(ctx, ev.TenantID, ev.UserID, ev.EventType)
		if err != nil {
			return 0, fmt.Errorf("list subscriptions: %w", err)
		}
		for _, ch := range channels {
			items = append(items, item{ev, ch, maxAttempts(ch.ChannelType)})
		}
	}
	if len(items) == 0 {
		return 0, nil
	}

	ds := make([]deliveries.Delivery, len(items))
	for i, it := range items {
		ds[i] = deliveries.Delivery{
			EventID:     it.ev.ID,
			ChannelID:   it.ch.ChannelID,
			ChannelType: it.ch.ChannelType,
			MaxAttempts: it.max,
		}
	}

	ids, err := deliveries.InsertBatch(ctx, q, ds)
	if err != nil {
		return 0, fmt.Errorf("batch insert deliveries: %w", err)
	}

	routingKeys := make([]string, len(items))
	payloads := make([]json.RawMessage, len(items))
	for i, it := range items {
		routingKeys[i] = it.ch.ChannelType
		raw, err := json.Marshal(WorkerMessage{
			DeliveryID:    ids[i],
			ChannelID:     it.ch.ChannelID,
			ChannelType:   it.ch.ChannelType,
			ChannelConfig: it.ch.Config,
			SubConfig:     it.ch.SubConfig,
			EventID:       it.ev.ID,
			EventType:     it.ev.EventType,
			TenantID:      it.ev.TenantID,
			UserID:        it.ev.UserID,
			Payload:       it.ev.Payload,
			Attempt:       0,
			MaxAttempts:   it.max,
			CreatedAt:     it.ev.CreatedAt,
		})
		if err != nil {
			return 0, fmt.Errorf("marshal worker message (channel %d): %w", it.ch.ChannelID, err)
		}
		payloads[i] = raw
	}

	if err := outbox.InsertBatch(ctx, q, routingKeys, payloads); err != nil {
		return 0, fmt.Errorf("batch insert outbox: %w", err)
	}

	return len(items), nil
}

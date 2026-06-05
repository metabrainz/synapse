// Package fanout resolves which delivery channels should receive an event and
// writes the resulting delivery + outbox rows atomically inside the caller's
// transaction. The two core operations are Fan (single event) and FanBatch
// (multiple events, two DB round-trips regardless of count).
package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/events"
	"github.com/metabrainz/synapse/internal/store/outbox"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
)

// Lookup resolves active channels for a set of recipients. Satisfied by both
// *subscriptions.Repo (direct DB) and *Cache (in-memory, preferred in production).
type Lookup interface {
	ListActiveForRecipients(ctx context.Context, tenantID, eventType string, recipients []string) ([]subscriptions.ActiveChannel, error)
}

// WorkerMessage is the payload written to the outbox and published to RabbitMQ.
// Fields are snapshotted at fan-out time — if a user updates their webhook URL
// after an event is queued, in-flight messages use the old config. Intentional.
type WorkerMessage struct {
	DeliveryID    int64           `json:"delivery_id"`
	ChannelID     int64           `json:"channel_id"`
	ChannelType   string          `json:"channel_type"`
	ChannelConfig json.RawMessage `json:"channel_config"`
	EventID       int64           `json:"event_id"`
	EventType     string          `json:"event_type"`
	TenantID      string          `json:"tenant_id"`
	UserID        string          `json:"user_id"`
	Payload       json.RawMessage `json:"payload"`
	Attempt       int             `json:"attempt"`
	MaxAttempts   int             `json:"max_attempts"`
	CreatedAt     time.Time       `json:"created_at"`
}

// newWorkerMessage builds the outbox payload for one delivery.
func newWorkerMessage(deliveryID int64, ev events.Event, ch subscriptions.ActiveChannel, maxAttempts int) WorkerMessage {
	return WorkerMessage{
		DeliveryID:    deliveryID,
		ChannelID:     ch.ChannelID,
		ChannelType:   ch.ChannelType,
		ChannelConfig: ch.Config,
		EventID:       ev.ID,
		EventType:     ev.EventType,
		TenantID:      ev.TenantID,
		UserID:        ch.UserID,
		Payload:       ev.Payload,
		Attempt:       0,
		MaxAttempts:   maxAttempts,
		CreatedAt:     ev.CreatedAt,
	}
}

// Fanout fans out events to all matching subscription channels.
// maxAttempts is injected rather than imported from the adapter package to
// avoid a circular dependency (adapter/webhook imports fanout for WorkerMessage).
type Fanout struct {
	subs        Lookup
	maxAttempts func(channelType string) int
	reg         *schema.Registry
}

// New creates a Fanout. Pass adapter.MaxAttemptsFor as maxAttempts in production.
func New(subs Lookup, maxAttempts func(channelType string) int, reg *schema.Registry) *Fanout {
	return &Fanout{subs: subs, maxAttempts: maxAttempts, reg: reg}
}

// Preview returns the number of channels that would receive this event
// without writing anything — used by the dry_run ingest path.
func (f *Fanout) Preview(ctx context.Context, tenantID, eventType string, recipients []string) (int, error) {
	channels, err := f.subs.ListActiveForRecipients(ctx, tenantID, eventType, recipients)
	if err != nil {
		return 0, fmt.Errorf("list subscriptions: %w", err)
	}
	channels = filterAllowed(channels, f.reg.AllowedChannels(tenantID, eventType))
	return len(channels), nil
}

// Fan creates one delivery and one outbox row per active subscription matching ev.
// Returns the number of deliveries created (0 if no subscriptions match).
func (f *Fanout) Fan(ctx context.Context, q store.Querier, ev events.Event) (int, error) {
	return f.FanBatch(ctx, q, []events.Event{ev})
}

// FanBatch fans out a slice of events in exactly two DB round-trips regardless
// of how many events or subscriptions are involved. It first resolves all
// subscriptions in memory, then writes every delivery in one InsertBatch call
// (getting all IDs back), marshals those IDs into outbox payloads, and writes
// every outbox row in a second InsertBatch call. Must be called inside a
// store.WithTx callback — all writes are atomic with the upstream event inserts.
func (f *Fanout) FanBatch(ctx context.Context, q store.Querier, evs []events.Event) (int, error) {
	// target represents one planned delivery: a single (event, subscription) pair.
	type target struct {
		ev          events.Event
		sub         subscriptions.ActiveChannel
		maxAttempts int
	}

	// Resolve all subscriptions in memory before touching the DB.
	var targets []target
	for _, ev := range evs {
		subs, err := f.subs.ListActiveForRecipients(ctx, ev.TenantID, ev.EventType, ev.Recipients)
		if err != nil {
			return 0, fmt.Errorf("list subscriptions: %w", err)
		}
		// Gate 1: filter by allowed channel types from the static registry.
		subs = filterAllowed(subs, f.reg.AllowedChannels(ev.TenantID, ev.EventType))
		for _, sub := range subs {
			targets = append(targets, target{ev, sub, f.maxAttempts(sub.ChannelType)})
		}
	}
	if len(targets) == 0 {
		return 0, nil
	}

	// Round 1: insert all delivery records in one query; returned IDs are
	// positionally aligned with targets so targets[i] ↔ deliveryIDs[i].
	ds := make([]deliveries.Delivery, len(targets))
	for i, target := range targets {
		ds[i] = deliveries.Delivery{
			EventID:     target.ev.ID,
			UserID:      target.sub.UserID,
			ChannelID:   target.sub.ChannelID,
			ChannelType: target.sub.ChannelType,
			MaxAttempts: target.maxAttempts,
		}
	}
	deliveryIDs, err := deliveries.InsertBatch(ctx, q, ds)
	if err != nil {
		return 0, fmt.Errorf("batch insert deliveries: %w", err)
	}

	// Round 2: build outbox payloads embedding the delivery IDs, then insert.
	routingKeys := make([]string, len(targets))
	payloads := make([]json.RawMessage, len(targets))
	for i, target := range targets {
		routingKeys[i] = target.sub.ChannelType
		raw, err := json.Marshal(newWorkerMessage(deliveryIDs[i], target.ev, target.sub, target.maxAttempts))
		if err != nil {
			return 0, fmt.Errorf("marshal worker message (channel %d): %w", target.sub.ChannelID, err)
		}
		payloads[i] = raw
	}
	if err := outbox.InsertBatch(ctx, q, routingKeys, payloads); err != nil {
		return 0, fmt.Errorf("batch insert outbox: %w", err)
	}

	return len(targets), nil
}

// filterAllowed applies Gate 1: keeps only channels whose type is in the allowed list.
func filterAllowed(channels []subscriptions.ActiveChannel, allowed []string) []subscriptions.ActiveChannel {
	if len(allowed) == 0 {
		return nil
	}
	allow := make(map[string]struct{}, len(allowed))
	for _, ch := range allowed {
		allow[ch] = struct{}{}
	}
	out := channels[:0]
	for _, ch := range channels {
		if _, ok := allow[ch.ChannelType]; ok {
			out = append(out, ch)
		}
	}
	return out
}

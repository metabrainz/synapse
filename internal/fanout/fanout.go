// Package fanout resolves which delivery channels should receive an event and
// writes the resulting delivery + outbox rows atomically inside the caller's
// transaction. The two core operations are Fan (single event) and FanBatch
// (multiple events, two DB round-trips regardless of count).
package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/metabrainz/synapse/internal/channel"
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/live"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/events"
	"github.com/metabrainz/synapse/internal/store/notifications"
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
	Username      string          `json:"username"`
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
		Username:      ch.Username,
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
	reg         *eventtype.Registry
	expiry      Expiry
}

// New creates a Fanout. Pass adapter.MaxAttemptsFor as maxAttempts in production.
func New(subs Lookup, maxAttempts func(channelType string) int, reg *eventtype.Registry) *Fanout {
	return &Fanout{subs: subs, maxAttempts: maxAttempts, reg: reg, expiry: NewExpiry(nil)}
}

// WithExpiry sets the per-tenant retention that gives notifications their expires_at.
func (f *Fanout) WithExpiry(e Expiry) *Fanout {
	f.expiry = e
	return f
}

// Preview returns the number of channels that would receive this event
// without writing anything — used by the dry_run ingest path.
func (f *Fanout) Preview(ctx context.Context, tenantID, eventType string, recipients []string) (int, error) {
	channels, err := f.resolve(ctx, tenantID, eventType, recipients)
	if err != nil {
		return 0, err
	}
	return len(channels), nil
}

// resolve returns the recipients' allowed subscribed channels plus the channels enabled for everyone.
func (f *Fanout) resolve(ctx context.Context, tenantID, eventType string, recipients []string) ([]subscriptions.ActiveChannel, error) {
	subs, err := f.subs.ListActiveForRecipients(ctx, tenantID, eventType, recipients)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	allowed := f.reg.AllowedChannels(tenantID, eventType)
	return withEveryoneChannels(filterAllowed(subs, allowed), recipients, allowed), nil
}

// InboxRecipient is a user who got an in-app notification row in a tenant.
type InboxRecipient struct{ UserID, TenantID string }

// CreatedNudges returns one "created" live nudge per recipient.
func CreatedNudges(rs []InboxRecipient) []live.Target {
	ts := make([]live.Target, len(rs))
	for i, r := range rs {
		ts[i] = live.Target{UserID: r.UserID, Nudge: live.Nudge{Kind: live.KindCreated, Tenant: r.TenantID}}
	}
	return ts
}

// Fan creates one delivery and one outbox row per active subscription matching ev,
// and one notification per recipient if the event type allows in-app.
// Returns the number of results created and the users who got an inbox row.
func (f *Fanout) Fan(ctx context.Context, q store.Querier, ev events.Event) (int, []InboxRecipient, error) {
	return f.FanBatch(ctx, q, []events.Event{ev})
}

// FanBatch fans out a slice of events in exactly two DB round-trips regardless
// of how many events or subscriptions are involved. It first resolves all
// subscriptions in memory, then writes every delivery in one InsertBatch call
// (getting all IDs back), marshals those IDs into outbox payloads, and writes
// every outbox row in a second InsertBatch call. Notifications for internal
// channels are written in one extra call. Must be called inside a
// store.WithTx callback — all writes are atomic with the upstream event inserts.
// The returned inbox recipients are deduplicated by (user, tenant).
func (f *Fanout) FanBatch(ctx context.Context, q store.Querier, evs []events.Event) (int, []InboxRecipient, error) {
	// target represents one planned delivery: a single (event, subscription) pair.
	type target struct {
		ev          events.Event
		sub         subscriptions.ActiveChannel
		maxAttempts int
	}

	// Resolve all subscriptions in memory before touching the DB.
	var targets []target
	var inbox []notifications.Entry
	var recipients []InboxRecipient
	seen := map[InboxRecipient]struct{}{}
	now := time.Now()
	for _, ev := range evs {
		subs, err := f.resolve(ctx, ev.TenantID, ev.EventType, ev.Recipients)
		if err != nil {
			return 0, nil, err
		}
		for _, sub := range subs {
			if channel.Of(sub.ChannelType).Kind == channel.Internal {
				inbox = append(inbox, notifications.Entry{
					EventID:   ev.ID,
					UserID:    sub.UserID,
					TenantID:  ev.TenantID,
					ExpiresAt: now.Add(f.expiry.For(ev.TenantID)),
				})
				r := InboxRecipient{UserID: sub.UserID, TenantID: ev.TenantID}
				if _, ok := seen[r]; !ok {
					seen[r] = struct{}{}
					recipients = append(recipients, r)
				}
				continue
			}
			targets = append(targets, target{ev, sub, f.maxAttempts(sub.ChannelType)})
		}
	}

	if len(inbox) > 0 {
		if err := notifications.InsertBatch(ctx, q, inbox); err != nil {
			return 0, nil, fmt.Errorf("batch insert notifications: %w", err)
		}
	}
	if len(targets) == 0 {
		return len(inbox), recipients, nil
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
		return 0, nil, fmt.Errorf("batch insert deliveries: %w", err)
	}

	// Round 2: build outbox payloads embedding the delivery IDs, then insert.
	routingKeys := make([]string, len(targets))
	payloads := make([]json.RawMessage, len(targets))
	for i, target := range targets {
		routingKeys[i] = target.sub.ChannelType
		raw, err := json.Marshal(newWorkerMessage(deliveryIDs[i], target.ev, target.sub, target.maxAttempts))
		if err != nil {
			return 0, nil, fmt.Errorf("marshal worker message (channel %d): %w", target.sub.ChannelID, err)
		}
		payloads[i] = raw
	}
	if err := outbox.InsertBatch(ctx, q, routingKeys, payloads); err != nil {
		return 0, nil, fmt.Errorf("batch insert outbox: %w", err)
	}

	return len(targets) + len(inbox), recipients, nil
}

// withEveryoneChannels adds one result per recipient for each channel enabled for
// everyone, dropping any subscription-lookup row for those channels.
func withEveryoneChannels(subs []subscriptions.ActiveChannel, recipients, allowed []string) []subscriptions.ActiveChannel {
	out := subs[:0]
	for _, sub := range subs {
		if channel.Subscribable(sub.ChannelType) {
			out = append(out, sub)
		}
	}
	for _, spec := range channel.All() {
		if spec.Enablement != channel.Everyone || !slices.Contains(allowed, spec.Type) {
			continue
		}
		for _, userID := range recipients {
			out = append(out, subscriptions.ActiveChannel{UserID: userID, ChannelType: spec.Type})
		}
	}
	return out
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

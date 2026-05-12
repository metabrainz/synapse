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
	subs *subscriptions.Repo
}

func New(subs *subscriptions.Repo) *Fanout {
	return &Fanout{subs: subs}
}

// Fan creates one delivery + one outbox row per active subscription matching
// the event. Must be called inside a store.WithTx callback — all writes are
// atomic with the event insert.
func (f *Fanout) Fan(ctx context.Context, q store.Querier, ev events.Event) error {
	channels, err := f.subs.ListActiveForEvent(ctx, ev.TenantID, ev.UserID, ev.EventType)
	if err != nil {
		return fmt.Errorf("list subscriptions: %w", err)
	}

	for _, ch := range channels {
		maxAtt := maxAttempts(ch.ChannelType)
		deliveryID, err := deliveries.Insert(ctx, q, deliveries.Delivery{
			EventID:     ev.ID,
			ChannelID:   ch.ChannelID,
			ChannelType: ch.ChannelType,
			MaxAttempts: maxAtt,
		})
		if err != nil {
			return fmt.Errorf("insert delivery (channel %d): %w", ch.ChannelID, err)
		}

		raw, err := json.Marshal(WorkerMessage{
			DeliveryID:    deliveryID,
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
			MaxAttempts:   maxAtt,
			CreatedAt:     ev.CreatedAt,
		})
		if err != nil {
			return fmt.Errorf("marshal worker message: %w", err)
		}

		if err := outbox.Insert(ctx, q, ch.ChannelType, raw); err != nil {
			return fmt.Errorf("insert outbox (channel %d): %w", ch.ChannelID, err)
		}
	}

	return nil
}

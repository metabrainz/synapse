package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/metabrainz/synapse/internal/fanout"
)

type channelConfig struct {
	URL string `json:"url"`
}

// deliveryPayload is what the receiving webhook endpoint sees.
type deliveryPayload struct {
	EventType  string          `json:"event_type"`
	TenantID   string          `json:"tenant_id"`
	UserID     string          `json:"user_id"`
	Payload    json.RawMessage `json:"payload"`
	DeliveryID int64           `json:"delivery_id"`
	CreatedAt  time.Time       `json:"created_at"`
}

type Adapter struct {
	client *http.Client
}

func New() *Adapter {
	return &Adapter{client: &http.Client{Timeout: 10 * time.Second}}
}

func (a *Adapter) Deliver(ctx context.Context, msg fanout.WorkerMessage) error {
	var cfg channelConfig
	if err := json.Unmarshal(msg.ChannelConfig, &cfg); err != nil {
		return fmt.Errorf("invalid channel config: %w", err)
	}

	body, err := json.Marshal(deliveryPayload{
		EventType:  msg.EventType,
		TenantID:   msg.TenantID,
		UserID:     msg.UserID,
		Payload:    msg.Payload,
		DeliveryID: msg.DeliveryID,
		CreatedAt:  msg.CreatedAt,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Synapse-Event-Type", msg.EventType)
	req.Header.Set("X-Synapse-Tenant-ID", msg.TenantID)
	req.Header.Set("X-Synapse-Delivery-ID", strconv.FormatInt(msg.DeliveryID, 10))

	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

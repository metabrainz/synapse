package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// New creates a webhook adapter with a connection-pooled HTTP client.
// MaxIdleConnsPerHost is set high because workers may fan out to the same host
// concurrently and we don't want to repeatedly open new TCP connections.
func New() *Adapter {
	return &Adapter{client: &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 100},
	}}
}

func (a *Adapter) MaxAttempts() int { return 5 }

// ValidateConfig checks that the webhook config has a non-empty, well-formed URL.
// HTTPS enforcement and private-range blocking are handled separately at the
// network/SSRF policy layer.
func (a *Adapter) ValidateConfig(config json.RawMessage) error {
	var cfg channelConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if cfg.URL == "" {
		return fmt.Errorf("url is required")
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	if u.Host == "" {
		return fmt.Errorf("url must have a host")
	}
	return nil
}

// Deliver POSTs the event payload to the webhook URL in the channel config.
// X-Synapse-* headers let the receiver validate delivery metadata without
// parsing the body (useful for HMAC verification or logging).
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

	// Drain body so the connection can be reused by the pool.
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned HTTP %d", resp.StatusCode)
	}
	return nil
}

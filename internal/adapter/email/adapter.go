package email

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/metabrainz/synapse/internal/fanout"
)

type channelConfig struct {
	To   string `json:"to"`
	Lang string `json:"lang"`
}

type Adapter struct {
	client                     *Client
	tenantFrom                 map[string]string
	tenantNotificationSettings map[string]string
}

func New(baseURL string, tenantFrom, tenantNotificationSettings map[string]string) *Adapter {
	return &Adapter{
		client:                     NewClient(baseURL),
		tenantFrom:                 tenantFrom,
		tenantNotificationSettings: tenantNotificationSettings,
	}
}

func (a *Adapter) MaxAttempts() int { return 5 }

func (a *Adapter) ValidateConfig(config json.RawMessage) error {
	var cfg channelConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if cfg.To == "" || !strings.Contains(cfg.To, "@") {
		return fmt.Errorf("to must be a valid email address")
	}
	return nil
}

func (a *Adapter) Deliver(ctx context.Context, msg fanout.WorkerMessage) error {
	var cfg channelConfig
	if err := json.Unmarshal(msg.ChannelConfig, &cfg); err != nil {
		return fmt.Errorf("email: invalid channel config: %w", err)
	}

	from := a.tenantFrom[msg.TenantID]
	if from == "" {
		return &permanentTransformError{msg: fmt.Sprintf("no email_from configured for tenant %q", msg.TenantID)}
	}
	lang := cfg.Lang
	if lang == "" {
		lang = "en"
	}

	templateID, params, err := transformPayload(msg.EventType, msg.Payload, transformContext{
		TenantID:                msg.TenantID,
		ToName:                  msg.Username,
		NotificationSettingsURL: a.tenantNotificationSettings[msg.TenantID],
	})
	if err != nil {
		return fmt.Errorf("email: transform payload: %w", err)
	}

	return a.client.SendSingle(ctx, SendRequest{
		TemplateID: templateID,
		From:       from,
		To:         cfg.To,
		Lang:       lang,
		Params:     params,
	})
}

package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/metabrainz/synapse/internal/fanout"
)

type channelConfig struct {
	ChatID string `json:"chat_id"`
}

type Adapter struct {
	bot        *Bot
	webhookURL string
	secret     string
}

func New(botToken, webhookURL, webhookSecret string) *Adapter {
	return &Adapter{
		bot:        NewBot(botToken),
		webhookURL: webhookURL,
		secret:     webhookSecret,
	}
}

// Start registers the Telegram webhook if WebhookURL is configured.
// Called once at startup by adapter.Build for every adapter implementing Starter.
func (a *Adapter) Start(ctx context.Context) error {
	if a.webhookURL == "" {
		return nil
	}
	if err := a.bot.SetWebhook(ctx, a.webhookURL, a.secret); err != nil {
		return err
	}
	slog.Info("telegram: webhook registered", "url", a.webhookURL)
	return nil
}

func (a *Adapter) MaxAttempts() int { return 5 }

func (a *Adapter) Deliver(ctx context.Context, msg fanout.WorkerMessage) error {
	var cfg channelConfig
	if err := json.Unmarshal(msg.ChannelConfig, &cfg); err != nil {
		return fmt.Errorf("invalid channel config: %w", err)
	}
	if cfg.ChatID == "" {
		return fmt.Errorf("channel config missing chat_id")
	}
	return a.bot.Send(ctx, cfg.ChatID, renderMessage(msg))
}

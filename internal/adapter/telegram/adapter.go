package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/redis/go-redis/v9"
)

type channelConfig struct {
	ChatID string `json:"chat_id"`
}

// unmarshalChannelConfig is a shared helper used by Deliver and the rate limiter.
func unmarshalChannelConfig(raw json.RawMessage, cfg *channelConfig) error {
	return json.Unmarshal(raw, cfg)
}

type Adapter struct {
	bot        *Bot
	webhookURL string
	secret     string
	rl         *rateLimiter // nil when no Redis is configured
}

// New creates the Telegram adapter.
// rdb may be nil; when nil, the per-chat and global rate limit pre-checks are
// skipped (the adapter still handles 429 responses from the API gracefully).
func New(botToken, webhookURL, webhookSecret string, rdb *redis.Client) *Adapter {
	a := &Adapter{
		bot:        NewBot(botToken),
		webhookURL: webhookURL,
		secret:     webhookSecret,
	}
	if rdb != nil {
		a.rl = newRateLimiter(rdb)
	}
	return a
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

func (a *Adapter) RateLimit(ctx context.Context, msg fanout.WorkerMessage) (bool, time.Duration) {
	if a.rl == nil {
		return true, 0
	}
	return a.rl.RateLimit(ctx, msg)
}

func (a *Adapter) Deliver(ctx context.Context, msg fanout.WorkerMessage) error {
	var cfg channelConfig
	if err := unmarshalChannelConfig(msg.ChannelConfig, &cfg); err != nil {
		return fmt.Errorf("invalid channel config: %w", err)
	}
	if cfg.ChatID == "" {
		return fmt.Errorf("channel config missing chat_id")
	}
	return a.bot.Send(ctx, cfg.ChatID, renderMessage(msg))
}

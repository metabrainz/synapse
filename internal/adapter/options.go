package adapter

import (
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/redis/go-redis/v9"
)

// TelegramOptions configures the Telegram adapter.
// Only BotToken is required for delivery. WebhookURL and WebhookSecret are
// needed only in the API process, which serves the inbound Telegram webhook.
type TelegramOptions struct {
	BotToken      string
	WebhookURL    string // e.g. https://your-domain.com/internal/telegram/webhook
	WebhookSecret string // forwarded as X-Telegram-Bot-Api-Secret-Token
}

// WebhookOptions configures the webhook adapter.
type WebhookOptions struct {
	// AllowPrivateURLs disables the SSRF IP blocklist. Set true only in development.
	AllowPrivateURLs bool
}

// Options holds per-adapter configuration passed to Build.
type Options struct {
	Webhook  WebhookOptions
	Telegram TelegramOptions
	Redis    *redis.Client
	// Registry supplies event templates to the Telegram adapter.
	Registry *eventtype.Registry
}

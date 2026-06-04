package adapter

import "github.com/redis/go-redis/v9"

// TelegramOptions configures the Telegram adapter.
// Only BotToken is required for delivery. WebhookURL and WebhookSecret are
// needed only in the API process, which serves the inbound Telegram webhook.
type TelegramOptions struct {
	BotToken      string
	WebhookURL    string // e.g. https://your-domain.com/internal/telegram/webhook
	WebhookSecret string // forwarded as X-Telegram-Bot-Api-Secret-Token
}

// Options holds per-adapter configuration passed to Build.
// Add one typed field per adapter. Adapters with an empty BotToken
// (or equivalent credential) are skipped by Build.
// Redis is used for adapter-level rate limiting; nil disables pre-emptive
// rate limit checks (adapters still handle 429 responses gracefully).
type Options struct {
	Telegram TelegramOptions
	Redis    *redis.Client
}

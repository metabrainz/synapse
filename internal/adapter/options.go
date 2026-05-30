package adapter

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
type Options struct {
	Telegram TelegramOptions
}

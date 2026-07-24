package eventspec

// EventType is the core contract every event must satisfy.
// Adapter-specific capabilities are separate optional interfaces (e.g. TelegramRenderer)
// so that adding a new adapter never requires touching existing event files.
type EventType interface {
	EventName() string
	AllowedChannels() []string
	Schema() []byte
	SchemaVersion() string // reserved for rolling-deploy schema safety; return "1" for now
}

// TelegramRenderer is implemented by events that support Telegram delivery.
// Telegram() must return a non-empty Go template string; ValidateRegistry enforces this.
type TelegramRenderer interface {
	Telegram() string
}

// Channel constants for use in AllowedChannels() returns and registry checks.
const (
	ChannelWebhook  = "webhook"
	ChannelTelegram = "telegram"
	ChannelEmail    = "email"
)

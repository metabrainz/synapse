package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed notification.json
var notificationSchema []byte

type notificationEvent struct{}

func (notificationEvent) EventName() string { return "notification" }
func (notificationEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram}
}
func (notificationEvent) SchemaVersion() string { return "1" }
func (notificationEvent) Schema() []byte        { return notificationSchema }
func (notificationEvent) Telegram() string {
	return `🔔 {{str .Payload "message"}}`
}

var Notification = notificationEvent{}

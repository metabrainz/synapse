package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed follow.json
var followSchema []byte

type followEvent struct{}

func (followEvent) EventName() string { return "follow" }
func (followEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram}
}
func (followEvent) SchemaVersion() string { return "1" }
func (followEvent) Schema() []byte        { return followSchema }
func (followEvent) Telegram() string {
	return `👤 {{str .Payload "actor" "username"}} started following you`
}

var Follow = followEvent{}

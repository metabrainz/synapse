package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed listen.json
var listenSchema []byte

type listenEvent struct{}

func (listenEvent) EventName() string { return "listen" }
func (listenEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram}
}
func (listenEvent) SchemaVersion() string { return "1" }
func (listenEvent) Schema() []byte        { return listenSchema }
func (listenEvent) Telegram() string {
	return `🎵 {{str .Payload "actor" "username"}} listened to {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}`
}

var Listen = listenEvent{}

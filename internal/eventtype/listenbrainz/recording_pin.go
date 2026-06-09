package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed recording_pin.json
var recordingPinSchema []byte

type recordingPinEvent struct{}

func (recordingPinEvent) EventName() string { return "recording_pin" }
func (recordingPinEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram}
}
func (recordingPinEvent) SchemaVersion() string { return "1" }
func (recordingPinEvent) Schema() []byte        { return recordingPinSchema }
func (recordingPinEvent) Telegram() string {
	return `📌 {{str .Payload "actor" "username"}} pinned {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}{{with str .Payload "message"}}

"{{.}}"{{end}}`
}

var RecordingPin = recordingPinEvent{}

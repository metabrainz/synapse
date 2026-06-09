package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed thanks.json
var thanksSchema []byte

type thanksEvent struct{}

func (thanksEvent) EventName() string { return "thanks" }
func (thanksEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram}
}
func (thanksEvent) SchemaVersion() string { return "1" }
func (thanksEvent) Schema() []byte        { return thanksSchema }
func (thanksEvent) Telegram() string {
	return `🙏 {{str .Payload "actor" "username"}} thanked you for recommending {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}{{with str .Payload "message"}}

"{{.}}"{{end}}`
}

var Thanks = thanksEvent{}

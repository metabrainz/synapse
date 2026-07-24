package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed cb_review.json
var cbReviewSchema []byte

type cbReviewEvent struct{}

func (cbReviewEvent) EventName() string { return "cb_review" }
func (cbReviewEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram, eventspec.ChannelEmail}
}
func (cbReviewEvent) SchemaVersion() string { return "1" }
func (cbReviewEvent) Schema() []byte        { return cbReviewSchema }
func (cbReviewEvent) Telegram() string {
	return `📝 {{str .Payload "actor" "username"}} reviewed {{str .Payload "entity" "name"}}`
}

var CbReview = cbReviewEvent{}

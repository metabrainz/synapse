package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed personal_recording_recommendation.json
var personalRecordingRecommendationSchema []byte

type personalRecordingRecommendationEvent struct{}

func (personalRecordingRecommendationEvent) EventName() string {
	return "personal_recording_recommendation"
}
func (personalRecordingRecommendationEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram}
}
func (personalRecordingRecommendationEvent) SchemaVersion() string { return "1" }
func (personalRecordingRecommendationEvent) Schema() []byte {
	return personalRecordingRecommendationSchema
}
func (personalRecordingRecommendationEvent) Telegram() string {
	return `💌 {{str .Payload "actor" "username"}} recommended {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}} to you{{with str .Payload "message"}}

"{{.}}"{{end}}`
}

var PersonalRecordingRecommendation = personalRecordingRecommendationEvent{}

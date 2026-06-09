package listenbrainz

import (
	_ "embed"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

//go:embed recording_recommendation.json
var recordingRecommendationSchema []byte

type recordingRecommendationEvent struct{}

func (recordingRecommendationEvent) EventName() string { return "recording_recommendation" }
func (recordingRecommendationEvent) AllowedChannels() []string {
	return []string{eventspec.ChannelWebhook, eventspec.ChannelTelegram}
}
func (recordingRecommendationEvent) SchemaVersion() string { return "1" }
func (recordingRecommendationEvent) Schema() []byte        { return recordingRecommendationSchema }
func (recordingRecommendationEvent) Telegram() string {
	return `🎵 {{str .Payload "actor" "username"}} recommended {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}`
}

var RecordingRecommendation = recordingRecommendationEvent{}

package listenbrainz

import "github.com/metabrainz/synapse/internal/eventtype/eventspec"

// All lists every event type for the listenbrainz tenant.
// Convention: one .go + .json file per event; all_test.go enforces this.
var All = []eventspec.EventType{
	Listen,
	RecordingRecommendation,
	PersonalRecordingRecommendation,
	RecordingPin,
	Thanks,
	Follow,
	Notification,
	CbReview,
}

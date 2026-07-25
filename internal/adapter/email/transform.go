package email

import (
	"encoding/json"
	"fmt"

	"github.com/metabrainz/synapse/internal/eventtype/eventview"
)

type transformContext struct {
	TenantID                string
	ToName                  string
	NotificationSettingsURL string
}

var templateIDs = map[string]string{
	"personal_recording_recommendation": "personal-recommendation",
	"follow":                            "follow",
	"recording_pin":                     "recording-pin",
	"recording_recommendation":          "recording-recommendation",
	"cb_review":                         "cb-review",
	"thanks":                            "thanks",
	"notification":                      "notification",
}

// transformPayload maps a Synapse event type to the mb-mail-service template ID
// and reshapes the nested event payload into the flat params each template expects.
func transformPayload(eventType string, payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	tid, ok := templateIDs[eventType]
	if !ok {
		return "", nil, &permanentTransformError{msg: fmt.Sprintf("no email template for event type %q", eventType)}
	}

	v, err := eventview.Extract(payload)
	if err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	params := map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 v.Actor.Username,
		"notification_settings_url": ctx.NotificationSettingsURL,
	}

	if v.Actor.URL != "" {
		params["from_url"] = v.Actor.URL
	}
	if v.Message != "" {
		params["message"] = v.Message
	}
	if r := v.Recording; r != nil {
		params["track_name"] = r.TrackName
		params["track_artist"] = r.ArtistName
		params["track_url"] = r.URL
		params["album_art_url"] = r.AlbumArtURL
	}
	if e := v.Entity; e != nil {
		params["entity_name"] = e.Name
		params["entity_url"] = e.URL
	}

	out, _ := json.Marshal(params)
	return tid, out, nil
}

type permanentTransformError struct{ msg string }

func (e *permanentTransformError) Error() string   { return e.msg }
func (e *permanentTransformError) Permanent() bool { return true }

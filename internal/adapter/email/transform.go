package email

import (
	"encoding/json"
	"fmt"
)

type transformContext struct {
	TenantID                string
	ToName                  string
	NotificationSettingsURL string
}

// transformPayload maps a Synapse event type to the mb-mail-service template ID
// and reshapes the nested event payload into the flat params each template expects.
func transformPayload(eventType string, payload json.RawMessage, ctx transformContext) (templateID string, params json.RawMessage, err error) {
	switch eventType {
	case "personal_recording_recommendation":
		return transformPersonalRecommendation(payload, ctx)
	case "follow":
		return transformFollow(payload, ctx)
	case "recording_pin":
		return transformRecordingPin(payload, ctx)
	case "recording_recommendation":
		return transformRecordingRecommendation(payload, ctx)
	case "cb_review":
		return transformCbReview(payload, ctx)
	case "thanks":
		return transformThanks(payload, ctx)
	case "notification":
		return transformNotification(payload, ctx)
	default:
		return "", nil, &permanentTransformError{msg: fmt.Sprintf("no email template for event type %q", eventType)}
	}
}

func transformPersonalRecommendation(payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	var ev struct {
		Actor struct {
			Username string `json:"username"`
		} `json:"actor"`
		Recording struct {
			TrackName   string `json:"track_name"`
			ArtistName  string `json:"artist_name"`
			URL         string `json:"url"`
			AlbumArtURL string `json:"album_art_url"`
		} `json:"recording"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	out, _ := json.Marshal(map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 ev.Actor.Username,
		"track_name":                ev.Recording.TrackName,
		"track_artist":              ev.Recording.ArtistName,
		"track_url":                 ev.Recording.URL,
		"album_art_url":             ev.Recording.AlbumArtURL,
		"message":                   ev.Message,
		"notification_settings_url": ctx.NotificationSettingsURL,
	})
	return "personal-recommendation", out, nil
}

func transformFollow(payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	var ev struct {
		Actor struct {
			Username string `json:"username"`
			URL      string `json:"url"`
		} `json:"actor"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	out, _ := json.Marshal(map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 ev.Actor.Username,
		"from_url":                  ev.Actor.URL,
		"notification_settings_url": ctx.NotificationSettingsURL,
	})
	return "follow", out, nil
}

func transformRecordingPin(payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	var ev struct {
		Actor struct {
			Username string `json:"username"`
		} `json:"actor"`
		Recording struct {
			TrackName  string `json:"track_name"`
			ArtistName string `json:"artist_name"`
			URL        string `json:"url"`
		} `json:"recording"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	out, _ := json.Marshal(map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 ev.Actor.Username,
		"track_name":                ev.Recording.TrackName,
		"track_artist":              ev.Recording.ArtistName,
		"track_url":                 ev.Recording.URL,
		"message":                   ev.Message,
		"notification_settings_url": ctx.NotificationSettingsURL,
	})
	return "recording-pin", out, nil
}

func transformRecordingRecommendation(payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	var ev struct {
		Actor struct {
			Username string `json:"username"`
		} `json:"actor"`
		Recording struct {
			TrackName   string `json:"track_name"`
			ArtistName  string `json:"artist_name"`
			URL         string `json:"url"`
			AlbumArtURL string `json:"album_art_url"`
		} `json:"recording"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	out, _ := json.Marshal(map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 ev.Actor.Username,
		"track_name":                ev.Recording.TrackName,
		"track_artist":              ev.Recording.ArtistName,
		"track_url":                 ev.Recording.URL,
		"album_art_url":             ev.Recording.AlbumArtURL,
		"notification_settings_url": ctx.NotificationSettingsURL,
	})
	return "recording-recommendation", out, nil
}

func transformCbReview(payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	var ev struct {
		Actor struct {
			Username string `json:"username"`
			URL      string `json:"url"`
		} `json:"actor"`
		Entity struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			Type string `json:"type"`
			URL  string `json:"url"`
		} `json:"entity"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	out, _ := json.Marshal(map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 ev.Actor.Username,
		"entity_name":               ev.Entity.Name,
		"entity_url":                ev.Entity.URL,
		"notification_settings_url": ctx.NotificationSettingsURL,
	})
	return "cb-review", out, nil
}

func transformThanks(payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	var ev struct {
		Actor struct {
			Username string `json:"username"`
			URL      string `json:"url"`
		} `json:"actor"`
		Recording struct {
			TrackName  string `json:"track_name"`
			ArtistName string `json:"artist_name"`
			URL        string `json:"url"`
		} `json:"recording"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	out, _ := json.Marshal(map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 ev.Actor.Username,
		"track_name":                ev.Recording.TrackName,
		"track_artist":              ev.Recording.ArtistName,
		"track_url":                 ev.Recording.URL,
		"notification_settings_url": ctx.NotificationSettingsURL,
	})
	return "thanks", out, nil
}

func transformNotification(payload json.RawMessage, ctx transformContext) (string, json.RawMessage, error) {
	var ev struct {
		Actor struct {
			Username string `json:"username"`
			URL      string `json:"url"`
		} `json:"actor"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		return "", nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	out, _ := json.Marshal(map[string]string{
		"to_name":                   ctx.ToName,
		"from_name":                 ev.Actor.Username,
		"message":                   ev.Message,
		"notification_settings_url": ctx.NotificationSettingsURL,
	})
	return "notification", out, nil
}

type permanentTransformError struct{ msg string }

func (e *permanentTransformError) Error() string   { return e.msg }
func (e *permanentTransformError) Permanent() bool { return true }

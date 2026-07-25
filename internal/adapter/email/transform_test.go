package email

import (
	"encoding/json"
	"testing"
)

func testTransform(t *testing.T, eventType string, payload json.RawMessage, wantTemplateID string, want map[string]string) {
	t.Helper()
	ctx := transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	}
	templateID, params, err := transformPayload(eventType, payload, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if templateID != wantTemplateID {
		t.Fatalf("templateID = %q, want %q", templateID, wantTemplateID)
	}
	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestTransformPersonalRecommendation(t *testing.T) {
	testTransform(t, "personal_recording_recommendation",
		json.RawMessage(`{
			"actor": {"username": "user", "url": "https://listenbrainz.org/user/user"},
			"recording": {
				"track_name": "Song Name",
				"artist_name": "Artist Name",
				"url": "https://listenbrainz.org/recording/abc",
				"album_art_url": "https://example.com/cover.jpg"
			},
			"message": "you have to hear this"
		}`),
		"personal-recommendation",
		map[string]string{
			"to_name":                   "Ansh",
			"from_name":                 "user",
			"track_name":                "Song Name",
			"track_artist":              "Artist Name",
			"track_url":                 "https://listenbrainz.org/recording/abc",
			"album_art_url":             "https://example.com/cover.jpg",
			"message":                   "you have to hear this",
			"notification_settings_url": "https://listenbrainz.org/settings/notifications",
		},
	)
}

func TestTransformFollow(t *testing.T) {
	testTransform(t, "follow",
		json.RawMessage(`{
			"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"}
		}`),
		"follow",
		map[string]string{
			"to_name":                   "Ansh",
			"from_name":                 "rob",
			"from_url":                  "https://listenbrainz.org/user/rob",
			"notification_settings_url": "https://listenbrainz.org/settings/notifications",
		},
	)
}

func TestTransformRecordingRecommendation(t *testing.T) {
	testTransform(t, "recording_recommendation",
		json.RawMessage(`{
			"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
			"recording": {
				"track_name": "Bohemian Rhapsody",
				"artist_name": "Queen",
				"url": "https://listenbrainz.org/recording/xyz",
				"album_art_url": "https://example.com/cover.jpg"
			}
		}`),
		"recording-recommendation",
		map[string]string{
			"to_name":                   "Ansh",
			"from_name":                 "rob",
			"track_name":                "Bohemian Rhapsody",
			"track_artist":              "Queen",
			"track_url":                 "https://listenbrainz.org/recording/xyz",
			"album_art_url":             "https://example.com/cover.jpg",
			"notification_settings_url": "https://listenbrainz.org/settings/notifications",
		},
	)
}

func TestTransformRecordingPin(t *testing.T) {
	testTransform(t, "recording_pin",
		json.RawMessage(`{
			"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
			"recording": {
				"track_name": "Bohemian Rhapsody",
				"artist_name": "Queen",
				"url": "https://listenbrainz.org/recording/xyz"
			},
			"message": "all time classic"
		}`),
		"recording-pin",
		map[string]string{
			"to_name":                   "Ansh",
			"from_name":                 "rob",
			"track_name":                "Bohemian Rhapsody",
			"track_artist":              "Queen",
			"track_url":                 "https://listenbrainz.org/recording/xyz",
			"message":                   "all time classic",
			"notification_settings_url": "https://listenbrainz.org/settings/notifications",
		},
	)
}

func TestTransformCbReview(t *testing.T) {
	testTransform(t, "cb_review",
		json.RawMessage(`{
			"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
			"entity": {"id": "abc-123", "name": "Kind of Blue", "type": "review", "url": "https://critiquebrainz.org/review/abc-123"}
		}`),
		"cb-review",
		map[string]string{
			"to_name":                   "Ansh",
			"from_name":                 "rob",
			"entity_name":               "Kind of Blue",
			"entity_url":                "https://critiquebrainz.org/review/abc-123",
			"notification_settings_url": "https://listenbrainz.org/settings/notifications",
		},
	)
}

func TestTransformThanks(t *testing.T) {
	testTransform(t, "thanks",
		json.RawMessage(`{
			"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
			"recording": {
				"track_name": "Bohemian Rhapsody",
				"artist_name": "Queen",
				"url": "https://listenbrainz.org/recording/xyz",
				"album_art_url": "https://example.com/cover.jpg"
			},
			"message": "great rec!"
		}`),
		"thanks",
		map[string]string{
			"to_name":                   "Ansh",
			"from_name":                 "rob",
			"track_name":                "Bohemian Rhapsody",
			"track_artist":              "Queen",
			"track_url":                 "https://listenbrainz.org/recording/xyz",
			"album_art_url":             "https://example.com/cover.jpg",
			"message":                   "great rec!",
			"notification_settings_url": "https://listenbrainz.org/settings/notifications",
		},
	)
}

func TestTransformNotification(t *testing.T) {
	testTransform(t, "notification",
		json.RawMessage(`{
			"actor": {"username": "admin", "url": "https://listenbrainz.org/user/admin"},
			"message": "Your listening streak hit 100 days!"
		}`),
		"notification",
		map[string]string{
			"to_name":                   "Ansh",
			"from_name":                 "admin",
			"message":                   "Your listening streak hit 100 days!",
			"notification_settings_url": "https://listenbrainz.org/settings/notifications",
		},
	)
}

func TestTransformPayload_UnknownEvent(t *testing.T) {
	_, _, err := transformPayload("unknown", json.RawMessage(`{}`), transformContext{})
	if err == nil {
		t.Fatal("expected error")
	}
	pe, ok := err.(interface{ Permanent() bool })
	if !ok || !pe.Permanent() {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

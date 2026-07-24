package email

import (
	"encoding/json"
	"testing"
)

func TestTransformPersonalRecommendation(t *testing.T) {
	payload := json.RawMessage(`{
		"actor": {"username": "user"},
		"recording": {
			"track_name": "Song Name",
			"artist_name": "Artist Name",
			"url": "https://listenbrainz.org/recording/abc",
			"album_art_url": "https://example.com/cover.jpg"
		},
		"message": "you have to hear this"
	}`)

	templateID, params, err := transformPersonalRecommendation(payload, transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	})
	if err != nil {
		t.Fatal(err)
	}
	if templateID != "personal-recommendation" {
		t.Fatalf("templateID = %q", templateID)
	}

	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"to_name":                   "Ansh",
		"from_name":                 "user",
		"track_name":                "Song Name",
		"track_artist":              "Artist Name",
		"track_url":                 "https://listenbrainz.org/recording/abc",
		"album_art_url":             "https://example.com/cover.jpg",
		"message":                   "you have to hear this",
		"notification_settings_url": "https://listenbrainz.org/settings/notifications",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestTransformFollow(t *testing.T) {
	payload := json.RawMessage(`{
		"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"}
	}`)

	templateID, params, err := transformFollow(payload, transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	})
	if err != nil {
		t.Fatal(err)
	}
	if templateID != "follow" {
		t.Fatalf("templateID = %q", templateID)
	}

	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"to_name":                   "Ansh",
		"from_name":                 "rob",
		"from_url":                  "https://listenbrainz.org/user/rob",
		"notification_settings_url": "https://listenbrainz.org/settings/notifications",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestTransformRecordingRecommendation(t *testing.T) {
	payload := json.RawMessage(`{
		"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
		"recording": {
			"track_name": "Bohemian Rhapsody",
			"artist_name": "Queen",
			"url": "https://listenbrainz.org/recording/xyz",
			"album_art_url": "https://example.com/cover.jpg"
		}
	}`)

	templateID, params, err := transformRecordingRecommendation(payload, transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	})
	if err != nil {
		t.Fatal(err)
	}
	if templateID != "recording-recommendation" {
		t.Fatalf("templateID = %q", templateID)
	}

	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"to_name":                   "Ansh",
		"from_name":                 "rob",
		"track_name":                "Bohemian Rhapsody",
		"track_artist":              "Queen",
		"track_url":                 "https://listenbrainz.org/recording/xyz",
		"album_art_url":             "https://example.com/cover.jpg",
		"notification_settings_url": "https://listenbrainz.org/settings/notifications",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestTransformRecordingPin(t *testing.T) {
	payload := json.RawMessage(`{
		"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
		"recording": {
			"track_name": "Bohemian Rhapsody",
			"artist_name": "Queen",
			"url": "https://listenbrainz.org/recording/xyz"
		},
		"message": "all time classic"
	}`)

	templateID, params, err := transformRecordingPin(payload, transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	})
	if err != nil {
		t.Fatal(err)
	}
	if templateID != "recording-pin" {
		t.Fatalf("templateID = %q", templateID)
	}

	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"to_name":                   "Ansh",
		"from_name":                 "rob",
		"track_name":                "Bohemian Rhapsody",
		"track_artist":              "Queen",
		"track_url":                 "https://listenbrainz.org/recording/xyz",
		"message":                   "all time classic",
		"notification_settings_url": "https://listenbrainz.org/settings/notifications",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestTransformCbReview(t *testing.T) {
	payload := json.RawMessage(`{
		"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
		"entity": {"id": "abc-123", "name": "Kind of Blue", "type": "review", "url": "https://critiquebrainz.org/review/abc-123"}
	}`)

	templateID, params, err := transformCbReview(payload, transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	})
	if err != nil {
		t.Fatal(err)
	}
	if templateID != "cb-review" {
		t.Fatalf("templateID = %q", templateID)
	}

	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"to_name":                   "Ansh",
		"from_name":                 "rob",
		"entity_name":               "Kind of Blue",
		"entity_url":                "https://critiquebrainz.org/review/abc-123",
		"notification_settings_url": "https://listenbrainz.org/settings/notifications",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestTransformThanks(t *testing.T) {
	payload := json.RawMessage(`{
		"actor": {"username": "rob", "url": "https://listenbrainz.org/user/rob"},
		"recording": {
			"track_name": "Bohemian Rhapsody",
			"artist_name": "Queen",
			"url": "https://listenbrainz.org/recording/xyz"
		},
		"message": "great rec!"
	}`)

	templateID, params, err := transformThanks(payload, transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	})
	if err != nil {
		t.Fatal(err)
	}
	if templateID != "thanks" {
		t.Fatalf("templateID = %q", templateID)
	}

	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"to_name":                   "Ansh",
		"from_name":                 "rob",
		"track_name":                "Bohemian Rhapsody",
		"track_artist":              "Queen",
		"track_url":                 "https://listenbrainz.org/recording/xyz",
		"notification_settings_url": "https://listenbrainz.org/settings/notifications",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestTransformNotification(t *testing.T) {
	payload := json.RawMessage(`{
		"actor": {"username": "admin", "url": "https://listenbrainz.org/user/admin"},
		"message": "Your listening streak hit 100 days!"
	}`)

	templateID, params, err := transformNotification(payload, transformContext{
		ToName:                  "Ansh",
		NotificationSettingsURL: "https://listenbrainz.org/settings/notifications",
	})
	if err != nil {
		t.Fatal(err)
	}
	if templateID != "notification" {
		t.Fatalf("templateID = %q", templateID)
	}

	var got map[string]string
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"to_name":                   "Ansh",
		"from_name":                 "admin",
		"message":                   "Your listening streak hit 100 days!",
		"notification_settings_url": "https://listenbrainz.org/settings/notifications",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, got[k], v)
		}
	}
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

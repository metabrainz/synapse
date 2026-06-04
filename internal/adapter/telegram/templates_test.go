package telegram

import (
	"encoding/json"
	"testing"

	"github.com/metabrainz/synapse/internal/fanout"
)

func TestRenderMessage_Listen(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "anshgoyal",
		EventType: "listen",
		Payload: json.RawMessage(`{
			"actor":     { "username": "anshgoyal", "url": "https://listenbrainz.org/user/anshgoyal" },
			"listen":    { "listened_at": 1234567890, "music_service": "spotify" },
			"recording": { "track_name": "Bohemian Rhapsody", "artist_name": "Queen" },
			"summary":   "anshgoyal listened to Bohemian Rhapsody by Queen"
		}`),
	}

	got := renderMessage(msg)
	want := "🎵 anshgoyal listened to Bohemian Rhapsody by Queen"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderMessage_RecordingRecommendation(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "recipient",
		EventType: "recording_recommendation",
		Payload: json.RawMessage(`{
			"actor":     { "username": "recommender", "url": "https://listenbrainz.org/user/recommender" },
			"recording": { "track_name": "Paranoid Android", "artist_name": "Radiohead" },
			"summary":   "recommender recommended Paranoid Android by Radiohead"
		}`),
	}

	got := renderMessage(msg)
	want := "🎵 recommender recommended Paranoid Android by Radiohead"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderMessage_PersonalRecommendation_WithMessage(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "recipient",
		EventType: "personal_recording_recommendation",
		Payload: json.RawMessage(`{
			"actor":     { "username": "sender", "url": "https://listenbrainz.org/user/sender" },
			"recording": { "track_name": "Exit Music", "artist_name": "Radiohead" },
			"message":   "you have to hear this",
			"summary":   "sender recommended Exit Music by Radiohead to you"
		}`),
	}

	got := renderMessage(msg)
	want := "💌 sender recommended Exit Music by Radiohead to you\n\n\"you have to hear this\""
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderMessage_PersonalRecommendation_NoMessage(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "recipient",
		EventType: "personal_recording_recommendation",
		Payload: json.RawMessage(`{
			"actor":     { "username": "sender", "url": "https://listenbrainz.org/user/sender" },
			"recording": { "track_name": "Exit Music", "artist_name": "Radiohead" },
			"summary":   "sender recommended Exit Music by Radiohead to you"
		}`),
	}

	got := renderMessage(msg)
	want := "💌 sender recommended Exit Music by Radiohead to you"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderMessage_Follow(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "followed",
		EventType: "follow",
		Payload: json.RawMessage(`{
			"actor":   { "username": "follower", "url": "https://listenbrainz.org/user/follower" },
			"summary": "follower started following you"
		}`),
	}

	got := renderMessage(msg)
	want := "👤 follower started following you"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderMessage_Notification(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "target",
		EventType: "notification",
		Payload: json.RawMessage(`{
			"actor":   { "username": "troi-bot", "url": "https://listenbrainz.org/user/troi-bot" },
			"message": "Your Weekly Jams is ready!",
			"summary": "troi-bot sent you a notification"
		}`),
	}

	got := renderMessage(msg)
	want := "🔔 Your Weekly Jams is ready!"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderMessage_UnknownEventType(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "anshgoyal",
		EventType: "some_future_event",
		Payload:   json.RawMessage(`{"foo":"bar"}`),
	}

	got := renderMessage(msg)
	if got == "" {
		t.Error("expected non-empty fallback message")
	}
	for _, substr := range []string{"listenbrainz", "some_future_event"} {
		if !containsStr(got, substr) {
			t.Errorf("expected fallback to contain %q, got: %q", substr, got)
		}
	}
}

func TestRenderMessage_MissingPayloadFields(t *testing.T) {
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "anshgoyal",
		EventType: "listen",
		Payload:   json.RawMessage(`{}`),
	}

	got := renderMessage(msg)
	if got == "" {
		t.Error("expected non-empty output even with missing fields")
	}
}

func containsStr(s, sub string) bool {
	for i := range s {
		if i+len(sub) <= len(s) && s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

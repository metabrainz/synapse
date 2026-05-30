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
			"listened_at": 1234567890,
			"track_metadata": {
				"track_name": "Bohemian Rhapsody",
				"artist_name": "Queen"
			}
		}`),
	}

	got := renderMessage(msg)
	want := "🎵 New listen: Bohemian Rhapsody by Queen"
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
	// Should contain tenant and event type from the default template
	for _, substr := range []string{"listenbrainz", "some_future_event"} {
		if len(got) > 0 && !contains(got, substr) {
			t.Errorf("expected fallback to contain %q, got: %q", substr, got)
		}
	}
}

func TestRenderMessage_MissingPayloadFields(t *testing.T) {
	// Payload exists but track_metadata is absent — template should not panic
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "anshgoyal",
		EventType: "listen",
		Payload:   json.RawMessage(`{}`),
	}

	got := renderMessage(msg)
	// Should not panic and should return something
	if got == "" {
		t.Error("expected non-empty output even with missing fields")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsRune(s, sub))
}

func containsRune(s, sub string) bool {
	for i := range s {
		if i+len(sub) <= len(s) && s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

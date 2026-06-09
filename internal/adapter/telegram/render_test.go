package telegram

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/fanout"
)

func newTestRenderer(t *testing.T) *renderer {
	t.Helper()
	return newRenderer(eventtype.NewRegistry(eventtype.KnownTenants))
}

func TestRender_KnownEvent(t *testing.T) {
	r := newTestRenderer(t)
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "anshgoyal",
		EventType: "listen",
		Payload: json.RawMessage(`{
			"actor":     { "username": "anshgoyal", "url": "https://listenbrainz.org/user/anshgoyal" },
			"listen":    { "listened_at": 1234567890 },
			"recording": { "track_name": "Bohemian Rhapsody", "artist_name": "Queen" }
		}`),
	}
	want := "🎵 anshgoyal listened to Bohemian Rhapsody by Queen"
	if got := r.render(msg); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRender_UnknownEventType(t *testing.T) {
	r := newTestRenderer(t)
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "anshgoyal",
		EventType: "some_future_event",
		Payload:   json.RawMessage(`{"foo":"bar"}`),
	}
	got := r.render(msg)
	if got == "" {
		t.Fatal("expected non-empty fallback message")
	}
	for _, substr := range []string{"listenbrainz", "some_future_event"} {
		if !strings.Contains(got, substr) {
			t.Errorf("fallback should contain %q, got: %q", substr, got)
		}
	}
}

func TestRender_MissingPayloadFields(t *testing.T) {
	r := newTestRenderer(t)
	msg := fanout.WorkerMessage{
		TenantID:  "listenbrainz",
		UserID:    "anshgoyal",
		EventType: "listen",
		Payload:   json.RawMessage(`{}`),
	}
	if got := r.render(msg); got == "" {
		t.Error("expected non-empty output even with missing fields")
	}
}

package listenbrainz

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"text/template"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
	"github.com/metabrainz/synapse/internal/eventtype/tmpl"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// renderData mirrors the fields the Telegram adapter exposes to templates.
type renderData struct {
	TenantID   string
	UserID     string
	EventType  string
	Payload    map[string]any
	RawPayload string
}

func mustRender(t *testing.T, tmplSrc, payloadJSON string) string {
	t.Helper()
	tpl := template.Must(template.New("t").Funcs(tmpl.FuncMap).Parse(tmplSrc))
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("bad fixture json: %v", err)
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, renderData{Payload: payload}); err != nil {
		t.Fatalf("render: %v", err)
	}
	return buf.String()
}

func compile(t *testing.T, et eventspec.EventType) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	key := et.EventName() + ".json"
	if err := c.AddResource(key, bytes.NewReader(et.Schema())); err != nil {
		t.Fatalf("[%s] add schema: %v", et.EventName(), err)
	}
	s, err := c.Compile(key)
	if err != nil {
		t.Fatalf("[%s] compile schema: %v", et.EventName(), err)
	}
	return s
}

func validateSchema(t *testing.T, s *jsonschema.Schema, payloadJSON string) error {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(payloadJSON), &v); err != nil {
		t.Fatalf("bad fixture json: %v", err)
	}
	return s.Validate(v)
}

func TestEvents(t *testing.T) {
	cases := []struct {
		et         eventspec.EventType
		good       string
		wantRender string
	}{
		{
			et:         Listen,
			good:       `{"actor":{"username":"anshgoyal","url":"https://listenbrainz.org/user/anshgoyal"},"listen":{"listened_at":1234567890},"recording":{"track_name":"Bohemian Rhapsody","artist_name":"Queen"}}`,
			wantRender: "🎵 anshgoyal listened to Bohemian Rhapsody by Queen",
		},
		{
			et:         RecordingRecommendation,
			good:       `{"actor":{"username":"recommender","url":"https://listenbrainz.org/user/recommender"},"recording":{"track_name":"Paranoid Android","artist_name":"Radiohead"}}`,
			wantRender: "🎵 recommender recommended Paranoid Android by Radiohead",
		},
		{
			et:         PersonalRecordingRecommendation,
			good:       `{"actor":{"username":"sender","url":"https://listenbrainz.org/user/sender"},"recording":{"track_name":"Exit Music","artist_name":"Radiohead"},"message":"you have to hear this"}`,
			wantRender: "💌 sender recommended Exit Music by Radiohead to you\n\n\"you have to hear this\"",
		},
		{
			et:         RecordingPin,
			good:       `{"actor":{"username":"pinner","url":"https://listenbrainz.org/user/pinner"},"recording":{"track_name":"Karma Police","artist_name":"Radiohead"},"message":"on repeat"}`,
			wantRender: "📌 pinner pinned Karma Police by Radiohead\n\n\"on repeat\"",
		},
		{
			et:         Thanks,
			good:       `{"actor":{"username":"grateful","url":"https://listenbrainz.org/user/grateful"},"recording":{"track_name":"No Surprises","artist_name":"Radiohead"},"message":"thank you"}`,
			wantRender: "🙏 grateful thanked you for recommending No Surprises by Radiohead\n\n\"thank you\"",
		},
		{
			et:         Follow,
			good:       `{"actor":{"username":"follower","url":"https://listenbrainz.org/user/follower"}}`,
			wantRender: "👤 follower started following you",
		},
		{
			et:         Notification,
			good:       `{"actor":{"username":"troi-bot","url":"https://listenbrainz.org/user/troi-bot"},"message":"Your Weekly Jams is ready!"}`,
			wantRender: "🔔 Your Weekly Jams is ready!",
		},
		{
			et:         CbReview,
			good:       `{"actor":{"username":"critic","url":"https://listenbrainz.org/user/critic"},"entity":{"id":"abc","name":"OK Computer","type":"review","url":"https://critiquebrainz.org/review/abc"}}`,
			wantRender: "📝 critic reviewed OK Computer",
		},
	}

	for _, tc := range cases {
		t.Run(tc.et.EventName(), func(t *testing.T) {
			s := compile(t, tc.et)

			if err := validateSchema(t, s, tc.good); err != nil {
				t.Errorf("good payload rejected: %v", err)
			}
			if err := validateSchema(t, s, `{}`); err == nil {
				t.Errorf("empty payload should be rejected by schema")
			}

			lists := false
			for _, ch := range tc.et.AllowedChannels() {
				if ch == "telegram" {
					lists = true
				}
			}
			tr, impl := tc.et.(eventspec.TelegramRenderer)
			if lists && !impl {
				t.Errorf("lists 'telegram' but does not implement TelegramRenderer")
			}
			if impl && strings.TrimSpace(tr.Telegram()) == "" {
				t.Errorf("implements TelegramRenderer but Telegram() is empty")
			}

			if impl {
				if got := mustRender(t, tr.Telegram(), tc.good); got != tc.wantRender {
					t.Errorf("render = %q, want %q", got, tc.wantRender)
				}
			}
		})
	}
}

// TestPersonalRecommendationNoMessage covers the {{with}} branch when message is
// absent — the trailing quote block must be omitted.
func TestPersonalRecommendationNoMessage(t *testing.T) {
	got := mustRender(t, PersonalRecordingRecommendation.Telegram(),
		`{"actor":{"username":"sender","url":"https://listenbrainz.org/user/sender"},"recording":{"track_name":"Exit Music","artist_name":"Radiohead"}}`)
	want := "💌 sender recommended Exit Music by Radiohead to you"
	if got != want {
		t.Errorf("render = %q, want %q", got, want)
	}
}

func TestRecordingPinNoMessage(t *testing.T) {
	got := mustRender(t, RecordingPin.Telegram(),
		`{"actor":{"username":"pinner","url":"https://listenbrainz.org/user/pinner"},"recording":{"track_name":"Karma Police","artist_name":"Radiohead"}}`)
	want := "📌 pinner pinned Karma Police by Radiohead"
	if got != want {
		t.Errorf("render = %q, want %q", got, want)
	}
}

func TestThanksNoMessage(t *testing.T) {
	got := mustRender(t, Thanks.Telegram(),
		`{"actor":{"username":"grateful","url":"https://listenbrainz.org/user/grateful"},"recording":{"track_name":"No Surprises","artist_name":"Radiohead"}}`)
	want := "🙏 grateful thanked you for recommending No Surprises by Radiohead"
	if got != want {
		t.Errorf("render = %q, want %q", got, want)
	}
}

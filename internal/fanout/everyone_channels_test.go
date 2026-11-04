package fanout

import (
	"encoding/json"
	"testing"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
)

func types(chs []subscriptions.ActiveChannel) map[string][]string {
	out := map[string][]string{}
	for _, c := range chs {
		out[c.UserID] = append(out[c.UserID], c.ChannelType)
	}
	return out
}

func TestWithEveryoneChannels(t *testing.T) {
	webhook := func(user string) subscriptions.ActiveChannel {
		return subscriptions.ActiveChannel{UserID: user, ChannelID: 7, ChannelType: "webhook", Config: json.RawMessage(`{"url":"http://x"}`)}
	}
	allowedAll := []string{eventspec.ChannelWebhook, eventspec.ChannelInApp}

	t.Run("every recipient gets in-app, even with no configured channels", func(t *testing.T) {
		got := withEveryoneChannels(nil, []string{"a", "b"}, allowedAll)
		if len(got) != 2 {
			t.Fatalf("want 2 results, got %d", len(got))
		}
		for _, c := range got {
			if c.ChannelType != eventspec.ChannelInApp || c.ChannelID != 0 {
				t.Fatalf("unexpected everyone-enabled result: %+v", c)
			}
		}
	})

	t.Run("in-app is added alongside configured channels", func(t *testing.T) {
		got := types(withEveryoneChannels([]subscriptions.ActiveChannel{webhook("a")}, []string{"a", "b"}, allowedAll))
		if len(got["a"]) != 2 || len(got["b"]) != 1 {
			t.Fatalf("want a:{webhook,in_app} b:{in_app}, got %v", got)
		}
	})

	t.Run("event types that do not allow in-app get none", func(t *testing.T) {
		got := withEveryoneChannels([]subscriptions.ActiveChannel{webhook("a")}, []string{"a", "b"}, []string{eventspec.ChannelWebhook})
		if len(got) != 1 || got[0].ChannelType != "webhook" {
			t.Fatalf("want only the webhook result, got %+v", got)
		}
	})

	t.Run("a stray in-app subscription row never produces a second result", func(t *testing.T) {
		stray := subscriptions.ActiveChannel{UserID: "a", ChannelID: 9, ChannelType: eventspec.ChannelInApp, Config: json.RawMessage(`{}`)}
		got := withEveryoneChannels([]subscriptions.ActiveChannel{stray}, []string{"a"}, allowedAll)
		if len(got) != 1 || got[0].ChannelID != 0 {
			t.Fatalf("want exactly one in-app result, got %+v", got)
		}
	})
}

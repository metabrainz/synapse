package channel

import (
	"testing"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

func TestOf(t *testing.T) {
	inApp := Of(eventspec.ChannelInApp)
	if inApp.Kind != Internal || inApp.Enablement != Everyone {
		t.Fatalf("in_app must be internal and enabled for everyone, got %+v", inApp)
	}
	for _, ct := range []string{eventspec.ChannelWebhook, eventspec.ChannelEmail, eventspec.ChannelTelegram, "unlisted"} {
		if s := Of(ct); s.Kind != External || s.Enablement != Subscribed {
			t.Errorf("%s: unlisted channel types must be external and subscribed, got %+v", ct, s)
		}
	}
	if Subscribable(eventspec.ChannelInApp) || !Subscribable(eventspec.ChannelWebhook) {
		t.Error("only channels enabled for everyone are non-subscribable")
	}
}

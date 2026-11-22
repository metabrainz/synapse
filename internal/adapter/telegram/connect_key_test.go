package telegram

import (
	"testing"

	"github.com/metabrainz/synapse/internal/store"
)

func TestConnectKeysFollowTheNamespace(t *testing.T) {
	plain := &connectHandler{rdb: &store.Redis{}}
	if got := plain.connectKey("tok"); got != "synapse:tg:connect:tok" {
		t.Errorf("connectKey = %q", got)
	}
	if got := plain.connectDoneKey("tok"); got != "synapse:tg:connect:done:tok" {
		t.Errorf("connectDoneKey = %q", got)
	}

	ns := &connectHandler{rdb: &store.Redis{Namespace: "test"}}
	if got := ns.connectKey("tok"); got != "test:synapse:tg:connect:tok" {
		t.Errorf("namespaced connectKey = %q", got)
	}
	if got := ns.connectDoneKey("tok"); got != "test:synapse:tg:connect:done:tok" {
		t.Errorf("namespaced connectDoneKey = %q", got)
	}
}

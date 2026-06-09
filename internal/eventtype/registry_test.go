package eventtype

import (
	"testing"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

func TestNewRegistryFromKnownTenants(t *testing.T) {
	reg := NewRegistry(KnownTenants) // panics on any misconfiguration

	if !reg.HasTenant("listenbrainz") {
		t.Fatal("expected listenbrainz tenant")
	}
	if !reg.Has("listenbrainz", "listen") {
		t.Error("expected listen event registered")
	}
	if !reg.IsAllowed("listenbrainz", "listen", "telegram") {
		t.Error("listen should allow telegram")
	}
	if reg.IsAllowed("listenbrainz", "listen", "carrier_pigeon") {
		t.Error("listen should not allow carrier_pigeon")
	}
	if got := len(reg.EventTypes("listenbrainz")); got != 8 {
		t.Errorf("EventTypes = %d, want 8", got)
	}
	if _, ok := reg.Lookup("listenbrainz", "listen"); !ok {
		t.Error("Lookup listen failed")
	}
	if _, ok := reg.Lookup("listenbrainz", "nope"); ok {
		t.Error("Lookup of unknown event should fail")
	}
}

func TestValidateRegistryRejectsDuplicateEvent(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected panic on duplicate event")
		}
	}()
	dup := append([]eventspec.EventType{}, KnownTenants[0].Events...)
	dup = append(dup, dup[0])
	ValidateRegistry([]Tenant{{ID: "lb", Events: dup}})
}

func TestValidateRegistryRejectsDuplicateTenant(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected panic on duplicate tenant")
		}
	}()
	ValidateRegistry([]Tenant{{ID: "x"}, {ID: "x"}})
}

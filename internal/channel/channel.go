// Package channel classifies channel types by kind (internal or external) and
// enablement (everyone or subscribed). Unlisted types are external and subscribed.
package channel

import (
	"slices"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

// Kind says whether a result is produced inside Synapse or by calling another system.
type Kind int

const (
	// External results are deliveries handled by a worker.
	External Kind = iota
	// Internal results are records that fan-out writes directly in the ingest transaction.
	Internal
)

// Enablement says which recipients get a channel.
type Enablement int

const (
	// Subscribed channels reach only recipients with a subscription.
	Subscribed Enablement = iota
	// Everyone channels reach every recipient, with no subscription or users row needed.
	Everyone
)

type Spec struct {
	Type       string
	Kind       Kind
	Enablement Enablement
}

var specs = []Spec{
	{Type: eventspec.ChannelInApp, Kind: Internal, Enablement: Everyone},
}

func All() []Spec { return slices.Clone(specs) }

func Of(channelType string) Spec {
	for _, s := range specs {
		if s.Type == channelType {
			return s
		}
	}
	return Spec{Type: channelType}
}

// Subscribable reports whether users can assign and subscribe to the channel type.
func Subscribable(channelType string) bool { return Of(channelType).Enablement == Subscribed }

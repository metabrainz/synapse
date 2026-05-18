package adapter

import (
	"slices"

	"github.com/metabrainz/synapse/internal/adapter/webhook"
)

// ChannelType represents the type of channel for which an adapter is responsible.
type ChannelType string

const (
	Webhook ChannelType = "webhook"
)

// Registry maps channel type names to their adapter implementations.
// To add a new channel type: implement Adapter in a new sub-package and
// add one line here. No other files need to change.
var Registry = map[ChannelType]Adapter{
	Webhook: webhook.New(),
}

// ChannelTypes returns the registered channel types in sorted order.
// Use this wherever a typed list of channel types is needed — topology
// declaration, config defaults, test setup — so all three stay in sync.
func ChannelTypes() []ChannelType {
	types := make([]ChannelType, 0, len(Registry))
	for k := range Registry {
		types = append(types, k)
	}

	slices.Sort(types)
	return types
}

// MaxAttemptsFor returns the max delivery attempts for the given channel type.
// Falls back to 3 for unregistered types.
func MaxAttemptsFor(channelType string) int {
	if a, ok := Registry[ChannelType(channelType)]; ok {
		return a.MaxAttempts()
	}
	return 3
}

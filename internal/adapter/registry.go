package adapter

import (
	"context"
	"fmt"
	"slices"

	"github.com/metabrainz/synapse/internal/adapter/telegram"
	"github.com/metabrainz/synapse/internal/adapter/webhook"
)

// ChannelType represents the type of channel for which an adapter is responsible.
type ChannelType string

const (
	Webhook  ChannelType = "webhook"
	Telegram ChannelType = "telegram"
)

// Registry maps channel type names to their adapter implementations.
// Populated by Build at startup — callers must call Build before using Registry.
var Registry map[ChannelType]Adapter

// Build initializes the adapter registry and calls Start on every adapter that
// implements Starter. Returns the first startup error encountered.
// Must be called once at program startup before any other adapter functions.
func Build(ctx context.Context, opts Options) error {
	Registry = map[ChannelType]Adapter{
		Webhook: webhook.New(),
	}

	if opts.Telegram.BotToken != "" {
		Registry[Telegram] = telegram.New(
			opts.Telegram.BotToken,
			opts.Telegram.WebhookURL,
			opts.Telegram.WebhookSecret,
			opts.Redis,
		)
	}

	for channelType, adp := range Registry {
		if starter, ok := adp.(Starter); ok {
			if err := starter.Start(ctx); err != nil {
				return fmt.Errorf("adapter %s: start: %w", channelType, err)
			}
		}
	}
	return nil
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

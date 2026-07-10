package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

// GET /v1/me/tenants/{tenant_id}/subscriptions

type listSubscriptionsInput struct {
	TenantID string `path:"tenant_id" doc:"Tenant ID"`
}

type listSubscriptionsOutput struct {
	Body []usereventsubs.Subscription
}

func (h *meHandler) listSubscriptions(ctx context.Context, input *listSubscriptionsInput) (*listSubscriptionsOutput, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	if !h.reg.HasTenant(input.TenantID) {
		return nil, huma.Error404NotFound("tenant not found")
	}
	subs, err := h.subscriptions.ListByUserTenant(ctx, uid, input.TenantID)
	if err != nil {
		return nil, huma.Error500InternalServerError("list subscriptions failed")
	}
	if subs == nil {
		subs = []usereventsubs.Subscription{}
	}
	return &listSubscriptionsOutput{Body: subs}, nil
}

// PUT /v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}

type subscribeInput struct {
	TenantID    string      `path:"tenant_id" doc:"Tenant ID"`
	EventType   string      `path:"event_type" doc:"Tenant-specific event type identifier"`
	ChannelType ChannelType `path:"channel_type" doc:"Channel type to deliver via"`
}

func (h *meHandler) subscribe(ctx context.Context, input *subscribeInput) (*struct{}, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	if !h.reg.HasTenant(input.TenantID) {
		return nil, huma.Error404NotFound("tenant not found")
	}
	if !h.reg.IsAllowed(input.TenantID, input.EventType, string(input.ChannelType)) {
		return nil, huma.Error400BadRequest("event_type or channel_type not permitted for this tenant")
	}

	if err := h.subscriptions.Upsert(ctx, usereventsubs.Subscription{
		UserID:      uid,
		TenantID:    input.TenantID,
		EventType:   input.EventType,
		ChannelType: string(input.ChannelType),
		IsEnabled:   true,
	}); err != nil {
		return nil, huma.Error500InternalServerError("subscribe failed")
	}

	// Auto-assign: if the user has no mapping for this channel type yet and
	// owns exactly one active channel of that type, wire it up automatically.
	mappings, err := h.tenantMappings.ListByUser(ctx, uid, input.TenantID)
	if err == nil {
		alreadyMapped := false
		for _, m := range mappings {
			if m.ChannelType == string(input.ChannelType) {
				alreadyMapped = true
				break
			}
		}
		if !alreadyMapped {
			channels, err := h.channels.ListByUser(ctx, uid)
			if err == nil {
				var candidates []int64
				for _, ch := range channels {
					if ch.ChannelType == string(input.ChannelType) && ch.IsActive {
						candidates = append(candidates, ch.ID)
					}
				}
				if len(candidates) == 1 {
					_ = h.tenantMappings.Upsert(ctx, usertenant.Mapping{
						UserID:        uid,
						TenantID:      input.TenantID,
						ChannelType:   string(input.ChannelType),
						UserChannelID: candidates[0],
						IsEnabled:     true,
					})
				}
			}
		}
	}

	return nil, nil
}

// DELETE /v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}

type unsubscribeInput struct {
	TenantID    string      `path:"tenant_id" doc:"Tenant ID"`
	EventType   string      `path:"event_type" doc:"Event type to unsubscribe from"`
	ChannelType ChannelType `path:"channel_type" doc:"Channel type"`
}

func (h *meHandler) unsubscribe(ctx context.Context, input *unsubscribeInput) (*struct{}, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	if !h.reg.HasTenant(input.TenantID) {
		return nil, huma.Error404NotFound("tenant not found")
	}
	if !h.reg.IsAllowed(input.TenantID, input.EventType, string(input.ChannelType)) {
		return nil, huma.Error400BadRequest("event_type or channel_type not permitted for this tenant")
	}
	if err := h.subscriptions.Delete(ctx, uid, input.TenantID, input.EventType, string(input.ChannelType)); err != nil {
		return nil, huma.Error500InternalServerError("unsubscribe failed")
	}
	return nil, nil
}

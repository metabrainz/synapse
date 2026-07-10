package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

// PUT /v1/me/tenants/{tenant_id}/channels/{channel_type}

type assignTenantChannelInput struct {
	TenantID    string      `path:"tenant_id" doc:"Tenant ID"`
	ChannelType ChannelType `path:"channel_type" doc:"Channel type"`
	Body        struct {
		ChannelID int64 `json:"channel_id" doc:"ID of the user channel to assign" minimum:"1"`
	}
}

func (h *meHandler) assignTenantChannel(ctx context.Context, input *assignTenantChannelInput) (*struct{}, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	if !h.reg.HasTenant(input.TenantID) {
		return nil, huma.Error404NotFound("tenant not found")
	}
	if !h.reg.HasChannelType(input.TenantID, string(input.ChannelType)) {
		return nil, huma.Error400BadRequest("channel_type not supported for this tenant")
	}

	ch, err := h.channels.GetByID(ctx, input.Body.ChannelID)
	if err != nil {
		return nil, huma.Error500InternalServerError("assign channel failed")
	}
	if ch == nil || ch.UserID != uid || ch.ChannelType != string(input.ChannelType) {
		return nil, huma.Error404NotFound("channel not found")
	}
	if !ch.IsActive {
		return nil, huma.Error400BadRequest("channel is not active")
	}

	if err := h.tenantMappings.Upsert(ctx, usertenant.Mapping{
		UserID:        uid,
		TenantID:      input.TenantID,
		ChannelType:   string(input.ChannelType),
		UserChannelID: input.Body.ChannelID,
		IsEnabled:     true,
	}); err != nil {
		return nil, huma.Error500InternalServerError("assign channel failed")
	}
	return nil, nil
}

// GET /v1/me/tenants/{tenant_id}/channels

type listTenantChannelsInput struct {
	TenantID string `path:"tenant_id" doc:"Tenant ID"`
}

type listTenantChannelsOutput struct {
	Body []usertenant.Mapping
}

func (h *meHandler) listTenantChannels(ctx context.Context, input *listTenantChannelsInput) (*listTenantChannelsOutput, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	if !h.reg.HasTenant(input.TenantID) {
		return nil, huma.Error404NotFound("tenant not found")
	}
	mappings, err := h.tenantMappings.ListByUser(ctx, uid, input.TenantID)
	if err != nil {
		return nil, huma.Error500InternalServerError("list tenant channels failed")
	}
	if mappings == nil {
		mappings = []usertenant.Mapping{}
	}
	return &listTenantChannelsOutput{Body: mappings}, nil
}

// DELETE /v1/me/tenants/{tenant_id}/channels/{channel_type}

type removeTenantChannelInput struct {
	TenantID    string      `path:"tenant_id" doc:"Tenant ID"`
	ChannelType ChannelType `path:"channel_type" doc:"Channel type to remove"`
}

func (h *meHandler) removeTenantChannel(ctx context.Context, input *removeTenantChannelInput) (*struct{}, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	if !h.reg.HasTenant(input.TenantID) {
		return nil, huma.Error404NotFound("tenant not found")
	}
	if err := h.tenantMappings.Delete(ctx, uid, input.TenantID, string(input.ChannelType)); err != nil {
		return nil, huma.Error500InternalServerError("remove tenant channel failed")
	}
	return nil, nil
}

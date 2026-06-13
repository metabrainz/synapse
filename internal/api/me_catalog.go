package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/api/middleware"
)

// GET /v1/me/tenants/{tenant_id}/event-types

type listTenantEventTypesInput struct {
	TenantID string `path:"tenant_id" doc:"Tenant ID"`
}

type eventTypeItem struct {
	Name            string   `json:"name"`
	AllowedChannels []string `json:"allowed_channels"`
}

type listTenantEventTypesOutput struct {
	Body []eventTypeItem
}

func (h *meHandler) listTenantEventTypes(ctx context.Context, input *listTenantEventTypesInput) (*listTenantEventTypesOutput, error) {
	if uid := middleware.UserFromContext(ctx); uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}

	eventTypes := h.reg.EventTypes(input.TenantID)
	if eventTypes == nil {
		return nil, huma.Error404NotFound("tenant not found")
	}

	out := make([]eventTypeItem, len(eventTypes))
	for i, et := range eventTypes {
		out[i] = eventTypeItem{Name: et.EventName(), AllowedChannels: et.AllowedChannels()}
	}
	return &listTenantEventTypesOutput{Body: out}, nil
}

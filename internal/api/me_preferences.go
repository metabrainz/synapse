package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/preferences"
)

type preferencesOutput struct {
	Body struct {
		CrossTenantNotifications bool `json:"cross_tenant_notifications" doc:"Whether the cross-tenant notification routes are available"`
	}
}

func newPreferencesOutput(p preferences.Preferences) *preferencesOutput {
	out := &preferencesOutput{}
	out.Body.CrossTenantNotifications = p.CrossTenantNotifications
	return out
}

func (h *meHandler) getPreferences(ctx context.Context, _ *struct{}) (*preferencesOutput, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	p, err := h.preferences.Get(ctx, uid)
	if err != nil {
		return nil, huma.Error500InternalServerError("load preferences failed")
	}
	return newPreferencesOutput(p), nil
}

type updatePreferencesInput struct {
	Body struct {
		CrossTenantNotifications *bool `json:"cross_tenant_notifications,omitempty" doc:"Turn the cross-tenant notification routes on or off"`
	}
}

func (h *meHandler) updatePreferences(ctx context.Context, input *updatePreferencesInput) (*preferencesOutput, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	p, err := h.preferences.Update(ctx, uid, preferences.Update{CrossTenantNotifications: input.Body.CrossTenantNotifications})
	if err != nil {
		return nil, huma.Error500InternalServerError("update preferences failed")
	}
	return newPreferencesOutput(p), nil
}

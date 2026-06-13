package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/userchannels"
)

// GET /v1/me/channels

type listChannelsOutput struct {
	Body []userchannels.UserChannel
}

func (h *meHandler) listChannels(ctx context.Context, _ *struct{}) (*listChannelsOutput, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	chans, err := h.channels.ListByUser(ctx, uid)
	if err != nil {
		return nil, huma.Error500InternalServerError("list channels failed")
	}
	if chans == nil {
		chans = []userchannels.UserChannel{}
	}
	return &listChannelsOutput{Body: chans}, nil
}

// POST /v1/me/channels

type createChannelBody struct {
	ChannelType string          `json:"channel_type" doc:"Type of notification channel (e.g. webhook, telegram)" minLength:"1"`
	Label       string          `json:"label,omitempty" doc:"Human-readable label for this channel"`
	Config      json.RawMessage `json:"config,omitempty" doc:"Channel-type-specific configuration"`
}

type createChannelInput struct {
	Body createChannelBody
}

type createChannelOutput struct {
	Body struct {
		ID int64 `json:"id" doc:"ID of the created channel"`
	}
}

func (h *meHandler) createChannel(ctx context.Context, input *createChannelInput) (*createChannelOutput, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	adp, ok := adapter.Registry[adapter.ChannelType(input.Body.ChannelType)]
	if !ok {
		return nil, huma.Error400BadRequest("unsupported channel_type")
	}
	cfg := input.Body.Config
	if len(cfg) == 0 {
		cfg = json.RawMessage(`{}`)
	}
	if v, ok := adp.(adapter.ConfigValidator); ok {
		if err := v.ValidateConfig(cfg); err != nil {
			return nil, huma.Error400BadRequest(fmt.Sprintf("invalid config: %s", err))
		}
	}
	id, err := h.channels.Insert(ctx, userchannels.UserChannel{
		UserID:      uid,
		ChannelType: input.Body.ChannelType,
		Label:       input.Body.Label,
		Config:      cfg,
	})
	if err != nil {
		return nil, huma.Error500InternalServerError("create channel failed")
	}
	out := &createChannelOutput{}
	out.Body.ID = id
	return out, nil
}

// DELETE /v1/me/channels/{id}

type deleteChannelInput struct {
	ID int64 `path:"id" doc:"Channel ID to delete"`
}

func (h *meHandler) deleteChannel(ctx context.Context, input *deleteChannelInput) (*struct{}, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	if err := h.channels.Delete(ctx, uid, input.ID); err != nil {
		if errors.Is(err, userchannels.ErrNotFound) {
			return nil, huma.Error404NotFound("channel not found")
		}
		return nil, huma.Error500InternalServerError("delete channel failed")
	}
	return nil, nil
}

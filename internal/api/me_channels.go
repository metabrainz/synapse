package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/userchannels"
)

// GET /v1/me/channels

// userChannelView is the API representation of a user channel. Uses API schema
// types so Huma renders ChannelType as an enum and Config as an object schema.
type userChannelView struct {
	ID          int64       `json:"id" doc:"Channel ID"`
	UserID      string      `json:"user_id" doc:"Owner's user ID"`
	ChannelType ChannelType `json:"channel_type" doc:"Notification channel type"`
	Label       string      `json:"label" doc:"Human-readable label"`
	Config      JSONObject  `json:"config" doc:"Channel-type-specific configuration"`
	IsActive    bool        `json:"is_active" doc:"Whether the channel is active"`
	CreatedAt   time.Time   `json:"created_at" doc:"Creation timestamp"`
}

type listChannelsOutput struct {
	Body []userChannelView
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
	views := make([]userChannelView, len(chans))
	for i, ch := range chans {
		views[i] = userChannelView{
			ID:          ch.ID,
			UserID:      ch.UserID,
			ChannelType: ChannelType(ch.ChannelType),
			Label:       ch.Label,
			Config:      JSONObject(ch.Config),
			IsActive:    ch.IsActive,
			CreatedAt:   ch.CreatedAt,
		}
	}
	return &listChannelsOutput{Body: views}, nil
}

// POST /v1/me/channels

type createChannelBody struct {
	ChannelType ChannelType `json:"channel_type" doc:"Type of notification channel"`
	Label       string      `json:"label,omitempty" doc:"Human-readable label for this channel"`
	Config      JSONObject  `json:"config,omitempty" doc:"Channel-type-specific configuration"`
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
	cfg := json.RawMessage(input.Body.Config)
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
		ChannelType: string(input.Body.ChannelType),
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

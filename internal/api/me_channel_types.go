package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/adapter"
	"github.com/metabrainz/synapse/internal/api/middleware"
)

// GET /v1/me/channel-types

type listChannelTypesOutput struct {
	Body []string
}

func (h *meHandler) listChannelTypes(ctx context.Context, _ *struct{}) (*listChannelTypesOutput, error) {
	if uid := middleware.UserFromContext(ctx); uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}

	types := adapter.ChannelTypes()
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return &listChannelTypesOutput{Body: out}, nil
}

package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/deliveries"
)

type deliveriesHandler struct {
	pool *pgxpool.Pool
}

type listDeliveriesInput struct {
	EventID int64 `path:"event_id" doc:"Event ID"`
}

type listDeliveriesOutput struct {
	Body []deliveries.Delivery
}

func (h *deliveriesHandler) listByEvent(ctx context.Context, input *listDeliveriesInput) (*listDeliveriesOutput, error) {
	tenant := middleware.TenantFromContext(ctx)
	if tenant == nil {
		return nil, huma.Error401Unauthorized("unauthorized")
	}

	list, found, err := deliveries.ListByEventForTenant(ctx, h.pool, input.EventID, tenant.ID)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list deliveries")
	}
	if !found {
		return nil, huma.Error404NotFound("event not found")
	}
	if list == nil {
		list = []deliveries.Delivery{}
	}
	return &listDeliveriesOutput{Body: list}, nil
}

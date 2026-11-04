package api

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
	"github.com/metabrainz/synapse/internal/store/deliveries"
	"github.com/metabrainz/synapse/internal/store/notifications"
)

type deliveriesHandler struct {
	pool          *pgxpool.Pool
	notifications *notifications.Repo
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

	// In-app has no delivery row, so list its notifications as DELIVERED results.
	inApp, err := h.notifications.ListByEvent(ctx, input.EventID)
	if err != nil {
		return nil, huma.Error500InternalServerError("failed to list deliveries")
	}
	for _, n := range inApp {
		delivered := n.CreatedAt
		list = append(list, deliveries.Delivery{
			ID:          n.ID,
			EventID:     n.EventID,
			UserID:      n.UserID,
			ChannelType: eventspec.ChannelInApp,
			Status:      deliveries.StatusDelivered,
			DeliveredAt: &delivered,
			CreatedAt:   n.CreatedAt,
			UpdatedAt:   n.CreatedAt,
		})
	}
	if list == nil {
		list = []deliveries.Delivery{}
	}
	return &listDeliveriesOutput{Body: list}, nil
}

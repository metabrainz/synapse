package api

import (
	"context"
	"errors"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/notifications"
)

func (h *meHandler) resolveTenant(ctx context.Context, tenantID string) (string, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return "", huma.Error401Unauthorized("unauthenticated")
	}
	if !h.reg.HasTenant(tenantID) {
		return "", huma.Error404NotFound("tenant not found")
	}
	return uid, nil
}

// GET /v1/me/tenants/{tenant_id}/notifications

type listNotificationsInput struct {
	TenantID   string `path:"tenant_id" doc:"Tenant ID"`
	Limit      int    `query:"limit" default:"25" minimum:"1" maximum:"100" doc:"Page size"`
	BeforeID   int64  `query:"before_id" minimum:"0" doc:"Cursor: return notifications with id lower than this. Use next_cursor from the previous page."`
	EventType  string `query:"event_type" doc:"Only notifications of this event type"`
	UnreadOnly bool   `query:"unread" doc:"Only unread notifications"`
}

type listNotificationsOutput struct {
	Body struct {
		Notifications []notifications.Notification `json:"notifications"`
		NextCursor    *int64                       `json:"next_cursor,omitempty" doc:"Pass as before_id to fetch the next page; absent on the last page"`
	}
}

func (h *meHandler) listNotifications(ctx context.Context, input *listNotificationsInput) (*listNotificationsOutput, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	if input.EventType != "" && !h.reg.Has(input.TenantID, input.EventType) {
		return nil, huma.Error400BadRequest("unknown event_type for this tenant")
	}

	items, next, err := h.notifications.List(ctx, notifications.ListParams{
		UserID:     uid,
		TenantID:   input.TenantID,
		BeforeID:   input.BeforeID,
		EventType:  input.EventType,
		UnreadOnly: input.UnreadOnly,
		Limit:      input.Limit,
	})
	if err != nil {
		return nil, huma.Error500InternalServerError("list notifications failed")
	}

	out := &listNotificationsOutput{}
	out.Body.Notifications = items
	if next != 0 {
		out.Body.NextCursor = &next
	}
	return out, nil
}

// GET /v1/me/tenants/{tenant_id}/notifications/unread-count

type tenantPathInput struct {
	TenantID string `path:"tenant_id" doc:"Tenant ID"`
}

type unreadCountOutput struct {
	Body struct {
		Count int64 `json:"count" doc:"Number of unread notifications"`
	}
}

func (h *meHandler) unreadCount(ctx context.Context, input *tenantPathInput) (*unreadCountOutput, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	n, err := h.notifications.UnreadCount(ctx, uid, input.TenantID)
	if err != nil {
		return nil, huma.Error500InternalServerError("unread count failed")
	}
	out := &unreadCountOutput{}
	out.Body.Count = n
	return out, nil
}

// POST /v1/me/tenants/{tenant_id}/notifications/read-all

type markAllReadOutput struct {
	Body struct {
		Updated int64 `json:"updated" doc:"Number of notifications newly marked read"`
	}
}

func (h *meHandler) markAllRead(ctx context.Context, input *tenantPathInput) (*markAllReadOutput, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	n, err := h.notifications.MarkAllRead(ctx, uid, input.TenantID)
	if err != nil {
		return nil, huma.Error500InternalServerError("mark all read failed")
	}
	out := &markAllReadOutput{}
	out.Body.Updated = n
	return out, nil
}

// POST   /v1/me/tenants/{tenant_id}/notifications/{id}/read
// DELETE /v1/me/tenants/{tenant_id}/notifications/{id}

type notificationIDInput struct {
	TenantID string `path:"tenant_id" doc:"Tenant ID"`
	ID       int64  `path:"id" minimum:"1" doc:"Notification ID"`
}

func (h *meHandler) markRead(ctx context.Context, input *notificationIDInput) (*struct{}, error) {
	return h.updateNotification(ctx, input, h.notifications.MarkRead, "mark read failed")
}

func (h *meHandler) dismissNotification(ctx context.Context, input *notificationIDInput) (*struct{}, error) {
	return h.updateNotification(ctx, input, h.notifications.Dismiss, "dismiss failed")
}

func (h *meHandler) updateNotification(
	ctx context.Context,
	input *notificationIDInput,
	op func(ctx context.Context, userID, tenantID string, id int64) error,
	failMsg string,
) (*struct{}, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	if err := op(ctx, uid, input.TenantID, input.ID); err != nil {
		if errors.Is(err, notifications.ErrNotFound) {
			return nil, huma.Error404NotFound("notification not found")
		}
		return nil, huma.Error500InternalServerError(failMsg)
	}
	return nil, nil
}

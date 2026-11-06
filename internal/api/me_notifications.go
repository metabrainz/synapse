package api

import (
	"context"
	"errors"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/live"
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

// requireCrossTenant guards the global routes. The preference is presentation only,
// not an access control.
func (h *meHandler) requireCrossTenant(ctx context.Context) (string, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return "", huma.Error401Unauthorized("unauthenticated")
	}
	prefs, err := h.preferences.Get(ctx, uid)
	if err != nil {
		return "", huma.Error500InternalServerError("load preferences failed")
	}
	if !prefs.CrossTenantNotifications {
		return "", huma.Error403Forbidden("cross-tenant notifications are disabled; turn them on with PATCH /v1/me/preferences")
	}
	return uid, nil
}

type listNotificationsOutput struct {
	Body struct {
		Notifications []notifications.Notification `json:"notifications"`
		NextCursor    *int64                       `json:"next_cursor,omitempty" doc:"Pass as before_id to fetch the next page; absent on the last page"`
	}
}

func (h *meHandler) list(ctx context.Context, p notifications.ListParams) (*listNotificationsOutput, error) {
	items, next, err := h.notifications.List(ctx, p)
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

type markAllReadOutput struct {
	Body struct {
		Updated int64 `json:"updated" doc:"Number of notifications newly marked read"`
	}
}

func (h *meHandler) readAll(ctx context.Context, userID, tenantID string) (*markAllReadOutput, error) {
	byTenant, err := h.notifications.MarkAllRead(ctx, userID, tenantID)
	if err != nil {
		return nil, huma.Error500InternalServerError("mark all read failed")
	}
	out := &markAllReadOutput{}
	for tenant, n := range byTenant {
		out.Body.Updated += n
		h.live.Publish(ctx, userID, live.Nudge{Kind: live.KindChanged, Tenant: tenant})
	}
	return out, nil
}

func (h *meHandler) update(
	ctx context.Context,
	userID, tenantID string,
	id int64,
	op func(ctx context.Context, userID, tenantID string, id int64) (string, error),
	failMsg string,
) (*struct{}, error) {
	tenant, err := op(ctx, userID, tenantID, id)
	if err != nil {
		if errors.Is(err, notifications.ErrNotFound) {
			return nil, huma.Error404NotFound("notification not found")
		}
		return nil, huma.Error500InternalServerError(failMsg)
	}
	h.live.Publish(ctx, userID, live.Nudge{Kind: live.KindChanged, Tenant: tenant})
	return nil, nil
}

// Tenant routes: always scoped to the tenant in the path.

// GET /v1/me/tenants/{tenant_id}/notifications

type listNotificationsInput struct {
	TenantID   string `path:"tenant_id" doc:"Tenant ID"`
	Limit      int    `query:"limit" default:"25" minimum:"1" maximum:"100" doc:"Page size"`
	BeforeID   int64  `query:"before_id" minimum:"0" doc:"Cursor: return notifications with id lower than this. Use next_cursor from the previous page."`
	EventType  string `query:"event_type" doc:"Only notifications of this event type"`
	UnreadOnly bool   `query:"unread" doc:"Only unread notifications"`
}

func (h *meHandler) listNotifications(ctx context.Context, input *listNotificationsInput) (*listNotificationsOutput, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	if input.EventType != "" && !h.reg.Has(input.TenantID, input.EventType) {
		return nil, huma.Error400BadRequest("unknown event_type for this tenant")
	}
	return h.list(ctx, notifications.ListParams{
		UserID:     uid,
		TenantID:   input.TenantID,
		BeforeID:   input.BeforeID,
		EventType:  input.EventType,
		UnreadOnly: input.UnreadOnly,
		Limit:      input.Limit,
	})
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

func (h *meHandler) markAllRead(ctx context.Context, input *tenantPathInput) (*markAllReadOutput, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	return h.readAll(ctx, uid, input.TenantID)
}

// POST   /v1/me/tenants/{tenant_id}/notifications/{id}/read
// DELETE /v1/me/tenants/{tenant_id}/notifications/{id}

type notificationIDInput struct {
	TenantID string `path:"tenant_id" doc:"Tenant ID"`
	ID       int64  `path:"id" minimum:"1" doc:"Notification ID"`
}

func (h *meHandler) markRead(ctx context.Context, input *notificationIDInput) (*struct{}, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	return h.update(ctx, uid, input.TenantID, input.ID, h.notifications.MarkRead, "mark read failed")
}

func (h *meHandler) dismissNotification(ctx context.Context, input *notificationIDInput) (*struct{}, error) {
	uid, err := h.resolveTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	return h.update(ctx, uid, input.TenantID, input.ID, h.notifications.Dismiss, "dismiss failed")
}

// Global routes: span every tenant, gated by cross_tenant_notifications.

// GET /v1/me/notifications

type listAllNotificationsInput struct {
	Limit      int    `query:"limit" default:"25" minimum:"1" maximum:"100" doc:"Page size"`
	BeforeID   int64  `query:"before_id" minimum:"0" doc:"Cursor: return notifications with id lower than this. Use next_cursor from the previous page."`
	UnreadOnly bool   `query:"unread" doc:"Only unread notifications"`
	TenantID   string `query:"tenant_id" doc:"Only notifications of this tenant"`
}

func (h *meHandler) listAllNotifications(ctx context.Context, input *listAllNotificationsInput) (*listNotificationsOutput, error) {
	uid, err := h.requireCrossTenant(ctx)
	if err != nil {
		return nil, err
	}
	if input.TenantID != "" && !h.reg.HasTenant(input.TenantID) {
		return nil, huma.Error400BadRequest("unknown tenant_id")
	}
	return h.list(ctx, notifications.ListParams{
		UserID:     uid,
		TenantID:   input.TenantID,
		BeforeID:   input.BeforeID,
		UnreadOnly: input.UnreadOnly,
		Limit:      input.Limit,
	})
}

// GET /v1/me/notifications/unread-counts

type unreadCountsOutput struct {
	Body struct {
		Count    int64            `json:"count" doc:"Unread notifications across every tenant"`
		ByTenant map[string]int64 `json:"by_tenant" doc:"Unread notifications per tenant"`
	}
}

func (h *meHandler) unreadCounts(ctx context.Context, _ *struct{}) (*unreadCountsOutput, error) {
	uid, err := h.requireCrossTenant(ctx)
	if err != nil {
		return nil, err
	}
	byTenant, err := h.notifications.UnreadCountsByTenant(ctx, uid)
	if err != nil {
		return nil, huma.Error500InternalServerError("unread counts failed")
	}
	out := &unreadCountsOutput{}
	out.Body.ByTenant = byTenant
	for _, n := range byTenant {
		out.Body.Count += n
	}
	return out, nil
}

// POST /v1/me/notifications/read-all

func (h *meHandler) markAllReadAllTenants(ctx context.Context, _ *struct{}) (*markAllReadOutput, error) {
	uid, err := h.requireCrossTenant(ctx)
	if err != nil {
		return nil, err
	}
	return h.readAll(ctx, uid, "")
}

// POST   /v1/me/notifications/{id}/read
// DELETE /v1/me/notifications/{id}

type globalNotificationIDInput struct {
	ID int64 `path:"id" minimum:"1" doc:"Notification ID"`
}

func (h *meHandler) markReadAnyTenant(ctx context.Context, input *globalNotificationIDInput) (*struct{}, error) {
	uid, err := h.requireCrossTenant(ctx)
	if err != nil {
		return nil, err
	}
	return h.update(ctx, uid, "", input.ID, h.notifications.MarkRead, "mark read failed")
}

func (h *meHandler) dismissAnyTenant(ctx context.Context, input *globalNotificationIDInput) (*struct{}, error) {
	uid, err := h.requireCrossTenant(ctx)
	if err != nil {
		return nil, err
	}
	return h.update(ctx, uid, "", input.ID, h.notifications.Dismiss, "dismiss failed")
}

// POST /v1/me/notifications/stream-ticket

type streamTicketOutput struct {
	Body struct {
		Ticket    string `json:"ticket" doc:"Single-use ticket; pass it to a stream route as ?ticket="`
		ExpiresIn int    `json:"expires_in" doc:"Seconds until the ticket expires"`
	}
}

func (h *meHandler) createStreamTicket(ctx context.Context, _ *struct{}) (*streamTicketOutput, error) {
	uid := middleware.UserFromContext(ctx)
	if uid == "" {
		return nil, huma.Error401Unauthorized("unauthenticated")
	}
	ticket, err := h.tickets.Mint(ctx, uid)
	if err != nil {
		return nil, huma.Error503ServiceUnavailable("stream unavailable")
	}
	out := &streamTicketOutput{}
	out.Body.Ticket = ticket
	out.Body.ExpiresIn = int(live.TicketTTL.Seconds())
	return out, nil
}

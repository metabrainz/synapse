package api

// routes.go is the complete API surface of Synapse — every route in one place.
// Handler logic lives in the handler files; this file is the index.
//
// Surface A (TenantAPIKey):  /v1/events/**
// Surface B (UserOAuth):     /v1/me/**

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/live"
	"github.com/metabrainz/synapse/internal/oauth"
	"github.com/metabrainz/synapse/internal/store/notifications"
	"github.com/metabrainz/synapse/internal/store/preferences"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

var (
	surfaceASec   = []map[string][]string{{"TenantAPIKey": {}}}
	surfaceBInbox = []map[string][]string{{"UserOAuth": {oauth.ScopeInbox}}}
	surfaceBFull  = []map[string][]string{{"UserOAuth": {oauth.ScopeFull}}}
)

func registerRoutes(
	api huma.API,
	pool *pgxpool.Pool,
	fan *fanout.Fanout,
	reg *eventtype.Registry,
	userChannels *userchannels.Repo,
	tenantMappings *usertenant.Repo,
	subscriptions *usereventsubs.Repo,
	inbox *notifications.Repo,
	prefs *preferences.Repo,
	pub *live.Publisher,
) {
	// --- Surface A: tenant API key ---

	ing := &ingestHandler{pool: pool, fan: fan, reg: reg, live: pub}

	huma.Register(api, huma.Operation{
		OperationID:   "ingest-event",
		Method:        http.MethodPost,
		Path:          "/v1/events",
		Summary:       "Ingest an event and fan it out to subscriber channels",
		Tags:          []string{"events"},
		DefaultStatus: http.StatusAccepted,
		Security:      surfaceASec,
	}, ing.handle)

	huma.Register(api, huma.Operation{
		OperationID: "list-event-deliveries",
		Method:      http.MethodGet,
		Path:        "/v1/events/{event_id}/deliveries",
		Summary:     "List delivery records for an event (tenant-scoped)",
		Tags:        []string{"events"},
		Security:    surfaceASec,
	}, (&deliveriesHandler{pool: pool, notifications: inbox}).listByEvent)

	// --- Surface B: MetaBrainz OAuth token ---

	me := &meHandler{
		channels:       userChannels,
		tenantMappings: tenantMappings,
		subscriptions:  subscriptions,
		notifications:  inbox,
		preferences:    prefs,
		reg:            reg,
		live:           pub,
	}

	// Channels — CRUD for the user's notification channels.
	huma.Register(api, huma.Operation{
		OperationID: "list-channels",
		Method:      http.MethodGet,
		Path:        "/v1/me/channels",
		Summary:     "List the authenticated user's notification channels",
		Tags:        []string{"channels"},
		Security:    surfaceBFull,
	}, me.listChannels)

	huma.Register(api, huma.Operation{
		OperationID:   "create-channel",
		Method:        http.MethodPost,
		Path:          "/v1/me/channels",
		Summary:       "Create a new notification channel",
		Tags:          []string{"channels"},
		DefaultStatus: http.StatusCreated,
		Security:      surfaceBFull,
	}, me.createChannel)

	huma.Register(api, huma.Operation{
		OperationID:   "delete-channel",
		Method:        http.MethodDelete,
		Path:          "/v1/me/channels/{id}",
		Summary:       "Delete a notification channel",
		Tags:          []string{"channels"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBFull,
	}, me.deleteChannel)

	// Catalog — read-only view of what a tenant exposes.
	huma.Register(api, huma.Operation{
		OperationID: "list-channel-types",
		Method:      http.MethodGet,
		Path:        "/v1/me/channel-types",
		Summary:     "List notification channel types enabled on this Synapse instance",
		Tags:        []string{"catalog"},
		Security:    surfaceBFull,
	}, me.listChannelTypes)

	huma.Register(api, huma.Operation{
		OperationID: "list-tenant-event-types",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/event-types",
		Summary:     "List event types available for a tenant",
		Tags:        []string{"catalog"},
		Security:    surfaceBFull,
	}, me.listTenantEventTypes)

	// Tenant channel mappings — which channel receives deliveries for a given tenant.
	huma.Register(api, huma.Operation{
		OperationID: "list-tenant-channels",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/channels",
		Summary:     "List the user's channel mappings for a tenant",
		Tags:        []string{"tenant-channels"},
		Security:    surfaceBFull,
	}, me.listTenantChannels)

	huma.Register(api, huma.Operation{
		OperationID:   "assign-tenant-channel",
		Method:        http.MethodPut,
		Path:          "/v1/me/tenants/{tenant_id}/channels/{channel_type}",
		Summary:       "Assign a channel to receive deliveries for a tenant",
		Tags:          []string{"tenant-channels"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBFull,
	}, me.assignTenantChannel)

	huma.Register(api, huma.Operation{
		OperationID:   "remove-tenant-channel",
		Method:        http.MethodDelete,
		Path:          "/v1/me/tenants/{tenant_id}/channels/{channel_type}",
		Summary:       "Remove the channel mapping for a tenant",
		Tags:          []string{"tenant-channels"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBFull,
	}, me.removeTenantChannel)

	// Subscriptions — opt in/out of specific event types per tenant and channel.
	huma.Register(api, huma.Operation{
		OperationID: "list-subscriptions",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/subscriptions",
		Summary:     "List the user's event subscriptions for a tenant",
		Tags:        []string{"subscriptions"},
		Security:    surfaceBFull,
	}, me.listSubscriptions)

	huma.Register(api, huma.Operation{
		OperationID:   "subscribe",
		Method:        http.MethodPut,
		Path:          "/v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}",
		Summary:       "Subscribe to an event type via a channel",
		Tags:          []string{"subscriptions"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBFull,
	}, me.subscribe)

	huma.Register(api, huma.Operation{
		OperationID:   "unsubscribe",
		Method:        http.MethodDelete,
		Path:          "/v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}",
		Summary:       "Unsubscribe from an event type",
		Tags:          []string{"subscriptions"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBFull,
	}, me.unsubscribe)

	// Notifications — the user's in-app inbox for a tenant.
	huma.Register(api, huma.Operation{
		OperationID: "list-notifications",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/notifications",
		Summary:     "List the user's in-app notifications for a tenant, newest first",
		Tags:        []string{"notifications"},
		Security:    surfaceBInbox,
	}, me.listNotifications)

	huma.Register(api, huma.Operation{
		OperationID: "notifications-unread-count",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/notifications/unread-count",
		Summary:     "Count unread in-app notifications for a tenant",
		Tags:        []string{"notifications"},
		Security:    surfaceBInbox,
	}, me.unreadCount)

	huma.Register(api, huma.Operation{
		OperationID: "mark-all-notifications-read",
		Method:      http.MethodPost,
		Path:        "/v1/me/tenants/{tenant_id}/notifications/read-all",
		Summary:     "Mark all of the user's in-app notifications for a tenant as read",
		Tags:        []string{"notifications"},
		Security:    surfaceBInbox,
	}, me.markAllRead)

	huma.Register(api, huma.Operation{
		OperationID:   "mark-notification-read",
		Method:        http.MethodPost,
		Path:          "/v1/me/tenants/{tenant_id}/notifications/{id}/read",
		Summary:       "Mark one in-app notification as read",
		Tags:          []string{"notifications"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBInbox,
	}, me.markRead)

	huma.Register(api, huma.Operation{
		OperationID:   "dismiss-notification",
		Method:        http.MethodDelete,
		Path:          "/v1/me/tenants/{tenant_id}/notifications/{id}",
		Summary:       "Dismiss (hide) an in-app notification",
		Tags:          []string{"notifications"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBInbox,
	}, me.dismissNotification)

	// Notifications across every tenant.
	huma.Register(api, huma.Operation{
		OperationID: "list-all-notifications",
		Method:      http.MethodGet,
		Path:        "/v1/me/notifications",
		Summary:     "List the user's in-app notifications across tenants, newest first",
		Tags:        []string{"notifications"},
		Security:    surfaceBInbox,
	}, me.listAllNotifications)

	huma.Register(api, huma.Operation{
		OperationID: "all-notifications-unread-counts",
		Method:      http.MethodGet,
		Path:        "/v1/me/notifications/unread-counts",
		Summary:     "Count unread in-app notifications across tenants, in total and per tenant",
		Tags:        []string{"notifications"},
		Security:    surfaceBInbox,
	}, me.unreadCounts)

	huma.Register(api, huma.Operation{
		OperationID: "mark-all-notifications-read-all-tenants",
		Method:      http.MethodPost,
		Path:        "/v1/me/notifications/read-all",
		Summary:     "Mark every in-app notification of every tenant as read",
		Tags:        []string{"notifications"},
		Security:    surfaceBInbox,
	}, me.markAllReadAllTenants)

	huma.Register(api, huma.Operation{
		OperationID:   "mark-notification-read-any-tenant",
		Method:        http.MethodPost,
		Path:          "/v1/me/notifications/{id}/read",
		Summary:       "Mark one in-app notification of any tenant as read",
		Tags:          []string{"notifications"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBInbox,
	}, me.markReadAnyTenant)

	huma.Register(api, huma.Operation{
		OperationID:   "dismiss-notification-any-tenant",
		Method:        http.MethodDelete,
		Path:          "/v1/me/notifications/{id}",
		Summary:       "Dismiss an in-app notification of any tenant",
		Tags:          []string{"notifications"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBInbox,
	}, me.dismissAnyTenant)

	// Preferences — per-user settings about how the inbox is presented.
	huma.Register(api, huma.Operation{
		OperationID: "get-preferences",
		Method:      http.MethodGet,
		Path:        "/v1/me/preferences",
		Summary:     "Get the user's preferences (defaults if none are stored)",
		Tags:        []string{"preferences"},
		Security:    surfaceBFull,
	}, me.getPreferences)

	huma.Register(api, huma.Operation{
		OperationID: "update-preferences",
		Method:      http.MethodPatch,
		Path:        "/v1/me/preferences",
		Summary:     "Change the given preferences",
		Tags:        []string{"preferences"},
		Security:    surfaceBFull,
	}, me.updatePreferences)
}

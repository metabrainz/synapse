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
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

var (
	surfaceASec = []map[string][]string{{"TenantAPIKey": {}}}
	surfaceBSec = []map[string][]string{{"UserOAuth": {}}}
)

func registerRoutes(
	api huma.API,
	pool *pgxpool.Pool,
	fan *fanout.Fanout,
	reg *eventtype.Registry,
	userChannels *userchannels.Repo,
	tenantMappings *usertenant.Repo,
	subscriptions *usereventsubs.Repo,
) {
	// --- Surface A: tenant API key ---

	ing := &ingestHandler{pool: pool, fan: fan, reg: reg}

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
	}, (&deliveriesHandler{pool: pool}).listByEvent)

	// --- Surface B: MetaBrainz OAuth token ---

	me := &meHandler{
		channels:       userChannels,
		tenantMappings: tenantMappings,
		subscriptions:  subscriptions,
		reg:            reg,
	}

	// Channels — CRUD for the user's notification channels.
	huma.Register(api, huma.Operation{
		OperationID: "list-channels",
		Method:      http.MethodGet,
		Path:        "/v1/me/channels",
		Summary:     "List the authenticated user's notification channels",
		Tags:        []string{"channels"},
		Security:    surfaceBSec,
	}, me.listChannels)

	huma.Register(api, huma.Operation{
		OperationID:   "create-channel",
		Method:        http.MethodPost,
		Path:          "/v1/me/channels",
		Summary:       "Create a new notification channel",
		Tags:          []string{"channels"},
		DefaultStatus: http.StatusCreated,
		Security:      surfaceBSec,
	}, me.createChannel)

	huma.Register(api, huma.Operation{
		OperationID:   "delete-channel",
		Method:        http.MethodDelete,
		Path:          "/v1/me/channels/{id}",
		Summary:       "Delete a notification channel",
		Tags:          []string{"channels"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBSec,
	}, me.deleteChannel)

	// Catalog — read-only view of what a tenant exposes.
	huma.Register(api, huma.Operation{
		OperationID: "list-channel-types",
		Method:      http.MethodGet,
		Path:        "/v1/me/channel-types",
		Summary:     "List notification channel types enabled on this Synapse instance",
		Tags:        []string{"catalog"},
		Security:    surfaceBSec,
	}, me.listChannelTypes)

	huma.Register(api, huma.Operation{
		OperationID: "list-tenant-event-types",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/event-types",
		Summary:     "List event types available for a tenant",
		Tags:        []string{"catalog"},
		Security:    surfaceBSec,
	}, me.listTenantEventTypes)

	// Tenant channel mappings — which channel receives deliveries for a given tenant.
	huma.Register(api, huma.Operation{
		OperationID: "list-tenant-channels",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/channels",
		Summary:     "List the user's channel mappings for a tenant",
		Tags:        []string{"tenant-channels"},
		Security:    surfaceBSec,
	}, me.listTenantChannels)

	huma.Register(api, huma.Operation{
		OperationID:   "assign-tenant-channel",
		Method:        http.MethodPut,
		Path:          "/v1/me/tenants/{tenant_id}/channels/{channel_type}",
		Summary:       "Assign a channel to receive deliveries for a tenant",
		Tags:          []string{"tenant-channels"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBSec,
	}, me.assignTenantChannel)

	huma.Register(api, huma.Operation{
		OperationID:   "remove-tenant-channel",
		Method:        http.MethodDelete,
		Path:          "/v1/me/tenants/{tenant_id}/channels/{channel_type}",
		Summary:       "Remove the channel mapping for a tenant",
		Tags:          []string{"tenant-channels"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBSec,
	}, me.removeTenantChannel)

	// Subscriptions — opt in/out of specific event types per tenant and channel.
	huma.Register(api, huma.Operation{
		OperationID: "list-subscriptions",
		Method:      http.MethodGet,
		Path:        "/v1/me/tenants/{tenant_id}/subscriptions",
		Summary:     "List the user's event subscriptions for a tenant",
		Tags:        []string{"subscriptions"},
		Security:    surfaceBSec,
	}, me.listSubscriptions)

	huma.Register(api, huma.Operation{
		OperationID:   "subscribe",
		Method:        http.MethodPut,
		Path:          "/v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}",
		Summary:       "Subscribe to an event type via a channel",
		Tags:          []string{"subscriptions"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBSec,
	}, me.subscribe)

	huma.Register(api, huma.Operation{
		OperationID:   "unsubscribe",
		Method:        http.MethodDelete,
		Path:          "/v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}",
		Summary:       "Unsubscribe from an event type",
		Tags:          []string{"subscriptions"},
		DefaultStatus: http.StatusNoContent,
		Security:      surfaceBSec,
	}, me.unsubscribe)
}

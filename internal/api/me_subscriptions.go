package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
)

// GET /v1/me/tenants/{tenant_id}/subscriptions
func (h *meHandler) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")

	subs, err := h.subscriptions.ListByUserTenant(r.Context(), uid, tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list subscriptions failed")
		return
	}
	if subs == nil {
		subs = []usereventsubs.Subscription{}
	}
	writeJSON(w, http.StatusOK, subs)
}

// POST /v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}
func (h *meHandler) subscribe(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")
	eventType := chi.URLParam(r, "event_type")
	channelType := chi.URLParam(r, "channel_type")

	if err := h.subscriptions.Upsert(r.Context(), usereventsubs.Subscription{
		UserID:      uid,
		TenantID:    tenantID,
		EventType:   eventType,
		ChannelType: channelType,
		IsEnabled:   true,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "subscribe failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DELETE /v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}
func (h *meHandler) unsubscribe(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")
	eventType := chi.URLParam(r, "event_type")
	channelType := chi.URLParam(r, "channel_type")

	if err := h.subscriptions.Delete(r.Context(), uid, tenantID, eventType, channelType); err != nil {
		writeError(w, http.StatusInternalServerError, "unsubscribe failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

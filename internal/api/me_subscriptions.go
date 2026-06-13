package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/usertenant"
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

// PUT /v1/me/tenants/{tenant_id}/subscriptions/{event_type}/{channel_type}
func (h *meHandler) subscribe(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")
	eventType := chi.URLParam(r, "event_type")
	channelType := chi.URLParam(r, "channel_type")

	if !h.reg.HasTenant(tenantID) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if !h.reg.IsAllowed(tenantID, eventType, channelType) {
		writeError(w, http.StatusBadRequest, "event_type or channel_type not permitted for this tenant")
		return
	}

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

	// Auto-assign: if the user has no mapping for this channel type yet and
	// owns exactly one active channel of that type, wire it up automatically.
	ctx := r.Context()
	mappings, err := h.tenantMappings.ListByUser(ctx, uid, tenantID)
	if err == nil {
		alreadyMapped := false
		for _, m := range mappings {
			if m.ChannelType == channelType {
				alreadyMapped = true
				break
			}
		}
		if !alreadyMapped {
			channels, err := h.channels.ListByUser(ctx, uid)
			if err == nil {
				var candidates []int64
				for _, ch := range channels {
					if ch.ChannelType == channelType && ch.IsActive {
						candidates = append(candidates, ch.ID)
					}
				}
				if len(candidates) == 1 {
					_ = h.tenantMappings.Upsert(ctx, usertenant.Mapping{
						UserID:        uid,
						TenantID:      tenantID,
						ChannelType:   channelType,
						UserChannelID: candidates[0],
						IsEnabled:     true,
					})
				}
			}
		}
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

	if !h.reg.HasTenant(tenantID) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if !h.reg.IsAllowed(tenantID, eventType, channelType) {
		writeError(w, http.StatusBadRequest, "event_type or channel_type not permitted for this tenant")
		return
	}

	if err := h.subscriptions.Delete(r.Context(), uid, tenantID, eventType, channelType); err != nil {
		writeError(w, http.StatusInternalServerError, "unsubscribe failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

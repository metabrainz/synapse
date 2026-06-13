package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

// PUT /v1/me/tenants/{tenant_id}/channels/{channel_type}
func (h *meHandler) assignTenantChannel(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")
	channelType := chi.URLParam(r, "channel_type")

	var body struct {
		ChannelID int64 `json:"channel_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.ChannelID == 0 {
		writeError(w, http.StatusBadRequest, "channel_id required")
		return
	}

	if !h.reg.HasTenant(tenantID) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if !h.reg.HasChannelType(tenantID, channelType) {
		writeError(w, http.StatusBadRequest, "channel_type not supported for this tenant")
		return
	}

	ch, err := h.channels.GetByID(r.Context(), body.ChannelID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "assign channel failed")
		return
	}
	if ch == nil || ch.UserID != uid || ch.ChannelType != channelType {
		writeError(w, http.StatusNotFound, "channel not found")
		return
	}

	if err := h.tenantMappings.Upsert(r.Context(), usertenant.Mapping{
		UserID:        uid,
		TenantID:      tenantID,
		ChannelType:   channelType,
		UserChannelID: body.ChannelID,
		IsEnabled:     true,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "assign channel failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /v1/me/tenants/{tenant_id}/channels
func (h *meHandler) listTenantChannels(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")

	mappings, err := h.tenantMappings.ListByUser(r.Context(), uid, tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list tenant channels failed")
		return
	}
	if mappings == nil {
		mappings = []usertenant.Mapping{}
	}
	writeJSON(w, http.StatusOK, mappings)
}

// DELETE /v1/me/tenants/{tenant_id}/channels/{channel_type}
func (h *meHandler) removeTenantChannel(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")
	channelType := chi.URLParam(r, "channel_type")

	if err := h.tenantMappings.Delete(r.Context(), uid, tenantID, channelType); err != nil {
		writeError(w, http.StatusInternalServerError, "remove tenant channel failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

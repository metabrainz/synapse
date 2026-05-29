package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// GET /v1/me/tenants/{tenant_id}/event-types
func (h *meHandler) listTenantEventTypes(w http.ResponseWriter, r *http.Request) {
	if _, ok := requireUser(w, r); !ok {
		return
	}
	tenantID := chi.URLParam(r, "tenant_id")

	eventTypes := h.reg.EventTypes(tenantID)
	if eventTypes == nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}

	type eventTypeResponse struct {
		Name            string   `json:"name"`
		AllowedChannels []string `json:"allowed_channels"`
	}
	out := make([]eventTypeResponse, len(eventTypes))
	for i, et := range eventTypes {
		out[i] = eventTypeResponse{Name: et.Name, AllowedChannels: et.AllowedChannels}
	}
	writeJSON(w, http.StatusOK, out)
}

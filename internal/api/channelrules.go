package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store/tenantrules"
)

type channelRulesHandler struct {
	repo *tenantrules.Repo
}

func (h *channelRulesHandler) upsert(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")

	var body struct {
		EventType   string `json:"event_type"`
		ChannelType string `json:"channel_type"`
		IsAllowed   *bool  `json:"is_allowed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.EventType == "" || body.ChannelType == "" || body.IsAllowed == nil {
		writeError(w, http.StatusBadRequest, "event_type, channel_type, and is_allowed are required")
		return
	}

	if err := h.repo.Upsert(r.Context(), tenantrules.Rule{
		TenantID:    tenantID,
		EventType:   body.EventType,
		ChannelType: body.ChannelType,
		IsAllowed:   *body.IsAllowed,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to upsert channel rule")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *channelRulesHandler) list(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")

	rules, err := h.repo.ListByTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list channel rules")
		return
	}
	writeJSON(w, http.StatusOK, rules)
}

func (h *channelRulesHandler) delete(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")
	eventType := chi.URLParam(r, "event_type")
	channelType := chi.URLParam(r, "channel_type")

	if err := h.repo.Delete(r.Context(), tenantID, eventType, channelType); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete channel rule")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

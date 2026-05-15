package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store/eventtypes"
)

type eventTypesHandler struct {
	repo *eventtypes.Repo
}

func (h *eventTypesHandler) register(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")

	var body struct {
		EventType   string `json:"event_type"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.EventType == "" {
		writeError(w, http.StatusBadRequest, "event_type is required")
		return
	}

	if err := h.repo.Upsert(r.Context(), tenantID, body.EventType, body.Description); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to register event type")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"tenant_id": tenantID, "event_type": body.EventType})
}

func (h *eventTypesHandler) list(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")
	types, err := h.repo.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list event types")
		return
	}
	writeJSON(w, http.StatusOK, types)
}

func (h *eventTypesHandler) delete(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "id")
	eventType := chi.URLParam(r, "event_type")

	if err := h.repo.Delete(r.Context(), tenantID, eventType); err != nil {
		if err == eventtypes.ErrNotFound {
			writeError(w, http.StatusNotFound, "event type not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete event type")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

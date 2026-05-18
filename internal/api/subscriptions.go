package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/subscriptions"
)

type subscriptionsHandler struct {
	repo *subscriptions.Repo
}

func (h *subscriptionsHandler) create(w http.ResponseWriter, r *http.Request) {
	channelIDStr := chi.URLParam(r, "id")
	channelID, err := strconv.ParseInt(channelIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid channel id")
		return
	}

	var body struct {
		EventType string          `json:"event_type"`
		Config    json.RawMessage `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.EventType == "" {
		writeError(w, http.StatusBadRequest, "event_type is required")
		return
	}
	if body.Config == nil {
		body.Config = json.RawMessage(`{}`)
	}

	id, err := h.repo.Insert(r.Context(), subscriptions.Subscription{
		ChannelID: channelID,
		EventType: body.EventType,
		Config:    body.Config,
	})
	if err != nil {
		if store.IsUniqueViolation(err) {
			writeError(w, http.StatusConflict, "subscription already exists for this channel and event type")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create subscription")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"id": id})
}

func (h *subscriptionsHandler) list(w http.ResponseWriter, r *http.Request) {
	channelIDStr := chi.URLParam(r, "id")
	channelID, err := strconv.ParseInt(channelIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid channel id")
		return
	}

	list, err := h.repo.ListByChannel(r.Context(), channelID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list subscriptions")
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *subscriptionsHandler) delete(w http.ResponseWriter, r *http.Request) {
	channelIDStr := chi.URLParam(r, "id")
	channelID, err := strconv.ParseInt(channelIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid channel id")
		return
	}
	eventType := chi.URLParam(r, "event_type")

	if err := h.repo.Delete(r.Context(), channelID, eventType); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete subscription")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

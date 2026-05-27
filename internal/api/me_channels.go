package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/store/userchannels"
)

// GET /v1/me/channels
func (h *meHandler) listChannels(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}

	chans, err := h.channels.ListByUser(r.Context(), uid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list channels failed")
		return
	}
	if chans == nil {
		chans = []userchannels.UserChannel{}
	}
	writeJSON(w, http.StatusOK, chans)
}

// POST /v1/me/channels
func (h *meHandler) createChannel(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}

	var body struct {
		ChannelType string          `json:"channel_type"`
		Label       string          `json:"label"`
		Config      json.RawMessage `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.ChannelType == "" {
		writeError(w, http.StatusBadRequest, "channel_type required")
		return
	}
	if len(body.Config) == 0 {
		body.Config = json.RawMessage(`{}`)
	}

	id, err := h.channels.Insert(r.Context(), userchannels.UserChannel{
		UserID:      uid,
		ChannelType: body.ChannelType,
		Label:       body.Label,
		Config:      body.Config,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "create channel failed")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"id": id})
}

// DELETE /v1/me/channels/{id}
func (h *meHandler) deleteChannel(w http.ResponseWriter, r *http.Request) {
	uid, ok := requireUser(w, r)
	if !ok {
		return
	}

	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid channel id")
		return
	}
	if err := h.channels.Delete(r.Context(), uid, id); err != nil {
		if errors.Is(err, userchannels.ErrNotFound) {
			writeError(w, http.StatusNotFound, "channel not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "delete channel failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

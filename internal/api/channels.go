package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/channels"
)

type channelsHandler struct {
	repo *channels.Repo
	pool *pgxpool.Pool
}

func (h *channelsHandler) create(w http.ResponseWriter, r *http.Request) {
	tenant := middleware.TenantFromContext(r.Context())
	userID := chi.URLParam(r, "user_id")

	var body struct {
		Type   string          `json:"type"`
		Config json.RawMessage `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if body.Type == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}
	if body.Config == nil {
		body.Config = json.RawMessage(`{}`)
	}

	id, err := h.repo.Insert(r.Context(), h.pool, channels.Channel{
		TenantID: tenant.ID,
		UserID:   userID,
		Type:     body.Type,
		Config:   body.Config,
	})
	if err != nil {
		if store.IsUniqueViolation(err) {
			writeError(w, http.StatusConflict, "channel of this type already exists for user")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to create channel")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"id": id})
}

func (h *channelsHandler) list(w http.ResponseWriter, r *http.Request) {
	tenant := middleware.TenantFromContext(r.Context())
	userID := chi.URLParam(r, "user_id")

	list, err := h.repo.ListByUser(r.Context(), tenant.ID, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list channels")
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *channelsHandler) delete(w http.ResponseWriter, r *http.Request) {
	tenant := middleware.TenantFromContext(r.Context())
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid channel id")
		return
	}

	if err := h.repo.Delete(r.Context(), tenant.ID, id); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete channel")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

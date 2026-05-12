package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/store/deliveries"
)

type deliveriesHandler struct {
	pool *pgxpool.Pool
}

func (h *deliveriesHandler) listByEvent(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "event_id")
	eventID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event id")
		return
	}

	list, err := deliveries.ListByEvent(r.Context(), h.pool, eventID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list deliveries")
		return
	}
	writeJSON(w, http.StatusOK, list)
}

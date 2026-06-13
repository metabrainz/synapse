package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/store/deliveries"
)

type deliveriesHandler struct {
	pool *pgxpool.Pool
}

func (h *deliveriesHandler) listByEvent(w http.ResponseWriter, req *http.Request) {
	tenant := middleware.TenantFromContext(req.Context())

	idStr := chi.URLParam(req, "event_id")
	eventID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event id")
		return
	}

	list, found, err := deliveries.ListByEventForTenant(req.Context(), h.pool, eventID, tenant.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list deliveries")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}
	if list == nil {
		list = []deliveries.Delivery{}
	}
	writeJSON(w, http.StatusOK, list)
}

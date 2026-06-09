package api

import (
	"net/http"

	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

type meHandler struct {
	channels       *userchannels.Repo
	tenantMappings *usertenant.Repo
	subscriptions  *usereventsubs.Repo
	reg            *eventtype.Registry
}

func requireUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	uid := middleware.UserFromContext(r.Context())
	if uid == "" {
		writeError(w, http.StatusUnauthorized, "unauthenticated")
		return "", false
	}
	return uid, true
}

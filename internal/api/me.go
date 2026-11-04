package api

import (
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/store/notifications"
	"github.com/metabrainz/synapse/internal/store/preferences"
	"github.com/metabrainz/synapse/internal/store/userchannels"
	"github.com/metabrainz/synapse/internal/store/usereventsubs"
	"github.com/metabrainz/synapse/internal/store/usertenant"
)

type meHandler struct {
	channels       *userchannels.Repo
	tenantMappings *usertenant.Repo
	subscriptions  *usereventsubs.Repo
	notifications  *notifications.Repo
	preferences    *preferences.Repo
	reg            *eventtype.Registry
}

package schema

// KnownTenants declares all tenants, their event types, and which channel types Gate 1 permits.
var KnownTenants = []Tenant{
	{
		ID: "listenbrainz",
		EventTypes: []EventType{
			{Name: "listen", AllowedChannels: []string{"webhook", "telegram"}},
		},
	},
}

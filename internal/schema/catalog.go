package schema

// KnownTenants declares all tenants, their event types, and which channel types Gate 1 permits.
var KnownTenants = []Tenant{
	{
		ID: "listenbrainz",
		EventTypes: []EventType{
			{Name: "listen", AllowedChannels: []string{"webhook", "telegram"}},
			{Name: "recording_recommendation", AllowedChannels: []string{"webhook", "telegram"}},
			{Name: "personal_recording_recommendation", AllowedChannels: []string{"webhook", "telegram"}},
			{Name: "recording_pin", AllowedChannels: []string{"webhook", "telegram"}},
			{Name: "thanks", AllowedChannels: []string{"webhook", "telegram"}},
			{Name: "follow", AllowedChannels: []string{"webhook", "telegram"}},
			{Name: "notification", AllowedChannels: []string{"webhook", "telegram"}},
			{Name: "cb_review", AllowedChannels: []string{"webhook", "telegram"}},
		},
	},
}

package schema

// KnownTenants is the authoritative list of tenants and their event types.
// Add a new entry here and drop a matching JSON schema file under
// schemas/{tenant_id}/{event_type}.json to register a new event type.
var KnownTenants = []Tenant{
	{
		ID: "listenbrainz",
		EventTypes: []EventType{
			{Name: "listen"},
		},
	},
}

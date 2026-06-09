package eventtype

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
	"github.com/metabrainz/synapse/internal/eventtype/listenbrainz"
)

// Tenant is a static tenant definition. APIKey is injected at startup from config.
type Tenant struct {
	ID     string
	APIKey string
	Events []eventspec.EventType
}

var KnownTenants = []Tenant{
	{ID: "listenbrainz", Events: listenbrainz.All},
}

// Registry validates payloads and answers routing questions for known tenants.
type Registry struct {
	schemas map[string]*jsonschema.Schema // "tenantID:eventName" → compiled schema
	tenants map[string]*Tenant            // tenantID → tenant
	byKey   map[string]*Tenant            // apiKey → tenant
}

// NewRegistry panics on any misconfiguration — fail fast, not at runtime.
func NewRegistry(tenants []Tenant) *Registry {
	ValidateRegistry(tenants)

	compiler := jsonschema.NewCompiler()
	schemas := make(map[string]*jsonschema.Schema)
	byID := make(map[string]*Tenant, len(tenants))
	byKey := make(map[string]*Tenant, len(tenants))

	for i := range tenants {
		tenant := &tenants[i]
		byID[tenant.ID] = tenant
		if tenant.APIKey != "" {
			byKey[tenant.APIKey] = tenant
		}
		for _, event := range tenant.Events {
			schemaKey := fmt.Sprintf("%s/%s.json", tenant.ID, event.EventName())
			if err := compiler.AddResource(schemaKey, bytes.NewReader(event.Schema())); err != nil {
				panic(fmt.Sprintf("eventtype: [%s/%s] invalid schema: %v", tenant.ID, event.EventName(), err))
			}
			compiled, err := compiler.Compile(schemaKey)
			if err != nil {
				panic(fmt.Sprintf("eventtype: [%s/%s] compile schema: %v", tenant.ID, event.EventName(), err))
			}
			schemas[tenant.ID+":"+event.EventName()] = compiled
		}
	}
	return &Registry{schemas: schemas, tenants: byID, byKey: byKey}
}

func ValidateRegistry(tenants []Tenant) {
	seenTenants := make(map[string]bool)
	for _, tenant := range tenants {
		if seenTenants[tenant.ID] {
			panic(fmt.Sprintf("eventtype: duplicate tenant %q", tenant.ID))
		}
		seenTenants[tenant.ID] = true

		seenEvents := make(map[string]bool)
		for _, event := range tenant.Events {
			name := event.EventName()
			if name == "" {
				panic(fmt.Sprintf("eventtype: tenant %q has event with empty EventName", tenant.ID))
			}
			if seenEvents[name] {
				panic(fmt.Sprintf("eventtype: tenant %q has duplicate event %q", tenant.ID, name))
			}
			seenEvents[name] = true
			if len(event.Schema()) == 0 {
				panic(fmt.Sprintf("eventtype: [%s/%s] Schema() is empty", tenant.ID, name))
			}
			for _, channel := range event.AllowedChannels() {
				if channel == eventspec.ChannelTelegram {
					tr, ok := event.(eventspec.TelegramRenderer)
					if !ok {
						panic(fmt.Sprintf("eventtype: [%s/%s] lists %q but does not implement TelegramRenderer", tenant.ID, name, eventspec.ChannelTelegram))
					}
					if tr.Telegram() == "" {
						panic(fmt.Sprintf("eventtype: [%s/%s] TelegramRenderer.Telegram() is empty", tenant.ID, name))
					}
				}
			}
		}
	}
}

// Validate returns nil for unknown tenants/events (they pass through unvalidated).
func (r *Registry) Validate(tenantID, eventType string, payload json.RawMessage) error {
	schema, ok := r.schemas[tenantID+":"+eventType]
	if !ok {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return schema.Validate(decoded)
}

func (r *Registry) Has(tenantID, eventType string) bool {
	_, ok := r.schemas[tenantID+":"+eventType]
	return ok
}

func (r *Registry) IsAllowed(tenantID, eventType, channelType string) bool {
	event, ok := r.Lookup(tenantID, eventType)
	if !ok {
		return false
	}
	return slices.Contains(event.AllowedChannels(), channelType)
}

func (r *Registry) AllowedChannels(tenantID, eventType string) []string {
	event, ok := r.Lookup(tenantID, eventType)
	if !ok {
		return nil
	}
	return event.AllowedChannels()
}

func (r *Registry) HasTenant(tenantID string) bool {
	_, ok := r.tenants[tenantID]
	return ok
}

func (r *Registry) HasChannelType(tenantID, channelType string) bool {
	tenant, ok := r.tenants[tenantID]
	if !ok {
		return false
	}
	for _, event := range tenant.Events {
		if slices.Contains(event.AllowedChannels(), channelType) {
			return true
		}
	}
	return false
}

func (r *Registry) EventTypes(tenantID string) []eventspec.EventType {
	tenant, ok := r.tenants[tenantID]
	if !ok {
		return nil
	}
	return tenant.Events
}

func (r *Registry) Lookup(tenantID, eventType string) (eventspec.EventType, bool) {
	tenant, ok := r.tenants[tenantID]
	if !ok {
		return nil, false
	}
	for _, event := range tenant.Events {
		if event.EventName() == eventType {
			return event, true
		}
	}
	return nil, false
}

func (r *Registry) RangeEvents(fn func(tenantID string, event eventspec.EventType)) {
	for tenantID, tenant := range r.tenants {
		for _, event := range tenant.Events {
			fn(tenantID, event)
		}
	}
}

func (r *Registry) LookupByAPIKey(apiKey string) (*Tenant, bool) {
	tenant, ok := r.byKey[apiKey]
	return tenant, ok
}

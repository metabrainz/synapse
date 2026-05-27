// Package schema provides a static registry of event type JSON schemas.
// Schemas are embedded at compile time — adding a new event type requires
// a new JSON file and a registry entry, no DB migration.
package schema

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

//go:embed schemas/**/*.json
var schemaFS embed.FS

// EventType pairs a name with its compiled validator and the channel types Gate 1 permits.
type EventType struct {
	Name            string
	AllowedChannels []string
}

// Tenant is a static tenant definition. APIKey is injected at startup from config.
type Tenant struct {
	ID         string
	APIKey     string
	EventTypes []EventType
}

// Registry validates payloads and answers routing questions for known tenants.
type Registry struct {
	schemas map[string]*jsonschema.Schema // "tenantID:eventType" → compiled schema
	tenants map[string]*Tenant            // tenantID → tenant
	byKey   map[string]*Tenant            // apiKey → tenant
}

// New builds a Registry from the provided tenant definitions.
// Schemas are loaded from the embedded FS at path schemas/{tenant.ID}/{et.Name}.json.
// Panics at startup if any schema file is missing or malformed — fail fast, not at runtime.
func New(tenants []Tenant) *Registry {
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
		for _, eventType := range tenant.EventTypes {
			path := fmt.Sprintf("schemas/%s/%s.json", tenant.ID, eventType.Name)
			data, err := schemaFS.ReadFile(path)
			if err != nil {
				panic(fmt.Sprintf("schema registry: missing schema file %s: %v", path, err))
			}
			if err := compiler.AddResource(path, bytes.NewReader(data)); err != nil {
				panic(fmt.Sprintf("schema registry: invalid schema %s: %v", path, err))
			}
			compiled, err := compiler.Compile(path)
			if err != nil {
				panic(fmt.Sprintf("schema registry: compile %s: %v", path, err))
			}
			schemas[tenant.ID+":"+eventType.Name] = compiled
		}
	}
	return &Registry{schemas: schemas, tenants: byID, byKey: byKey}
}

// Validate checks payload against the registered schema for (tenantID, eventType).
// Returns nil if valid or if no schema is registered (unknown tenants/events pass through).
// Returns a descriptive error with field-level detail on validation failure.
func (r *Registry) Validate(tenantID, eventType string, payload json.RawMessage) error {
	s, ok := r.schemas[tenantID+":"+eventType]
	if !ok {
		return nil
	}
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return s.Validate(v)
}

// Has reports whether (tenantID, eventType) is registered.
func (r *Registry) Has(tenantID, eventType string) bool {
	_, ok := r.schemas[tenantID+":"+eventType]
	return ok
}

// IsAllowed reports whether channelType passes Gate 1 for (tenantID, eventType).
func (r *Registry) IsAllowed(tenantID, eventType, channelType string) bool {
	t, ok := r.tenants[tenantID]
	if !ok {
		return false
	}
	for _, et := range t.EventTypes {
		if et.Name == eventType {
			return slices.Contains(et.AllowedChannels, channelType)
		}
	}
	return false
}

// AllowedChannels returns the channel types Gate 1 permits for (tenantID, eventType).
func (r *Registry) AllowedChannels(tenantID, eventType string) []string {
	t, ok := r.tenants[tenantID]
	if !ok {
		return nil
	}
	for _, et := range t.EventTypes {
		if et.Name == eventType {
			return et.AllowedChannels
		}
	}
	return nil
}

// LookupByAPIKey returns the tenant for the given API key, or false if unknown.
func (r *Registry) LookupByAPIKey(apiKey string) (*Tenant, bool) {
	t, ok := r.byKey[apiKey]
	return t, ok
}

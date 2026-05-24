// Package schema provides a static registry of event type JSON schemas.
// Schemas are embedded at compile time — adding a new event type requires
// a new JSON file and a registry entry, no DB migration.
package schema

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

//go:embed schemas/**/*.json
var schemaFS embed.FS

// EventType pairs an event type name with its compiled validator.
type EventType struct {
	Name string
}

// Tenant groups event types by tenant ID.
type Tenant struct {
	ID         string
	EventTypes []EventType
}

// Registry validates payloads for known (tenant, event_type) pairs.
type Registry struct {
	// m maps "tenantID:eventType" → compiled schema.
	m map[string]*jsonschema.Schema
}

// New builds a Registry from the provided tenant definitions.
// Schemas are loaded from the embedded FS at path schemas/{tenant.ID}/{et.Name}.json.
// Panics at startup if any schema file is missing or malformed — fail fast, not at runtime.
func New(tenants []Tenant) *Registry {
	compiler := jsonschema.NewCompiler()
	m := make(map[string]*jsonschema.Schema)

	for _, t := range tenants {
		for _, et := range t.EventTypes {
			path := fmt.Sprintf("schemas/%s/%s.json", t.ID, et.Name)
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
			m[t.ID+":"+et.Name] = compiled
		}
	}
	return &Registry{m: m}
}

// Validate checks payload against the registered schema for (tenantID, eventType).
// Returns nil if valid or if no schema is registered (unknown tenants/events pass through).
// Returns a descriptive error with field-level detail on validation failure.
func (r *Registry) Validate(tenantID, eventType string, payload json.RawMessage) error {
	s, ok := r.m[tenantID+":"+eventType]
	if !ok {
		return nil
	}
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return s.Validate(v)
}

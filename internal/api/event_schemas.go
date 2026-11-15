package api

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/danielgtaylor/huma/v2"

	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

// addEventSchemas publishes each event's payload schema as the component
// "<tenant>.<event_type>", so tenants can generate payload types from /openapi.json.
func addEventSchemas(spec *huma.OpenAPI, reg *eventtype.Registry) {
	schemas := spec.Components.Schemas.Map()
	reg.RangeEvents(func(tenantID string, event eventspec.EventType) {
		name := tenantID + "." + event.EventName()
		// Refs in a component resolve from the spec's root, not the schema's.
		raw := bytes.ReplaceAll(event.Schema(), []byte(`"#/$defs/`), []byte(`"#/components/schemas/`+name+`/$defs/`))
		var schema map[string]any
		if err := json.Unmarshal(raw, &schema); err != nil {
			panic(fmt.Sprintf("api: [%s] schema: %v", name, err))
		}
		delete(schema, "$schema")
		// huma.Schema has no $defs field; Extensions are marshalled inline, so nothing is lost.
		schemas[name] = &huma.Schema{Extensions: schema}
	})
}

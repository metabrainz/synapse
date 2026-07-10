package api

import (
	"encoding/json"

	"github.com/danielgtaylor/huma/v2"
	"github.com/metabrainz/synapse/internal/adapter"
)

// JSONObject is an arbitrary JSON object. Backed by json.RawMessage; Huma
// renders it as {"type": "object"} in the OpenAPI schema instead of the default
// empty schema ({}) that json.RawMessage produces.
type JSONObject json.RawMessage

func (JSONObject) Schema(_ huma.Registry) *huma.Schema {
	return &huma.Schema{Type: "object"}
}

func (j JSONObject) MarshalJSON() ([]byte, error) {
	if j == nil {
		return []byte("null"), nil
	}
	return []byte(j), nil
}

func (j *JSONObject) UnmarshalJSON(data []byte) error {
	*j = append((*j)[0:0], data...)
	return nil
}

// ChannelType is a string constrained to the registered channel types.
// Huma renders it as an enum in the OpenAPI schema, populated from
// adapter.Registry at route-registration time (after adapter.Build).
type ChannelType string

func (ChannelType) Schema(_ huma.Registry) *huma.Schema {
	registered := adapter.ChannelTypes()
	enum := make([]any, len(registered))
	for i, t := range registered {
		enum[i] = string(t)
	}
	return &huma.Schema{Type: "string", Enum: enum}
}

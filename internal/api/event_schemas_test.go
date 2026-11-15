package api

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/danielgtaylor/huma/v2/humatest"
	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
)

// The published schemas must accept and reject exactly what ingest validation does.
func TestEventSchemasMatchValidation(t *testing.T) {
	_, api := humatest.New(t)
	reg := eventtype.NewRegistry(eventtype.KnownTenants)
	addEventSchemas(api.OpenAPI(), reg)
	spec, err := json.Marshal(api.OpenAPI())
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.Draft = jsonschema.Draft2020
	if err := c.AddResource("openapi.json", bytes.NewReader(spec)); err != nil {
		t.Fatal(err)
	}

	payloads := []string{
		`{}`,
		`{"actor": {"username": "user-a", "url": "https://listenbrainz.org/user/user-a"}}`,
		`{"actor": {"username": "user-a", "url": "u"}, "message": "hi"}`,
		`{"actor": {"username": "", "url": "u"}, "message": "hi"}`,
		`{"actor": {"username": "user-a", "url": "u"}, "recording": {"track_name": "t", "artist_name": "a"}}`,
		`{"actor": {"username": "user-a", "url": "u"}, "recording": {"track_name": "t", "artist_name": "a", "album_art_url": "x"}}`,
		`{"actor": {"username": "user-a", "url": "u"}, "recording": {"track_name": "t", "artist_name": "a", "extra": 1}}`,
		`{"actor": {"username": "user-a", "url": "u"}, "entity": {"id": "1", "name": "n", "type": "review", "url": "u"}}`,
	}
	checked := 0
	reg.RangeEvents(func(tenantID string, event eventspec.EventType) {
		name := tenantID + "." + event.EventName()
		published, err := c.Compile("openapi.json#/components/schemas/" + name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for _, p := range payloads {
			var v any
			_ = json.Unmarshal([]byte(p), &v)
			want := reg.Validate(tenantID, event.EventName(), json.RawMessage(p)) == nil
			if got := published.Validate(v) == nil; got != want {
				t.Errorf("%s accepts %s: %v, ingest: %v", name, p, got, want)
			}
		}
		checked++
	})
	if checked == 0 {
		t.Fatal("no events checked")
	}
}

package eventview

import "encoding/json"

type View struct {
	Actor     Actor      `json:"actor"`
	Recording *Recording `json:"recording,omitempty"`
	Entity    *Entity    `json:"entity,omitempty"`
	Message   string     `json:"message,omitempty"`
}

type Actor struct {
	Username string `json:"username"`
	URL      string `json:"url"`
}

type Recording struct {
	MBID        string `json:"mbid,omitempty"`
	TrackName   string `json:"track_name"`
	ArtistName  string `json:"artist_name"`
	ReleaseName string `json:"release_name,omitempty"`
	URL         string `json:"url,omitempty"`
	AlbumArtURL string `json:"album_art_url,omitempty"`
}

type Entity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
	URL  string `json:"url"`
}

// Extract unmarshals a schema-validated payload into a View.
// Safe to call without per-event-type switching because ingest
// already validated the payload against the event's JSON schema.
func Extract(payload json.RawMessage) (View, error) {
	var v View
	err := json.Unmarshal(payload, &v)
	return v, err
}

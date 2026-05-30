package telegram

import (
	"bytes"
	"encoding/json"
	"text/template"

	"github.com/metabrainz/synapse/internal/fanout"
)

// eventTemplates maps event_type to a Telegram message template string.
// Templates have access to: .TenantID, .UserID, .EventType, .Payload (map[string]any),
// .RawPayload (pretty-printed JSON fallback).
// Use {{get .Payload "key"}} or {{get .Payload "nested" "key"}} for deep map access.
//
// To add a new event type: add one entry here. No other changes needed.
var eventTemplates = map[string]string{
	"listen": "🎵 New listen: {{get .Payload \"track_metadata\" \"track_name\"}} by {{get .Payload \"track_metadata\" \"artist_name\"}}",
}

const defaultTemplate = "[{{.TenantID}}] {{.EventType}}\n\n{{.RawPayload}}"

// templateFuncs are available in all event templates.
var templateFuncs = template.FuncMap{
	// get traverses nested map[string]any by successive keys.
	// Returns nil if any key is missing or the value is not a map.
	"get": func(m map[string]any, keys ...string) any {
		var cur any = m
		for _, k := range keys {
			mm, ok := cur.(map[string]any)
			if !ok {
				return nil
			}
			cur = mm[k]
		}
		return cur
	},
}

// compiled holds pre-parsed templates, keyed by event type.
// Compiled once at init — a bad template panics at startup, not at delivery time.
var compiled map[string]*template.Template
var compiledDefault *template.Template

func init() {
	compiled = make(map[string]*template.Template, len(eventTemplates))
	for et, src := range eventTemplates {
		compiled[et] = template.Must(template.New(et).Funcs(templateFuncs).Parse(src))
	}
	compiledDefault = template.Must(template.New("default").Funcs(templateFuncs).Parse(defaultTemplate))
}

type tmplData struct {
	TenantID   string
	UserID     string
	EventType  string
	Payload    map[string]any
	RawPayload string
}

func renderMessage(msg fanout.WorkerMessage) string {
	var payload map[string]any
	if err := json.Unmarshal(msg.Payload, &payload); err != nil || payload == nil {
		payload = map[string]any{}
	}
	pretty, _ := json.MarshalIndent(msg.Payload, "", "  ")

	data := tmplData{
		TenantID:   msg.TenantID,
		UserID:     msg.UserID,
		EventType:  msg.EventType,
		Payload:    payload,
		RawPayload: string(pretty),
	}

	tmpl, ok := compiled[msg.EventType]
	if !ok {
		tmpl = compiledDefault
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		// Execution failure (e.g. nil value in template): fall back to raw payload
		return data.RawPayload
	}
	return buf.String()
}

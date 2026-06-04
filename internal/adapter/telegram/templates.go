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
// Use {{get .Payload "key"}} or {{get .Payload "key1" "key2"}} for nested access.
// Use {{str .Payload "key"}} for safe string access (returns "" if missing or not a string).
//
// To add a new event type: add one entry here. No other changes needed.
var eventTemplates = map[string]string{
	"listen": `🎵 {{str .Payload "actor" "username"}} listened to {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}`,

	"recording_recommendation": `🎵 {{str .Payload "actor" "username"}} recommended {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}`,

	"personal_recording_recommendation": `💌 {{str .Payload "actor" "username"}} recommended {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}} to you{{with str .Payload "message"}}

"{{.}}"{{end}}`,

	"recording_pin": `📌 {{str .Payload "actor" "username"}} pinned {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}{{with str .Payload "message"}}

"{{.}}"{{end}}`,

	"thanks": `🙏 {{str .Payload "actor" "username"}} thanked you for recommending {{str .Payload "recording" "track_name"}} by {{str .Payload "recording" "artist_name"}}{{with str .Payload "message"}}

"{{.}}"{{end}}`,

	"follow": `👤 {{str .Payload "actor" "username"}} started following you`,

	"notification": `🔔 {{str .Payload "message"}}`,

	"cb_review": `📝 {{str .Payload "actor" "username"}} reviewed {{str .Payload "entity" "name"}}`,
}

const defaultTemplate = "[{{.TenantID}}] {{.EventType}}\n\n{{.RawPayload}}"

var templateFuncs = template.FuncMap{
	// get traverses nested map[string]any by successive keys, returning nil if any step is missing.
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
	// str is like get but returns a string (empty string if missing or not a string).
	"str": func(m map[string]any, keys ...string) string {
		var cur any = m
		for _, k := range keys {
			mm, ok := cur.(map[string]any)
			if !ok {
				return ""
			}
			cur = mm[k]
		}
		s, _ := cur.(string)
		return s
	},
}

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
		return data.RawPayload
	}
	return buf.String()
}

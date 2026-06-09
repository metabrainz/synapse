package telegram

import (
	"bytes"
	"encoding/json"
	"text/template"

	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/eventtype/eventspec"
	"github.com/metabrainz/synapse/internal/eventtype/tmpl"
	"github.com/metabrainz/synapse/internal/fanout"
)

// defaultTemplate is the fallback for events without a TelegramRenderer (e.g. stale queue messages).
const defaultTemplate = "[{{.TenantID}}] {{.EventType}}\n\n{{.RawPayload}}"

type tmplData struct {
	TenantID   string
	UserID     string
	EventType  string
	Payload    map[string]any
	RawPayload string
}

type renderer struct {
	compiled map[string]*template.Template
	def      *template.Template
}

func newRenderer(reg *eventtype.Registry) *renderer {
	r := &renderer{
		compiled: make(map[string]*template.Template),
		def:      template.Must(template.New("default").Funcs(tmpl.FuncMap).Parse(defaultTemplate)),
	}
	reg.RangeEvents(func(tenantID string, event eventspec.EventType) {
		tr, ok := event.(eventspec.TelegramRenderer)
		if !ok {
			return
		}
		key := tenantID + ":" + event.EventName()
		r.compiled[key] = template.Must(template.New(key).Funcs(tmpl.FuncMap).Parse(tr.Telegram()))
	})
	return r
}

func (r *renderer) render(msg fanout.WorkerMessage) string {
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

	tpl, ok := r.compiled[msg.TenantID+":"+msg.EventType]
	if !ok {
		tpl = r.def
	}

	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return data.RawPayload
	}
	return buf.String()
}

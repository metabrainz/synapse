package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/live"
	"github.com/metabrainz/synapse/internal/store/preferences"
)

// streamWriteTimeout bounds each frame write, so a client that stops reading
// releases its goroutine and stream slot.
const streamWriteTimeout = 10 * time.Second

// streamHandler serves the inbox SSE streams. They are plain chi routes because
// huma can't clear the write deadline or flush per event.
type streamHandler struct {
	hub      *live.Hub
	prefs    *preferences.Repo
	reg      *eventtype.Registry
	origins  map[string][]string
	ping     time.Duration
	lifetime time.Duration
}

// GET /v1/stream/tenants/{tenant_id}/notifications
func (h *streamHandler) tenant(w http.ResponseWriter, r *http.Request) {
	tenant := chi.URLParam(r, "tenant_id")
	if !h.reg.HasTenant(tenant) {
		writeError(w, http.StatusNotFound, "unknown tenant")
		return
	}
	h.serve(w, r, middleware.UserFromContext(r.Context()), tenant)
}

// GET /v1/stream/notifications
func (h *streamHandler) global(w http.ResponseWriter, r *http.Request) {
	uid := middleware.UserFromContext(r.Context())
	p, err := h.prefs.Get(r.Context(), uid)
	if err != nil {
		slog.Error("stream: load preferences", "user", uid, "err", err)
		begin(w)
		return
	}
	if !p.CrossTenantNotifications {
		writeError(w, http.StatusForbidden, "cross-tenant notifications are off")
		return
	}
	h.serve(w, r, uid, "")
}

// allowOrigin runs before auth so every response, 401 included, is readable by an allowed page.
func (h *streamHandler) allowOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.cors(w, r, chi.URLParam(r, "tenant_id"))
		next.ServeHTTP(w, r)
	})
}

// preflight answers the CORS preflight that the Authorization header triggers.
func (h *streamHandler) preflight(w http.ResponseWriter, _ *http.Request) {
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		w.Header().Set("Access-Control-Allow-Methods", "GET")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization")
		w.Header().Set("Access-Control-Max-Age", "600")
	}
	w.WriteHeader(http.StatusNoContent)
}

// cors allows the origins of tenant, or of every tenant when tenant is "".
func (h *streamHandler) cors(w http.ResponseWriter, r *http.Request, tenant string) {
	w.Header().Add("Vary", "Origin")
	origin := r.Header.Get("Origin")
	for id, allowed := range h.origins {
		if (tenant == "" || id == tenant) && slices.Contains(allowed, origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			return
		}
	}
}

// serve streams uid's nudges, only those for tenant unless tenant is "".
func (h *streamHandler) serve(w http.ResponseWriter, r *http.Request, uid, tenant string) {
	nudges, unregister, err := h.hub.Register(r.Context(), uid)
	if errors.Is(err, live.ErrTooManyStreams) {
		writeError(w, http.StatusTooManyRequests, "too many open streams")
		return
	}
	if err != nil {
		if !errors.Is(err, live.ErrHubClosed) && r.Context().Err() == nil {
			slog.Error("stream: register", "user", uid, "err", err)
		}
		begin(w)
		return
	}
	defer unregister()

	rc, err := begin(w)
	if err != nil {
		return
	}
	ping := time.NewTicker(h.ping)
	defer ping.Stop()
	// Expiry makes the client reconnect, which re-checks its OAuth token and
	// preferences. Jitter keeps streams opened together from expiring together.
	expire := time.NewTimer(h.lifetime + rand.N(h.lifetime/5))
	defer expire.Stop()
	for {
		var event string
		var data []byte
		select {
		case <-r.Context().Done():
			return
		case <-expire.C:
			return
		case n, ok := <-nudges:
			if !ok {
				return
			}
			if tenant != "" && n.Tenant != tenant {
				continue
			}
			event = "inbox"
			data, _ = json.Marshal(n)
		case <-ping.C:
			event, data = "ping", []byte("{}")
		}
		if err := writeFrame(w, rc, fmt.Sprintf("event: %s\ndata: %s\n\n", event, data)); err != nil {
			return
		}
	}
}

// begin opens the event stream.
func begin(w http.ResponseWriter) (*http.ResponseController, error) {
	hdr := w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(w)
	return rc, writeFrame(w, rc, "retry: 5000\n\n")
}

// writeFrame replaces the server's WriteTimeout, which would cut every stream
// after 30s, with a deadline per frame.
func writeFrame(w http.ResponseWriter, rc *http.ResponseController, frame string) error {
	if err := rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil {
		slog.Error("stream: set write deadline", "err", err)
		return err
	}
	if _, err := io.WriteString(w, frame); err != nil {
		return err
	}
	return rc.Flush()
}

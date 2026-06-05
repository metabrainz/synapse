package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ingest"
	"github.com/metabrainz/synapse/internal/schema"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/events"
)

type ingestHandler struct {
	pool    *pgxpool.Pool
	fan     *fanout.Fanout
	deduper *dedup.Deduper
	reg     *schema.Registry
}

type ingestRequest struct {
	Recipients     []string        `json:"recipients"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key"`
}

type ingestResponse struct {
	EventID       int64 `json:"event_id"`
	DeliveryCount int   `json:"delivery_count"`
}

func (h *ingestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenant := middleware.TenantFromContext(r.Context())

	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.EventType == "" {
		writeError(w, http.StatusBadRequest, "event_type is required")
		return
	}
	recipients := ingest.DedupeRecipients(req.Recipients)
	if len(recipients) == 0 {
		writeError(w, http.StatusBadRequest, "recipients must contain at least one user_id")
		return
	}
	if len(recipients) > ingest.MaxRecipientsPerEvent {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("recipients exceeds max of %d", ingest.MaxRecipientsPerEvent))
		return
	}
	if req.Payload == nil {
		req.Payload = json.RawMessage(`{}`)
	}

	// Reject unknown (tenant, event_type) pairs before touching the DB.
	if !h.reg.Has(tenant.ID, req.EventType) {
		writeError(w, http.StatusBadRequest, "event type not registered for this tenant")
		return
	}

	// Validate payload against the registered schema for this (tenant, event_type).
	if err := h.reg.Validate(tenant.ID, req.EventType, req.Payload); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("payload validation failed: %s", err))
		return
	}

	ctx := r.Context()

	// Dry-run: preview matching channels without writing anything.
	if r.URL.Query().Get("dry_run") == "true" {
		channels, err := h.fan.Preview(ctx, tenant.ID, req.EventType, recipients)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "fanout preview failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"dry_run":        true,
			"delivery_count": channels,
		})
		return
	}

	// Step 1: Redis idempotency pre-check (fail-open — deduper returns false on Redis error).
	if req.IdempotencyKey != nil {
		seen, _ := h.deduper.SeenIdempotency(ctx, tenant.ID, *req.IdempotencyKey)
		if seen {
			writeJSON(w, http.StatusOK, map[string]any{"deduplicated": true})
			return
		}
	}

	// Step 2: Atomic transaction — event + deliveries + outbox.
	var eventID int64
	var deliveryCount int
	err := store.WithTx(ctx, h.pool, func(q store.Querier) error {
		ev := events.Event{
			TenantID:       tenant.ID,
			EventType:      req.EventType,
			Recipients:     recipients,
			Payload:        req.Payload,
			IdempotencyKey: req.IdempotencyKey,
		}

		ev, err := events.Insert(ctx, q, ev)
		if err != nil {
			return err
		}
		eventID = ev.ID

		count, err := h.fan.Fan(ctx, q, ev)
		if err != nil {
			return err
		}
		deliveryCount = count
		return nil
	})
	if err != nil {
		// PG unique constraint on idempotency_key — treat as deduplicated.
		if store.IsUniqueViolation(err) {
			writeJSON(w, http.StatusOK, map[string]any{"deduplicated": true})
			return
		}
		// Suppress noise from clients that cancelled mid-request
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "event ingestion failed", "tenant", tenant.ID, "err", err)
		}
		writeError(w, http.StatusInternalServerError, "event ingestion failed")
		return
	}

	// Step 3: Mark idempotency key in Redis AFTER commit (set before commit risks
	// losing the event if we crash between SET and INSERT).
	if req.IdempotencyKey != nil {
		h.deduper.MarkIdempotency(ctx, tenant.ID, *req.IdempotencyKey)
	}

	writeJSON(w, http.StatusAccepted, ingestResponse{
		EventID:       eventID,
		DeliveryCount: deliveryCount,
	})
}

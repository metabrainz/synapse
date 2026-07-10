package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/danielgtaylor/huma/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/metabrainz/synapse/internal/api/middleware"
	"github.com/metabrainz/synapse/internal/dedup"
	"github.com/metabrainz/synapse/internal/eventtype"
	"github.com/metabrainz/synapse/internal/fanout"
	"github.com/metabrainz/synapse/internal/ingest"
	"github.com/metabrainz/synapse/internal/store"
	"github.com/metabrainz/synapse/internal/store/events"
)

type ingestHandler struct {
	pool    *pgxpool.Pool
	fan     *fanout.Fanout
	deduper *dedup.Deduper
	reg     *eventtype.Registry
}

type ingestRequestBody struct {
	Recipients     []string   `json:"recipients" doc:"User IDs to deliver this event to" minItems:"1"`
	EventType      string     `json:"event_type" doc:"Tenant-specific event type identifier" minLength:"1"`
	Payload        JSONObject `json:"payload,omitempty" doc:"Event-specific payload; shape is defined per event type"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty" doc:"Client-supplied deduplication key"`
}

type ingestInput struct {
	DryRun bool              `query:"dry_run" doc:"Preview matching channels without writing anything"`
	Body   ingestRequestBody `required:"true"`
}

type ingestResponseBody struct {
	EventID       *int64 `json:"event_id,omitempty" doc:"ID of the created event"`
	DeliveryCount *int   `json:"delivery_count,omitempty" doc:"Number of deliveries queued"`
	DryRun        bool   `json:"dry_run,omitempty" doc:"True when this was a dry-run preview"`
	Deduplicated  bool   `json:"deduplicated,omitempty" doc:"True when this request matched a prior idempotency key"`
}

type ingestOutput struct {
	Status int // 202 Accepted normally; 200 for dry-run or deduplicated responses
	Body   ingestResponseBody
}

func (h *ingestHandler) handle(ctx context.Context, input *ingestInput) (*ingestOutput, error) {
	tenant := middleware.TenantFromContext(ctx)
	if tenant == nil {
		return nil, huma.Error401Unauthorized("unauthorized")
	}

	recipients := ingest.DedupeRecipients(input.Body.Recipients)
	if len(recipients) == 0 {
		return nil, huma.Error400BadRequest("recipients must contain at least one user_id")
	}
	if len(recipients) > ingest.MaxRecipientsPerEvent {
		return nil, huma.Error400BadRequest(fmt.Sprintf("recipients exceeds max of %d", ingest.MaxRecipientsPerEvent))
	}
	if len(input.Body.Payload) == 0 {
		input.Body.Payload = JSONObject(`{}`)
	}

	if !h.reg.Has(tenant.ID, input.Body.EventType) {
		return nil, huma.Error400BadRequest("event type not registered for this tenant")
	}
	if err := h.reg.Validate(tenant.ID, input.Body.EventType, json.RawMessage(input.Body.Payload)); err != nil {
		return nil, huma.Error400BadRequest(fmt.Sprintf("payload validation failed: %s", err))
	}

	// Dry-run: preview matching channels without writing anything.
	if input.DryRun {
		count, err := h.fan.Preview(ctx, tenant.ID, input.Body.EventType, recipients)
		if err != nil {
			return nil, huma.Error500InternalServerError("fanout preview failed")
		}
		return &ingestOutput{Status: 200, Body: ingestResponseBody{DryRun: true, DeliveryCount: &count}}, nil
	}

	// Step 1: Redis idempotency pre-check (fail-open — deduper returns false on Redis error).
	if input.Body.IdempotencyKey != nil {
		seen, _ := h.deduper.SeenIdempotency(ctx, tenant.ID, *input.Body.IdempotencyKey)
		if seen {
			return h.deduplicated(ctx, tenant.ID, *input.Body.IdempotencyKey)
		}
	}

	// Step 2: Atomic transaction — event + deliveries + outbox.
	var eventID int64
	var deliveryCount int
	err := store.WithTx(ctx, h.pool, func(q store.Querier) error {
		ev := events.Event{
			TenantID:       tenant.ID,
			EventType:      input.Body.EventType,
			Recipients:     recipients,
			Payload:        json.RawMessage(input.Body.Payload),
			IdempotencyKey: input.Body.IdempotencyKey,
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
		if store.IsUniqueViolation(err) && input.Body.IdempotencyKey != nil {
			return h.deduplicated(ctx, tenant.ID, *input.Body.IdempotencyKey)
		}
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "event ingestion failed", "tenant", tenant.ID, "err", err)
		}
		return nil, huma.Error500InternalServerError("event ingestion failed")
	}

	// Step 3: Mark idempotency key in Redis AFTER commit (set before commit risks
	// losing the event if we crash between SET and INSERT).
	if input.Body.IdempotencyKey != nil {
		h.deduper.MarkIdempotency(ctx, tenant.ID, *input.Body.IdempotencyKey)
	}

	return &ingestOutput{Status: 202, Body: ingestResponseBody{EventID: &eventID, DeliveryCount: &deliveryCount}}, nil
}

func (h *ingestHandler) deduplicated(ctx context.Context, tenantID, idempotencyKey string) (*ingestOutput, error) {
	body := ingestResponseBody{Deduplicated: true}
	if id, err := events.GetIDByIdempotencyKey(ctx, h.pool, tenantID, idempotencyKey); err == nil && id > 0 {
		body.EventID = &id
	}
	return &ingestOutput{Status: 200, Body: body}, nil
}

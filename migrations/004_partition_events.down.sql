BEGIN;

DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE events (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_idempotency UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX idx_events_tenant_type ON events (tenant_id, event_type, created_at DESC);

DROP FUNCTION IF EXISTS create_monthly_partitions(TEXT, INT);

COMMIT;

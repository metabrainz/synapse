BEGIN;

-- Recreate unpartitioned events table from partitioned data.
CREATE TABLE events_unpartitioned (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_idempotency UNIQUE (tenant_id, idempotency_key)
);

INSERT INTO events_unpartitioned (id, tenant_id, event_type, payload, idempotency_key, created_at)
SELECT id, tenant_id, event_type, payload, idempotency_key, created_at
FROM events;

DROP TABLE events CASCADE;
ALTER TABLE events_unpartitioned RENAME TO events;

SELECT setval(pg_get_serial_sequence('events', 'id'), COALESCE((SELECT MAX(id) FROM events), 0));

CREATE INDEX idx_events_tenant_type ON events (tenant_id, event_type, created_at DESC);

DROP FUNCTION IF EXISTS create_monthly_partitions(TEXT, INT);

COMMIT;

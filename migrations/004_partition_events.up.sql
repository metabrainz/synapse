BEGIN;

DROP TABLE IF EXISTS events CASCADE;

CREATE TABLE events (
    id              BIGSERIAL,
    tenant_id       TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Idempotency constraint scoped to partition (includes created_at).
CREATE UNIQUE INDEX uq_event_idempotency_part
    ON events (tenant_id, idempotency_key, created_at)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_events_tenant_type ON events (tenant_id, event_type, created_at DESC);

-- Partitions: current month + 3 months ahead.
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOR i IN 0..3 LOOP
        start_date := date_trunc('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        end_date := start_date + '1 month'::INTERVAL;
        partition_name := 'events_' || to_char(start_date, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF events
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
    END LOOP;
END;
$$;

CREATE TABLE events_default PARTITION OF events DEFAULT;

CREATE OR REPLACE FUNCTION create_monthly_partitions(
    parent_table TEXT,
    months_ahead INT DEFAULT 3
) RETURNS void AS $fn$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOR i IN 0..months_ahead LOOP
        start_date := date_trunc('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        end_date := start_date + '1 month'::INTERVAL;
        partition_name := parent_table || '_' || to_char(start_date, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, parent_table, start_date, end_date
        );
    END LOOP;
END;
$fn$ LANGUAGE plpgsql;

COMMIT;

BEGIN;

-- Step 1: Rename existing table to make room for the partitioned replacement.
ALTER TABLE events RENAME TO events_old;

-- Step 2: Drop indexes that reference the old table name.
DROP INDEX IF EXISTS idx_events_tenant_type;

-- Step 3: Create partitioned table with created_at in the PK/UNIQUE.
-- Postgres requires the partition key in every unique constraint.
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
-- Safe: Redis dedup (30min TTL) catches cross-partition retries.
CREATE UNIQUE INDEX uq_event_idempotency_part
    ON events (tenant_id, idempotency_key, created_at)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_events_tenant_type ON events (tenant_id, event_type, created_at DESC);

-- Step 4: Create partitions covering existing data + 3 months ahead.
-- DEFAULT partition catches any rows outside explicit ranges.
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    min_date DATE;
    i INT;
    months_back INT;
BEGIN
    -- Determine how far back we need partitions
    SELECT date_trunc('month', MIN(created_at))::DATE INTO min_date FROM events_old;
    IF min_date IS NULL THEN
        min_date := date_trunc('month', CURRENT_DATE)::DATE;
    END IF;
    months_back := EXTRACT(YEAR FROM age(date_trunc('month', CURRENT_DATE), min_date)) * 12
                 + EXTRACT(MONTH FROM age(date_trunc('month', CURRENT_DATE), min_date));

    FOR i IN (-months_back)..3 LOOP
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

-- Default partition as safety net for any outliers.
CREATE TABLE events_default PARTITION OF events DEFAULT;

-- Step 5: Migrate existing data into the partitioned table.
INSERT INTO events (id, tenant_id, event_type, payload, idempotency_key, created_at)
SELECT id, tenant_id, event_type, payload, idempotency_key, created_at
FROM events_old;

-- Step 6: Sync the sequence. Use pg_get_serial_sequence to get the correct
-- sequence name for the NEW partitioned table (not the old one).
SELECT setval(pg_get_serial_sequence('events', 'id'), COALESCE((SELECT MAX(id) FROM events), 0));

-- Step 7: Drop old table. CASCADE drops the old constraints/indexes.
DROP TABLE events_old CASCADE;

-- Step 8: Create function for ongoing partition maintenance.
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

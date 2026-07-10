BEGIN;

-- Step 1: Rename existing table.
ALTER TABLE deliveries RENAME TO deliveries_old;

-- Step 2: Drop indexes referencing the old table.
DROP INDEX IF EXISTS idx_deliveries_status;
DROP INDEX IF EXISTS idx_deliveries_event;
DROP INDEX IF EXISTS idx_deliveries_user;

-- Step 3: Create partitioned deliveries table.
-- The FK to events is dropped: both tables are now partitioned and Postgres
-- does not support cross-partition foreign keys. Referential integrity is
-- guaranteed by the transactional outbox — deliveries are always written in
-- the same transaction as their parent event.
CREATE TABLE deliveries (
    id           BIGSERIAL,
    event_id     BIGINT      NOT NULL,
    user_id      TEXT        NOT NULL,
    channel_id   BIGINT      NOT NULL,
    channel_type TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN ('PENDING', 'RETRYING', 'DELIVERED', 'DEAD')),
    attempt      INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 5,
    last_error   TEXT,
    delivered_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_deliveries_status ON deliveries (status, created_at) WHERE status IN ('PENDING', 'RETRYING');
CREATE INDEX idx_deliveries_event  ON deliveries (event_id);
CREATE INDEX idx_deliveries_user   ON deliveries (user_id, created_at DESC);

-- Step 4: Create partitions covering existing data + 3 months ahead.
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
    min_date DATE;
    i INT;
    months_back INT;
BEGIN
    SELECT date_trunc('month', MIN(created_at))::DATE INTO min_date FROM deliveries_old;
    IF min_date IS NULL THEN
        min_date := date_trunc('month', CURRENT_DATE)::DATE;
    END IF;
    months_back := EXTRACT(YEAR FROM age(date_trunc('month', CURRENT_DATE), min_date)) * 12
                 + EXTRACT(MONTH FROM age(date_trunc('month', CURRENT_DATE), min_date));

    FOR i IN (-months_back)..3 LOOP
        start_date := date_trunc('month', CURRENT_DATE + (i || ' months')::INTERVAL);
        end_date := start_date + '1 month'::INTERVAL;
        partition_name := 'deliveries_' || to_char(start_date, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF deliveries
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
    END LOOP;
END;
$$;

-- Default partition as safety net.
CREATE TABLE deliveries_default PARTITION OF deliveries DEFAULT;

-- Step 5: Migrate existing data.
INSERT INTO deliveries (id, event_id, user_id, channel_id, channel_type, status,
                        attempt, max_attempts, last_error, delivered_at, created_at, updated_at)
SELECT id, event_id, user_id, channel_id, channel_type, status,
       attempt, max_attempts, last_error, delivered_at, created_at, updated_at
FROM deliveries_old;

-- Step 6: Sync sequence using the NEW table's actual sequence name.
SELECT setval(pg_get_serial_sequence('deliveries', 'id'), COALESCE((SELECT MAX(id) FROM deliveries), 0));

-- Step 7: Drop old table.
DROP TABLE deliveries_old;

COMMIT;

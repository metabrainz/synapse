BEGIN;

DROP TABLE IF EXISTS deliveries CASCADE;

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
        partition_name := 'deliveries_' || to_char(start_date, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF deliveries
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
    END LOOP;
END;
$$;

CREATE TABLE deliveries_default PARTITION OF deliveries DEFAULT;

COMMIT;

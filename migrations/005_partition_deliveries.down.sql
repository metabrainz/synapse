BEGIN;

-- Recreate unpartitioned deliveries table.
-- FK to events is omitted: events may still be partitioned (PK includes created_at),
-- and Postgres won't accept a FK to a non-matching unique constraint.
-- If both 004 and 005 are rolled back, the FK can be re-added manually.
CREATE TABLE deliveries_unpartitioned (
    id           BIGSERIAL   PRIMARY KEY,
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
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO deliveries_unpartitioned (id, event_id, user_id, channel_id, channel_type, status,
                                      attempt, max_attempts, last_error, delivered_at, created_at, updated_at)
SELECT id, event_id, user_id, channel_id, channel_type, status,
       attempt, max_attempts, last_error, delivered_at, created_at, updated_at
FROM deliveries;

DROP TABLE deliveries CASCADE;
ALTER TABLE deliveries_unpartitioned RENAME TO deliveries;

SELECT setval(pg_get_serial_sequence('deliveries', 'id'), COALESCE((SELECT MAX(id) FROM deliveries), 0));

CREATE INDEX idx_deliveries_status ON deliveries (status, created_at) WHERE status IN ('PENDING', 'RETRYING');
CREATE INDEX idx_deliveries_event  ON deliveries (event_id);
CREATE INDEX idx_deliveries_user   ON deliveries (user_id, created_at DESC);

COMMIT;

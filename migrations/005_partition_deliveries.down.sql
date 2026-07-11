BEGIN;

DROP TABLE IF EXISTS deliveries CASCADE;

CREATE TABLE deliveries (
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

CREATE INDEX idx_deliveries_status ON deliveries (status, created_at) WHERE status IN ('PENDING', 'RETRYING');
CREATE INDEX idx_deliveries_event  ON deliveries (event_id);
CREATE INDEX idx_deliveries_user   ON deliveries (user_id, created_at DESC);

COMMIT;

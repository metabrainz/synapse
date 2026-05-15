BEGIN;

ALTER TABLE outbox
    ADD COLUMN status    TEXT        NOT NULL DEFAULT 'PENDING',
    ADD COLUMN locked_by TEXT,
    ADD COLUMN locked_at TIMESTAMPTZ;

CREATE INDEX idx_outbox_poll ON outbox (created_at ASC)
    WHERE status = 'PENDING';

COMMIT;

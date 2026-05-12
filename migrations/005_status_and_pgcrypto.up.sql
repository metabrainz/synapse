BEGIN;

-- Enable pgcrypto for encrypting channel configs (webhook secrets, email addresses).
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Normalise status values to uppercase across the board.
UPDATE deliveries SET status = UPPER(status);

ALTER TABLE deliveries
    ALTER COLUMN status SET DEFAULT 'PENDING',
    ADD CONSTRAINT deliveries_status_check
        CHECK (status IN ('PENDING', 'RETRYING', 'DELIVERED', 'DEAD'));

COMMIT;

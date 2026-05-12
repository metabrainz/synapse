BEGIN;

ALTER TABLE deliveries
    DROP CONSTRAINT IF EXISTS deliveries_status_check,
    ALTER COLUMN status SET DEFAULT 'pending';

DROP EXTENSION IF EXISTS pgcrypto;

COMMIT;

BEGIN;

ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_channel_type_check;
ALTER TABLE channels   DROP CONSTRAINT IF EXISTS channels_type_check;

COMMIT;

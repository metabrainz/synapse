BEGIN;
-- Drop the FK on deliveries.channel_id → channels(id).
-- New deliveries reference user_channels(id), not the old channels table.
ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_channel_id_fkey;
COMMIT;

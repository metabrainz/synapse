BEGIN;
ALTER TABLE deliveries ADD CONSTRAINT deliveries_channel_id_fkey
    FOREIGN KEY (channel_id) REFERENCES channels(id);
COMMIT;

BEGIN;

ALTER TABLE channels
    ADD CONSTRAINT channels_type_check
    CHECK (type IN ('webhook', 'email'));

ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_channel_type_check
    CHECK (channel_type IN ('webhook', 'email'));

COMMIT;

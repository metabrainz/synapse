BEGIN;

ALTER TABLE channels
    DROP CONSTRAINT uq_channel_user_type_config,
    ADD CONSTRAINT uq_channel_user_type UNIQUE (tenant_id, user_id, type);

COMMIT;

BEGIN;

-- The original constraint allowed only one channel per (tenant, user, type).
-- A user may legitimately have multiple webhook endpoints or email addresses,
-- so we tighten the key to include config — uniqueness is now per destination.
ALTER TABLE channels
    DROP CONSTRAINT uq_channel_user_type,
    ADD CONSTRAINT uq_channel_user_type_config UNIQUE (tenant_id, user_id, type, config);

COMMIT;

BEGIN;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS channels;
DROP TABLE IF EXISTS tenant_event_channel_rules;
-- Drop all FK references to tenants so the tenants table can be dropped next.
ALTER TABLE events
    DROP CONSTRAINT IF EXISTS events_tenant_id_fkey;
ALTER TABLE user_tenant_channel_mapping
    DROP CONSTRAINT IF EXISTS user_tenant_channel_mapping_tenant_id_fkey;
ALTER TABLE user_event_subscriptions
    DROP CONSTRAINT IF EXISTS user_event_subscriptions_tenant_id_fkey;
COMMIT;

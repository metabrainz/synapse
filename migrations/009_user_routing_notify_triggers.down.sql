BEGIN;
DROP TRIGGER IF EXISTS trg_user_event_subs_changes ON user_event_subscriptions;
DROP TRIGGER IF EXISTS trg_tenant_rules_changes ON tenant_event_channel_rules;
DROP TRIGGER IF EXISTS trg_user_tenant_mapping_changes ON user_tenant_channel_mapping;
DROP TRIGGER IF EXISTS trg_user_channels_changes ON user_channels;
COMMIT;

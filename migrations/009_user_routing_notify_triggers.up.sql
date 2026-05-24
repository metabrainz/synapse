BEGIN;

-- Reuse the existing notify_subscription_change function so the fanout cache
-- rebuilds whenever routing tables change.

CREATE TRIGGER trg_user_channels_changes
AFTER INSERT OR UPDATE OR DELETE ON user_channels
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

CREATE TRIGGER trg_user_tenant_mapping_changes
AFTER INSERT OR UPDATE OR DELETE ON user_tenant_channel_mapping
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

CREATE TRIGGER trg_tenant_rules_changes
AFTER INSERT OR UPDATE OR DELETE ON tenant_event_channel_rules
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

CREATE TRIGGER trg_user_event_subs_changes
AFTER INSERT OR UPDATE OR DELETE ON user_event_subscriptions
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

COMMIT;

BEGIN;

CREATE OR REPLACE FUNCTION notify_subscription_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('subscription_changes', TG_OP);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_user_channels_changes
AFTER INSERT OR UPDATE OR DELETE ON user_channels
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

CREATE TRIGGER trg_user_tenant_mapping_changes
AFTER INSERT OR UPDATE OR DELETE ON user_tenant_channel_mapping
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

CREATE TRIGGER trg_user_event_subs_changes
AFTER INSERT OR UPDATE OR DELETE ON user_event_subscriptions
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

COMMIT;

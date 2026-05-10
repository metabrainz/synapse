BEGIN;

CREATE OR REPLACE FUNCTION notify_subscription_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('subscription_changes', TG_OP);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_subscription_changes
AFTER INSERT OR UPDATE OR DELETE ON subscriptions
FOR EACH STATEMENT EXECUTE FUNCTION notify_subscription_change();

COMMIT;

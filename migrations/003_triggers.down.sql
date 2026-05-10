BEGIN;

DROP TRIGGER IF EXISTS trg_subscription_changes ON subscriptions;
DROP FUNCTION IF EXISTS notify_subscription_change();

COMMIT;

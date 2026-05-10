BEGIN;

DROP INDEX idx_outbox_created;
DROP INDEX idx_deliveries_event;
DROP INDEX idx_deliveries_status;
DROP INDEX idx_events_tenant_user;
DROP INDEX idx_subs_channel;
DROP INDEX idx_channels_tenant_user;

COMMIT;

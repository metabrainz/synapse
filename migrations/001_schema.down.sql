BEGIN;
DROP TABLE IF EXISTS outbox, deliveries, events, user_event_subscriptions, user_tenant_channel_mapping, user_channels, users CASCADE;
DROP EXTENSION IF EXISTS pgcrypto;
COMMIT;

BEGIN;

DROP TABLE outbox;
DROP TABLE deliveries;
DROP TABLE events;
DROP TABLE subscriptions;
DROP TABLE channels;
DROP TABLE event_type_definitions;
DROP TABLE tenants;

COMMIT;

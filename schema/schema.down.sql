-- =============================================================================
-- Synapse — complete teardown (apply in order, reverses schema.up.sql)
-- This file is for reference only. The authoritative source is migrations/*.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 004: Drop channel type constraints
-- -----------------------------------------------------------------------------

ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_channel_type_check;
ALTER TABLE channels   DROP CONSTRAINT IF EXISTS channels_type_check;

-- -----------------------------------------------------------------------------
-- 003: Drop triggers
-- -----------------------------------------------------------------------------

DROP TRIGGER   IF EXISTS trg_subscription_changes ON subscriptions;
DROP FUNCTION  IF EXISTS notify_subscription_change();

-- -----------------------------------------------------------------------------
-- 002: Drop indexes
-- -----------------------------------------------------------------------------

DROP INDEX IF EXISTS idx_outbox_created;
DROP INDEX IF EXISTS idx_deliveries_event;
DROP INDEX IF EXISTS idx_deliveries_status;
DROP INDEX IF EXISTS idx_events_tenant_user;
DROP INDEX IF EXISTS idx_subs_channel;
DROP INDEX IF EXISTS idx_channels_tenant_user;

-- -----------------------------------------------------------------------------
-- 001: Drop tables (reverse FK order)
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS deliveries;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS channels;
DROP TABLE IF EXISTS event_type_definitions;
DROP TABLE IF EXISTS tenants;

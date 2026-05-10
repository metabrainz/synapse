-- =============================================================================
-- Synapse — complete schema (apply in order)
-- This file is for reference only. The authoritative source is migrations/*.sql
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 001: Core tables
-- -----------------------------------------------------------------------------

CREATE TABLE tenants (
    id         TEXT        PRIMARY KEY,
    api_key    TEXT        NOT NULL UNIQUE,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tenants declare which event types they emit. Events table FKs into this,
-- so publishing an undeclared event type is rejected at the DB level.
CREATE TABLE event_type_definitions (
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,
    description TEXT,
    PRIMARY KEY (tenant_id, event_type)
);

-- A channel is one delivery endpoint owned by a user (e.g. their webhook URL,
-- their email address). One user can have at most one channel per type.
CREATE TABLE channels (
    id                   BIGSERIAL   PRIMARY KEY,
    tenant_id            TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id              TEXT        NOT NULL,
    type                 TEXT        NOT NULL,
    config               JSONB       NOT NULL DEFAULT '{}',
    enabled              BOOLEAN     NOT NULL DEFAULT TRUE,
    status               TEXT        NOT NULL DEFAULT 'active',
    consecutive_failures INT         NOT NULL DEFAULT 0,
    last_failed_at       TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_channel_user_type UNIQUE (tenant_id, user_id, type)
);

-- A subscription links a channel to an event type. event_type = '*' means all
-- events for that tenant+user.
CREATE TABLE subscriptions (
    id         BIGSERIAL   PRIMARY KEY,
    channel_id BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    event_type TEXT        NOT NULL,
    enabled    BOOLEAN     NOT NULL DEFAULT TRUE,
    config     JSONB       NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_sub_channel_event UNIQUE (channel_id, event_type)
);

-- Immutable record of every inbound event. idempotency_key prevents duplicates
-- from noisy callers at the DB level (UNIQUE per tenant).
CREATE TABLE events (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       TEXT        NOT NULL REFERENCES tenants(id),
    user_id         TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_event_idempotency UNIQUE (tenant_id, idempotency_key),
    CONSTRAINT fk_event_type FOREIGN KEY (tenant_id, event_type)
        REFERENCES event_type_definitions (tenant_id, event_type)
);

-- One delivery row per (event, channel) pair. Tracks retry state.
CREATE TABLE deliveries (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     BIGINT      NOT NULL REFERENCES events(id),
    channel_id   BIGINT      NOT NULL REFERENCES channels(id),
    channel_type TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'pending',
    attempt      INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 5,
    last_error   TEXT,
    delivered_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Transactional outbox: written in the same tx as event + deliveries.
-- The relay process polls this table and publishes to RabbitMQ, then deletes
-- the row. At-least-once guarantee: if relay crashes after publish but before
-- delete, the row is re-published on restart.
CREATE TABLE outbox (
    id          BIGSERIAL   PRIMARY KEY,
    routing_key TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- 002: Indexes
-- -----------------------------------------------------------------------------

CREATE INDEX idx_channels_tenant_user ON channels (tenant_id, user_id);
CREATE INDEX idx_subs_channel         ON subscriptions (channel_id, enabled) WHERE enabled = TRUE;
CREATE INDEX idx_events_tenant_user   ON events (tenant_id, user_id, created_at DESC);
CREATE INDEX idx_deliveries_status    ON deliveries (status, created_at) WHERE status IN ('pending', 'retrying');
CREATE INDEX idx_deliveries_event     ON deliveries (event_id);
CREATE INDEX idx_outbox_created       ON outbox (created_at ASC);

-- -----------------------------------------------------------------------------
-- 003: Triggers
-- -----------------------------------------------------------------------------

-- Notifies the API cache layer whenever subscriptions change so it can
-- invalidate in-memory subscription lookups without polling.
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

-- -----------------------------------------------------------------------------
-- 004: Channel type constraints
-- -----------------------------------------------------------------------------

-- CHECK constraints rather than a PG enum so adding a new channel type is a
-- normal transactional ALTER TABLE (enum ADD VALUE cannot be rolled back).
ALTER TABLE channels
    ADD CONSTRAINT channels_type_check
    CHECK (type IN ('webhook', 'email'));

ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_channel_type_check
    CHECK (channel_type IN ('webhook', 'email'));

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
    id         TEXT        PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE user_channels (
    id           BIGSERIAL   PRIMARY KEY,
    user_id      TEXT        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_type TEXT        NOT NULL,
    label        TEXT        NOT NULL DEFAULT '',
    config       JSONB       NOT NULL DEFAULT '{}',
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_user_channels_user ON user_channels (user_id);

CREATE TABLE user_tenant_channel_mapping (
    user_id         TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id       TEXT    NOT NULL,
    channel_type    TEXT    NOT NULL,
    user_channel_id BIGINT  NOT NULL REFERENCES user_channels(id) ON DELETE CASCADE,
    is_enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (user_id, tenant_id, channel_type)
);

CREATE TABLE user_event_subscriptions (
    user_id      TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id    TEXT    NOT NULL,
    event_type   TEXT    NOT NULL,
    channel_type TEXT    NOT NULL,
    is_enabled   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, tenant_id, event_type, channel_type)
);

-- events is the logical event: what happened, not who receives it. A single
-- event fans out to many recipients, so there is no per-recipient column here —
-- the recipient lives on each delivery row.
CREATE TABLE events (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_event_idempotency UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX idx_events_tenant_type ON events (tenant_id, event_type, created_at DESC);

CREATE TABLE deliveries (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     BIGINT      NOT NULL REFERENCES events(id),
    user_id      TEXT        NOT NULL,  -- the recipient; stored explicitly so it
                                        -- survives channel deletion and powers
                                        -- per-user queries (e.g. in-app inbox).
    channel_id   BIGINT      NOT NULL,  -- snapshot of user_channels.id, no FK
    channel_type TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN ('PENDING', 'RETRYING', 'DELIVERED', 'DEAD')),
    attempt      INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 5,
    last_error   TEXT,
    delivered_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_deliveries_status ON deliveries (status, created_at) WHERE status IN ('PENDING', 'RETRYING');
CREATE INDEX idx_deliveries_event  ON deliveries (event_id);
CREATE INDEX idx_deliveries_user   ON deliveries (user_id, created_at DESC);

CREATE TABLE outbox (
    id          BIGSERIAL   PRIMARY KEY,
    routing_key TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'PENDING',
    locked_by   TEXT,
    locked_at   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_poll ON outbox (created_at ASC) WHERE status = 'PENDING';

COMMIT;

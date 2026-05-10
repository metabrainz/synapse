BEGIN;

CREATE TABLE tenants (
    id         TEXT        PRIMARY KEY,
    api_key    TEXT        NOT NULL UNIQUE,
    name       TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE event_type_definitions (
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,
    description TEXT,
    PRIMARY KEY (tenant_id, event_type)
);

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

CREATE TABLE subscriptions (
    id         BIGSERIAL   PRIMARY KEY,
    channel_id BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    event_type TEXT        NOT NULL,
    enabled    BOOLEAN     NOT NULL DEFAULT TRUE,
    config     JSONB       NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_sub_channel_event UNIQUE (channel_id, event_type)
);

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

CREATE TABLE outbox (
    id          BIGSERIAL   PRIMARY KEY,
    routing_key TEXT        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;

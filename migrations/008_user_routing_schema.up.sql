BEGIN;

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

CREATE TABLE tenant_event_channel_rules (
    tenant_id    TEXT    NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type   TEXT    NOT NULL,
    channel_type TEXT    NOT NULL,
    is_allowed   BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (tenant_id, event_type, channel_type)
);

CREATE TABLE user_tenant_channel_mapping (
    user_id         TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id       TEXT    NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    channel_type    TEXT    NOT NULL,
    user_channel_id BIGINT  NOT NULL REFERENCES user_channels(id) ON DELETE CASCADE,
    is_enabled      BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (user_id, tenant_id, channel_type)
);

CREATE TABLE user_event_subscriptions (
    user_id      TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id    TEXT    NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type   TEXT    NOT NULL,
    channel_type TEXT    NOT NULL,
    is_enabled   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, tenant_id, event_type, channel_type)
);

COMMIT;

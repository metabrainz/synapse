BEGIN;
ALTER TABLE user_tenant_channel_mapping
    ADD CONSTRAINT user_tenant_channel_mapping_tenant_id_fkey
    FOREIGN KEY (tenant_id) REFERENCES tenants(id);
ALTER TABLE user_event_subscriptions
    ADD CONSTRAINT user_event_subscriptions_tenant_id_fkey
    FOREIGN KEY (tenant_id) REFERENCES tenants(id);
CREATE TABLE IF NOT EXISTS tenant_event_channel_rules (
    tenant_id    TEXT    NOT NULL REFERENCES tenants(id),
    event_type   TEXT    NOT NULL,
    channel_type TEXT    NOT NULL,
    is_allowed   BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (tenant_id, event_type, channel_type)
);
COMMIT;

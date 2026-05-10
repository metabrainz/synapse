BEGIN;

CREATE INDEX idx_channels_tenant_user ON channels (tenant_id, user_id);
CREATE INDEX idx_subs_channel ON subscriptions (channel_id, enabled) WHERE enabled = TRUE;
CREATE INDEX idx_events_tenant_user ON events (tenant_id, user_id, created_at DESC);
CREATE INDEX idx_deliveries_status ON deliveries (status, created_at) WHERE status IN ('pending', 'retrying');
CREATE INDEX idx_deliveries_event ON deliveries (event_id);
CREATE INDEX idx_outbox_created ON outbox (created_at ASC);

COMMIT;

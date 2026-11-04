BEGIN;

-- user_id has no users foreign key: a recipient can be notified before ever logging in.
CREATE TABLE notifications (
    id           BIGSERIAL   PRIMARY KEY,
    event_id     BIGINT      NOT NULL REFERENCES events(id),
    user_id      TEXT        NOT NULL,
    tenant_id    TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at      TIMESTAMPTZ,
    dismissed_at TIMESTAMPTZ,
    expires_at   TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_notification_event_user UNIQUE (event_id, user_id)
);

CREATE INDEX idx_notifications_inbox ON notifications (user_id, id DESC)
    WHERE dismissed_at IS NULL;

CREATE INDEX idx_notifications_inbox_tenant ON notifications (user_id, tenant_id, id DESC)
    WHERE dismissed_at IS NULL;

CREATE INDEX idx_notifications_unread ON notifications (user_id, tenant_id)
    WHERE dismissed_at IS NULL AND read_at IS NULL;

CREATE INDEX idx_notifications_expiry ON notifications (expires_at);

COMMIT;

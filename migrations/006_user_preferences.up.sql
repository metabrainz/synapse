BEGIN;

CREATE TABLE user_preferences (
    user_id                    TEXT    PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    cross_tenant_notifications BOOLEAN NOT NULL DEFAULT false
);

COMMIT;

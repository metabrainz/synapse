BEGIN;
CREATE TABLE tenants (
    id         TEXT PRIMARY KEY,
    api_key    TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMIT;

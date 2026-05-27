BEGIN;
CREATE TABLE event_type_definitions (
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    event_type  TEXT NOT NULL,
    description TEXT,
    PRIMARY KEY (tenant_id, event_type)
);
COMMIT;

BEGIN;
ALTER TABLE events ADD CONSTRAINT fk_event_type
    FOREIGN KEY (tenant_id, event_type)
    REFERENCES event_type_definitions (tenant_id, event_type);
COMMIT;

BEGIN;

ALTER TABLE deliveries DROP CONSTRAINT deliveries_event_id_fkey;
ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_event_id_fkey
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE;

COMMIT;

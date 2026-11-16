-- Every enqueue stamps a node-wide revision, so an acknowledgement deletes only
-- the obligation it sent and keeps a re-enqueue made while the send was in flight.
CREATE TABLE node_dirty_ref_revision (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    value INTEGER NOT NULL CHECK (typeof(value) = 'integer' AND value >= 0)
);

INSERT INTO node_dirty_ref_revision (singleton, value) VALUES (1, 0);

ALTER TABLE node_dirty_refs ADD COLUMN revision INTEGER NOT NULL DEFAULT 0;

CREATE TRIGGER node_dirty_refs_insert_revision AFTER INSERT ON node_dirty_refs
BEGIN
    UPDATE node_dirty_ref_revision SET value = value + 1 WHERE singleton = 1;
    UPDATE node_dirty_refs
    SET revision = (SELECT value FROM node_dirty_ref_revision WHERE singleton = 1)
    WHERE facet = NEW.facet AND element_kind = NEW.element_kind
      AND element_value = NEW.element_value;
END;

CREATE TRIGGER node_dirty_refs_update_revision
AFTER UPDATE OF requested_at, updated_at ON node_dirty_refs
BEGIN
    UPDATE node_dirty_ref_revision SET value = value + 1 WHERE singleton = 1;
    UPDATE node_dirty_refs
    SET revision = (SELECT value FROM node_dirty_ref_revision WHERE singleton = 1)
    WHERE facet = NEW.facet AND element_kind = NEW.element_kind
      AND element_value = NEW.element_value;
END;

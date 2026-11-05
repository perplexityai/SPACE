DROP TRIGGER node_dirty_refs_update_revision;
DROP TRIGGER node_dirty_refs_insert_revision;
ALTER TABLE node_dirty_refs DROP COLUMN revision;
DROP TABLE node_dirty_ref_revision;

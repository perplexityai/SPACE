-- Marks an artifact whose created_at may be compared with its sandbox's
-- state_changed_at to decide whether a stopped sandbox's pin still describes
-- its rootfs. Only binaries that drop a stopped pin on every path that can
-- leave it stale write 1; rows written before this column existed keep 0 and
-- are reported with no capture time.
ALTER TABLE local_artifacts
    ADD COLUMN capture_time_comparable INTEGER NOT NULL DEFAULT 0
    CHECK (capture_time_comparable IN (0, 1));

-- Enrollment and unresolved ownership history must survive binary rollback.
CREATE TEMP TABLE device_rollback_guard (remaining INTEGER CHECK (remaining = 0));
INSERT INTO device_rollback_guard SELECT
    (SELECT COUNT(*) FROM device_parents) +
    (SELECT COUNT(*) FROM device_parent_members) +
    (SELECT COUNT(*) FROM device_active_members) +
    (SELECT COUNT(*) FROM device_allocations) +
    (SELECT COUNT(*) FROM device_allocation_members) +
    (SELECT COUNT(*) FROM device_exclusive_member_claims);
DROP TABLE device_exclusive_member_claims;
DROP TABLE device_allocation_members;
DROP TABLE device_allocations;
DROP TABLE device_active_members;
DROP TABLE device_parent_members;
DROP TABLE device_parents;
DROP TABLE device_rollback_guard;

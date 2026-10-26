-- Old readers must not start a device-bearing sandbox as CPU-only.
CREATE TEMP TABLE device_requirements_rollback_guard (remaining INTEGER CHECK (remaining = 0));
INSERT INTO device_requirements_rollback_guard
    SELECT COUNT(*) FROM sandboxes WHERE device_requirements_json IS NOT NULL;
ALTER TABLE sandboxes DROP COLUMN device_requirements_json;
DROP TABLE device_requirements_rollback_guard;

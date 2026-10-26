-- Nonlegacy images cannot be reinterpreted by an older reader.
CREATE TEMP TABLE guest_image_rollback_guard (count INTEGER CHECK (count = 0));
INSERT INTO guest_image_rollback_guard SELECT COUNT(*) FROM sandboxes WHERE guest_image_contract_json IS NOT NULL;
DROP TABLE guest_image_rollback_guard;
ALTER TABLE sandboxes DROP COLUMN guest_image_contract_json;

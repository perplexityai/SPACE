SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '60s';

DROP TRIGGER sandboxes_clear_evacuation_restore_target ON sandboxes;
CREATE TRIGGER sandboxes_clear_evacuation_restore_target BEFORE UPDATE ON sandboxes
FOR EACH ROW EXECUTE FUNCTION sandboxes_clear_evacuation_restore_target();

DROP TRIGGER sandboxes_clear_restore_lease ON sandboxes;
CREATE TRIGGER sandboxes_clear_restore_lease BEFORE UPDATE ON sandboxes
FOR EACH ROW EXECUTE FUNCTION sandboxes_clear_restore_lease();

DROP TRIGGER sandboxes_set_updated_at ON sandboxes;
CREATE TRIGGER sandboxes_set_updated_at BEFORE UPDATE ON sandboxes
FOR EACH ROW EXECUTE FUNCTION set_sandbox_updated_at();

ALTER TABLE sandboxes DROP COLUMN lifecycle_registration;

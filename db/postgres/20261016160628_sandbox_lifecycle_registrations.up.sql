SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '60s';

-- Enrollment is independent of runtime policy. NULL preserves legacy behavior.
ALTER TABLE sandboxes ADD COLUMN lifecycle_registration JSONB;
ALTER TABLE sandboxes ADD CONSTRAINT sandboxes_lifecycle_registration_shape CHECK (
    lifecycle_registration IS NULL OR COALESCE(
        jsonb_typeof(lifecycle_registration) = 'object'
        AND jsonb_typeof(lifecycle_registration -> 'enabled') = 'boolean'
        AND jsonb_typeof(lifecycle_registration -> 'revision') = 'number'
        AND (lifecycle_registration ->> 'revision') ~ '^[1-9][0-9]{0,18}$'
        AND jsonb_typeof(lifecycle_registration -> 'binding') = 'object',
        FALSE
    )
) NOT VALID;

-- Registration-only writes must not look like runtime updates or repair unrelated
-- restore bookkeeping. Ordinary updates retain the existing trigger behavior.
DROP TRIGGER sandboxes_clear_evacuation_restore_target ON sandboxes;
CREATE TRIGGER sandboxes_clear_evacuation_restore_target BEFORE UPDATE ON sandboxes
FOR EACH ROW WHEN (
    OLD.lifecycle_registration IS NOT DISTINCT FROM NEW.lifecycle_registration
    OR (to_jsonb(OLD) - 'lifecycle_registration') IS DISTINCT FROM
       (to_jsonb(NEW) - 'lifecycle_registration')
) EXECUTE FUNCTION sandboxes_clear_evacuation_restore_target();

DROP TRIGGER sandboxes_clear_restore_lease ON sandboxes;
CREATE TRIGGER sandboxes_clear_restore_lease BEFORE UPDATE ON sandboxes
FOR EACH ROW WHEN (
    OLD.lifecycle_registration IS NOT DISTINCT FROM NEW.lifecycle_registration
    OR (to_jsonb(OLD) - 'lifecycle_registration') IS DISTINCT FROM
       (to_jsonb(NEW) - 'lifecycle_registration')
) EXECUTE FUNCTION sandboxes_clear_restore_lease();

DROP TRIGGER sandboxes_set_updated_at ON sandboxes;
CREATE TRIGGER sandboxes_set_updated_at BEFORE UPDATE ON sandboxes
FOR EACH ROW WHEN (
    OLD.lifecycle_registration IS NOT DISTINCT FROM NEW.lifecycle_registration
    OR (to_jsonb(OLD) - 'lifecycle_registration') IS DISTINCT FROM
       (to_jsonb(NEW) - 'lifecycle_registration')
) EXECUTE FUNCTION set_sandbox_updated_at();

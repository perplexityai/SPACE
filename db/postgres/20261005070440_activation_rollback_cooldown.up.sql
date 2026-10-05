SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE sandboxes ADD COLUMN activation_rollback_retry_after TIMESTAMPTZ;

CREATE FUNCTION clear_activation_rollback_cooldown() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lifecycle = 'started' OR (OLD.lifecycle = 'restoring' AND NEW.lifecycle = 'paused') THEN
        NEW.activation_rollback_retry_after := NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sandboxes_clear_activation_rollback_cooldown
BEFORE UPDATE OF lifecycle ON sandboxes
FOR EACH ROW EXECUTE FUNCTION clear_activation_rollback_cooldown();

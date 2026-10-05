SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DROP TRIGGER sandboxes_clear_activation_rollback_cooldown ON sandboxes;
DROP FUNCTION clear_activation_rollback_cooldown();
ALTER TABLE sandboxes DROP COLUMN activation_rollback_retry_after;

DROP TRIGGER IF EXISTS clear_stop_cause_when_running;
ALTER TABLE sandboxes DROP COLUMN stop_cause;

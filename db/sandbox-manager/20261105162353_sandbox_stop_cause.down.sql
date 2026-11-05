DROP TRIGGER IF EXISTS clear_stop_attribution_when_running;
ALTER TABLE sandboxes DROP COLUMN stop_cause_at;
ALTER TABLE sandboxes DROP COLUMN stop_cause;

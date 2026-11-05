-- Why a stopped sandbox stopped when no one asked it to (`pod_terminated` or
-- `vm_exited`). The stop admission that newly moves the row to `stopping`
-- records it, NULL for a requested stop; a stop that re-admits a row another
-- stop already owns keeps that stop's attribution.
ALTER TABLE sandboxes ADD COLUMN stop_cause TEXT;

-- A cause lasts until the sandbox runs again: whatever writer moves the row
-- to `started` or `paused` ends the stop it describes. The stop's own cleanup
-- states (`cleanup_required`, `cleanup_failed` and the retry back through
-- `stopping`), a failed start that returns the row to `stopped`, and a delete
-- all keep it.
CREATE TRIGGER clear_stop_cause_when_running
AFTER UPDATE OF state ON sandboxes
WHEN NEW.stop_cause IS NOT NULL
 AND NEW.state IN ('started', 'paused')
BEGIN
    UPDATE sandboxes SET stop_cause = NULL WHERE id = NEW.id;
END;

-- The node's attribution of the sandbox's last stop: why it stopped when no
-- one asked it to (`pod_terminated` or `vm_exited`; NULL for a requested
-- stop), and when, in epoch milliseconds of this node's clock. The stop
-- admission that newly moves the row to `stopping` writes both, a requested
-- stop included; a stop that re-admits a row another stop already owns keeps
-- that stop's. `stop_cause_at` NULL means no attribution: the sandbox ran
-- since, or an older sbx-manager stopped it.
ALTER TABLE sandboxes ADD COLUMN stop_cause TEXT;
ALTER TABLE sandboxes ADD COLUMN stop_cause_at INTEGER;

-- An attribution lasts until the sandbox runs again: whatever writer moves
-- the row to `started` or `paused` ends the stop it describes. The stop's own
-- cleanup states and their retry, and a failed start that returns the row to
-- `stopped`, keep it.
CREATE TRIGGER clear_stop_attribution_when_running
AFTER UPDATE OF state ON sandboxes
WHEN NEW.stop_cause_at IS NOT NULL
 AND NEW.state IN ('started', 'paused')
BEGIN
    UPDATE sandboxes SET stop_cause = NULL, stop_cause_at = NULL WHERE id = NEW.id;
END;

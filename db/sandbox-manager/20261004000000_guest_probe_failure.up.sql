CREATE TABLE sandbox_guest_probe_failures (
    sandbox_id TEXT PRIMARY KEY REFERENCES sandboxes(id) ON DELETE CASCADE,
    node_execution_id TEXT,
    runtime_pid INTEGER,
    vsock_uds_path TEXT,
    first_failed_at INTEGER NOT NULL CHECK (first_failed_at >= 0)
);

CREATE TRIGGER reset_sandbox_guest_probe_failure
AFTER UPDATE OF state, node_execution_id, node_execution_retired, pid, vsock_uds_path ON sandboxes
WHEN OLD.state IS NOT NEW.state
  OR OLD.node_execution_id IS NOT NEW.node_execution_id
  OR OLD.node_execution_retired IS NOT NEW.node_execution_retired
  OR OLD.pid IS NOT NEW.pid
  OR OLD.vsock_uds_path IS NOT NEW.vsock_uds_path
BEGIN
    DELETE FROM sandbox_guest_probe_failures WHERE sandbox_id = NEW.id;
END;

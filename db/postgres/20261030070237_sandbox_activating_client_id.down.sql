-- Roll back the images that read and write the column before removing it.
-- Down runs once, without the lock retry up has; rerun it if it times out.
-- If up is reapplied, every sandbox starts NULL: a running Kubernetes sandbox
-- whose pod carries a label gets the value back when pod tracking next sees
-- the pod, and any other sandbox stays NULL until a caller's own activation.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE sandboxes
    DROP COLUMN activating_client_id;

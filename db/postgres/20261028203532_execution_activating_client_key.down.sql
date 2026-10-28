-- Roll back the API images that read and write the column before removing it.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

ALTER TABLE sandbox_k8s_executions
    DROP COLUMN activating_client_key;

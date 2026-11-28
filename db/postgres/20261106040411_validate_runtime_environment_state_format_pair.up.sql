SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Run in a separate SQLx transaction, after the metadata-changing migration
-- releases its ACCESS EXCLUSIVE lock. Validation then takes its own weaker lock.
ALTER TABLE runtime_environments
    VALIDATE CONSTRAINT runtime_environments_state_format_pair_check;

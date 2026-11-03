SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The runtime upgrade a stay-stopped activation decided for this execution: the
-- operation id, target environment and authorization id of the upgrade it asks
-- the node for. A failed attempt can leave the execution behind, after a
-- dispatched Wake timed out or a step after a successful boot failed. A retry
-- that claims the leftover re-sends that exact upgrade, so sandbox-manager
-- settles it as the same operation, and the recorded target says whether the
-- upgraded disk still needs its backup. Deciding again needn't name that
-- upgrade: the operation id hashes the decision's cause, source environment and
-- cluster default with the sandbox, activation and subject, so a retry whose
-- inputs differ from the first attempt's, as after a policy change, can derive
-- a different operation, or decide on none. Re-sending the recorded operation
-- id takes the recorded authorization id, which names the same cause and
-- source: while sandbox-manager keeps its record of an operation, it refuses a
-- repeat whose sandbox, target, target state or authorization id differs from
-- the recorded ones. The row already names the sandbox, and a stay-stopped
-- activation always asks for the same target state. Without these columns the
-- decision lives only in the attempt's memory.
--
-- All three are NULL on every execution that isn't such an upgrade, and on
-- every row that predates the writers. They're set together or not at all.
-- The operation id is the UUID the node's runtime upgrade record is keyed by;
-- it names no control-plane row, so it has no foreign key. The target
-- reference has the runtime_environments format. The authorization id is
-- printable ASCII with no leading or trailing space, as its producer's
-- runtime-environment-<cause>:<environment> is, within sandbox-manager's bound
-- of 1 to 256 bytes. Sandbox-manager trims the id before checking that bound;
-- ASCII without edge spaces is unchanged by the trim and needs no locale to
-- classify, so every id the CHECK accepts, sandbox-manager accepts unchanged.
--
-- Nullable columns with no default change the catalog only; no row is
-- rewritten. The CHECKs are NOT VALID so adding them doesn't scan the table;
-- every existing row is NULL and satisfies them, and new writes are checked
-- either way. A later migration can VALIDATE them. The pattern matches return
-- NULL for a NULL column without running, so only a row carrying a decision
-- pays for them when it's inserted or updated.
ALTER TABLE sandbox_k8s_executions
    ADD COLUMN runtime_upgrade_operation_id UUID,
    ADD COLUMN runtime_upgrade_target_environment_ref TEXT,
    ADD COLUMN runtime_upgrade_authorization_id TEXT,
    ADD CONSTRAINT sandbox_k8s_executions_runtime_upgrade_target_check
        CHECK (
            runtime_upgrade_target_environment_ref ~ '^(sha256|blake3):[0-9a-f]{64}$'
        ) NOT VALID,
    ADD CONSTRAINT sandbox_k8s_executions_runtime_upgrade_authorization_id_check
        CHECK (
            octet_length(runtime_upgrade_authorization_id) BETWEEN 1 AND 256
            AND runtime_upgrade_authorization_id ~ '^[!-~]([ -~]*[!-~])?$'
        ) NOT VALID,
    ADD CONSTRAINT sandbox_k8s_executions_runtime_upgrade_decision_check
        CHECK (
            (runtime_upgrade_operation_id IS NULL)
                = (runtime_upgrade_target_environment_ref IS NULL)
            AND (runtime_upgrade_operation_id IS NULL)
                = (runtime_upgrade_authorization_id IS NULL)
        ) NOT VALID;

COMMENT ON COLUMN sandbox_k8s_executions.runtime_upgrade_operation_id IS
    'Operation id of the runtime upgrade a stay-stopped activation decided for this execution; set exactly when the other runtime_upgrade_ columns are';
COMMENT ON COLUMN sandbox_k8s_executions.runtime_upgrade_target_environment_ref IS
    'Environment that upgrade targets; set exactly when the other runtime_upgrade_ columns are';
COMMENT ON COLUMN sandbox_k8s_executions.runtime_upgrade_authorization_id IS
    'Authorization id that upgrade carries, which a retry re-sending its operation id must repeat; set exactly when the other runtime_upgrade_ columns are';

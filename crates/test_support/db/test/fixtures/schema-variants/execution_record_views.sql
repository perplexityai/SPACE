-- TEST CANDIDATE, NOT A MIGRATION. Nothing applies this file to a real
-- database. The test fixtures apply it after the ordinary migrations when a
-- run sets SPACE_TEST_POSTGRES_SCHEMA_VARIANT=execution-record-views (see this
-- crate's README), so the existing Postgres suites can show that today's code
-- works unchanged once these two tables are renamed and compatibility views
-- keep their old names.
--
-- `sandbox_k8s_executions` becomes `sandbox_execution_records`, a record of
-- every substrate's executions, and `node_k8s` becomes `node_identities`.
-- Each view selects the old column list in the old order, so a statement
-- written against the old table, `SELECT *` and statements prepared before the
-- rename included, reads the same row type. The views are automatically
-- updatable; `WITH CASCADED CHECK OPTION` keeps a write through them from
-- leaving a row the view cannot see.
--
-- Renames keep a relation's indexes, constraints, triggers, foreign keys and
-- grants, column grants included, so the base tables need new grants only
-- where an invoker view reads a column its caller was not granted. The service
-- logins get their privileges on the views from `space_migrator`'s default
-- privileges and bootstrap-roles, as for any new relation. The views run with
-- the caller's privileges (`security_invoker`), so every role that reads
-- through a view needs the base table's privileges as well as the view's.

ALTER TABLE sandbox_k8s_executions RENAME TO sandbox_execution_records;
ALTER TABLE sandbox_execution_records
    RENAME COLUMN pod_materialized_at TO materialized_at;
ALTER TABLE sandbox_execution_records
    ADD COLUMN managed_by TEXT NOT NULL DEFAULT 'k8s',
    ADD COLUMN node_id UUID NULL,
    ADD COLUMN node_incarnation_uid TEXT NULL,
    ADD COLUMN route_generation BIGINT NULL,
    ALTER COLUMN pod_name DROP NOT NULL,
    ALTER COLUMN pod_namespace DROP NOT NULL;

CREATE VIEW sandbox_k8s_executions WITH (security_invoker = true) AS
SELECT id,
       sandbox_id,
       generation,
       pod_name,
       pod_namespace,
       pod_uid,
       pod_ip,
       k8s_node_name,
       state,
       active,
       created_at,
       updated_at,
       terminated_at,
       activation_attempt_id,
       activation_attempt_expires_at,
       activation_attempt_target_state,
       route_activation_id,
       materialized_at AS pod_materialized_at,
       runtime_upgrade_operation_id,
       runtime_upgrade_target_environment_ref,
       runtime_upgrade_authorization_id
FROM sandbox_execution_records
WHERE managed_by = 'k8s'
WITH CASCADED CHECK OPTION;

ALTER TABLE node_k8s RENAME TO node_identities;
ALTER TABLE node_identities RENAME COLUMN k8s_node_name TO name;
ALTER TABLE node_identities RENAME COLUMN k8s_node_uid TO incarnation_uid;
ALTER TABLE node_identities
    ADD COLUMN managed_by TEXT NOT NULL DEFAULT 'k8s';

CREATE VIEW node_k8s WITH (security_invoker = true) AS
SELECT node_id,
       name AS k8s_node_name,
       incarnation_uid AS k8s_node_uid,
       updated_at
FROM node_identities
WHERE managed_by = 'k8s'
WITH CASCADED CHECK OPTION;

-- The credential manager's capability role reads four execution columns and
-- three node columns, and the view grants are those columns. Through an
-- invoker view Postgres also checks the caller's privileges on every base
-- column the view's query reads, whichever view columns the caller selects:
-- the whole select list and `managed_by`. The renames kept the base grants
-- the role already held, so these add the rest. The role can then read those
-- base columns directly, a wider fence than the old table's column grants.
GRANT SELECT (sandbox_id, generation, k8s_node_name, active)
    ON sandbox_k8s_executions TO space_credmgr;
GRANT SELECT (
    id, pod_name, pod_namespace, pod_uid, pod_ip, state, created_at,
    updated_at, terminated_at, activation_attempt_id,
    activation_attempt_expires_at, activation_attempt_target_state,
    route_activation_id, materialized_at, runtime_upgrade_operation_id,
    runtime_upgrade_target_environment_ref, runtime_upgrade_authorization_id,
    managed_by
) ON sandbox_execution_records TO space_credmgr;
GRANT SELECT (node_id, k8s_node_name, k8s_node_uid)
    ON node_k8s TO space_credmgr;
GRANT SELECT (updated_at, managed_by) ON node_identities TO space_credmgr;

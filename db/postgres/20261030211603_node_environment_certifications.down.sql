-- Roll back the environment writer and every caller of
-- node_environment_evidence before this migration. The rows are rebuilt from
-- the next complete inventory of each node, so nothing is lost.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DROP FUNCTION node_environment_evidence(node_status_snapshots);
DROP TABLE node_environment_certifications;

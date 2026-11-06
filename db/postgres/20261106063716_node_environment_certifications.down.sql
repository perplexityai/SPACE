-- Roll back the environment writer before this migration. The rows are
-- rebuilt from the next complete inventory of each node, so nothing is lost.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DROP TABLE node_environment_certifications;

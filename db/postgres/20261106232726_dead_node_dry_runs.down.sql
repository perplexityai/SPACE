-- The rows are a report the next dry-run tick rewrites, so nothing is lost.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

DROP TABLE dead_node_dry_runs;

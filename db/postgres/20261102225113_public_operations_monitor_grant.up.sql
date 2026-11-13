SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Let monitoring see whether public-operation retention is keeping up. A
-- database-monitoring query reports how long the oldest finished operation has
-- been past gc_after; without a grant the monitoring login (a pg_monitor
-- member) cannot run it, so the lag metric never reports.
--
-- Column-level, unlike the baseline's whole-table pg_monitor grants:
-- public_operations also carries request payloads, results and claim tokens,
-- and the query needs only these two columns.
GRANT SELECT(state) ON TABLE public.public_operations TO pg_monitor;

GRANT SELECT(gc_after) ON TABLE public.public_operations TO pg_monitor;

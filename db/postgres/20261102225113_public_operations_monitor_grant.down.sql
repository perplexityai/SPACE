SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Reverting removes monitoring's read of public_operations; the retention-lag
-- query then fails and its metric stops reporting.
REVOKE SELECT(state, gc_after) ON TABLE public.public_operations FROM pg_monitor;

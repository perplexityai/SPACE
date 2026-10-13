SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.node_reconcile_queue DROP COLUMN IF EXISTS traceparent;

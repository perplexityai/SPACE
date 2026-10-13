SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- The W3C traceparent of the most recent writer that queued a node reconcile
-- ref with trace context, so the node-ctl pass that applies the ref can link
-- to it. Nullable with no default: adding it rewrites no rows, background and
-- older producers leave it NULL, and older consumers never select it. The
-- application validates the value.
ALTER TABLE public.node_reconcile_queue ADD COLUMN traceparent text;

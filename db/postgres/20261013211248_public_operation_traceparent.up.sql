SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- The W3C traceparent of the request that persisted the operation, so the
-- dispatched execution continues that request's trace. Nullable with no
-- default: adding it rewrites no rows, rows written before this migration or
-- by older producers stay NULL, and older consumers never select it. The
-- application validates the value; it is telemetry context, not operation
-- state.
ALTER TABLE public.public_operations ADD COLUMN traceparent text;

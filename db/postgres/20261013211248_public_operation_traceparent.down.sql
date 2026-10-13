SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE public.public_operations DROP COLUMN IF EXISTS traceparent;

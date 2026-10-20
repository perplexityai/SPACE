SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Safe to run while rows carry a value, but only once the deployment is on a
-- release that doesn't use the column and whose `public_operations` reads
-- name their columns. A release that doesn't use the column never waits on a
-- placed `creating` row, so it has no deletion fence to keep, and the values
-- dropped here fence nothing. A release whose reads still select `*` holds
-- cached statements that fail with `0A000` on every pooled connection that
-- ran them once the column is dropped, until the pool recycles it. Dropping it
-- while a release that uses it is still deployed stops every new create: each
-- attempt that finds no sandbox row, as a create's first attempt does, or
-- finds it placed and still `creating` or being deleted, fails its read or
-- write of the column and retries until its retry budget or the operation's
-- deadline ends it.
ALTER TABLE public.public_operations DROP COLUMN IF EXISTS sandbox_row_observed_at;

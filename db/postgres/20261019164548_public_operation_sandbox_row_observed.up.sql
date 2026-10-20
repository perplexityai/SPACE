SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- When an attempt at a sandbox create, holding the operation's claim, first
-- found the create's sandbox row placed on a node and still `creating` (and
-- began waiting for it) or being deleted. NULL for every other operation and
-- for a create no such attempt saw. The create executor reads it as a
-- deletion fence: a create whose row has since gone doesn't build the sandbox
-- again. Nullable with no default, so adding it rewrites no rows. Releases
-- whose `public_operations` reads name their columns don't see it; being a
-- column, it needs no payload field an older executor's `deny_unknown_fields`
-- would reject.
ALTER TABLE public.public_operations ADD COLUMN sandbox_row_observed_at timestamptz;

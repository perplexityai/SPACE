SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Run only once neither a deployed API nor a deployed internal-api uses these
-- columns: the API's shared sandbox column list reads them on every sandbox
-- read, and internal-api writes them inside the node-publish transaction, so
-- dropping them fails every publication from a node that holds a stopped
-- sandbox.
DROP TRIGGER IF EXISTS sandboxes_clear_stop_cause ON public.sandboxes;
DROP FUNCTION IF EXISTS public.sandboxes_clear_stop_cause();
ALTER TABLE public.sandboxes
    DROP COLUMN IF EXISTS stop_cause_at,
    DROP COLUMN IF EXISTS stop_cause;

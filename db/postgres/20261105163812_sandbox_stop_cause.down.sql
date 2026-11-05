SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Run only once no deployed API selects these columns: its shared sandbox
-- column list reads them on every sandbox read.
DROP TRIGGER IF EXISTS sandboxes_clear_stop_cause ON public.sandboxes;
DROP FUNCTION IF EXISTS public.sandboxes_clear_stop_cause();
ALTER TABLE public.sandboxes
    DROP COLUMN IF EXISTS stop_cause_at,
    DROP COLUMN IF EXISTS stop_cause;

SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Why a stopped sandbox stopped when no one asked it to, as its node
-- published it (`pod_terminated`, `vm_exited`), and when the node stopped it.
-- NULL for a requested stop. The publish ingest's stopped convergence writes
-- both columns; the admin sandbox response reads them.
--
-- `stop_cause_at` is the node's clock for the stop. `lifecycle_changed_at`
-- can't stand in for it: it is the control plane's time of the convergence,
-- a publication after the stop, and every later lifecycle change moves it,
-- a stopped sandbox's evacuation (`stopped` -> `restoring` -> `stopped`)
-- included, which keeps the cause.
--
-- Nullable with no default, backfill, CHECK or index: NULL means no cause, the
-- labels belong to the writer's enum, and nothing filters on either column.
ALTER TABLE public.sandboxes
    ADD COLUMN stop_cause text,
    ADD COLUMN stop_cause_at timestamp with time zone;

CREATE FUNCTION public.sandboxes_clear_stop_cause() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- A cause lasts until the sandbox runs again: a start, wake or restore
    -- that reaches `started` or `paused` ends the stop it describes. Every
    -- other move keeps it, so a stopped sandbox's evacuation and the stop's
    -- own retries don't lose it; a later unexpected stop writes its own cause
    -- when it converges, and a requested one converges with none.
    IF NEW.lifecycle IS DISTINCT FROM OLD.lifecycle
       AND LOWER(TRIM(NEW.lifecycle)) IN ('started', 'paused')
    THEN
        NEW.stop_cause := NULL;
        NEW.stop_cause_at := NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sandboxes_clear_stop_cause BEFORE UPDATE ON public.sandboxes FOR EACH ROW EXECUTE FUNCTION public.sandboxes_clear_stop_cause();

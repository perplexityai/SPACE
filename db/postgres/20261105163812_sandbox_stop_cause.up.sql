SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The node's attribution of the sandbox's last stop, as it published it with
-- a `stopped` status: why the sandbox stopped when no one asked it to
-- (`pod_terminated`, `vm_exited`), and when the node stopped it. A requested
-- stop is attributed too, with a NULL `stop_cause` and its time; both NULL
-- means no attribution is recorded (the sandbox ran since, or its node
-- predates attributions). The publish ingest writes both columns, in a
-- statement of its own, from a newer attribution; the admin sandbox response
-- reads them.
--
-- `stop_cause_at` is the node's clock for the stop. `lifecycle_changed_at`
-- can't stand in for it: it is the control plane's time of the convergence,
-- a publication after the stop, and every later lifecycle change moves it,
-- a stopped sandbox's evacuation (`stopped` -> `restoring` -> `stopped`)
-- included, which keeps the cause.
--
-- Nullable with no default, backfill, CHECK or index: the labels belong to the
-- writer's enum, and nothing filters on either column.
ALTER TABLE public.sandboxes
    ADD COLUMN stop_cause text,
    ADD COLUMN stop_cause_at timestamp with time zone;

CREATE FUNCTION public.sandboxes_clear_stop_cause() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- An attribution lasts until the sandbox runs again: a start, wake or
    -- restore that reaches `started` or `paused` ends the stop it describes.
    -- Every other move keeps it, so a stopped sandbox's evacuation doesn't
    -- lose it; the node's attribution of a later stop replaces it.
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

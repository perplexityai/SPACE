SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- `next_auto_event_at` is the idle deadline the sandbox's node publishes. It
-- moves whenever the sandbox is used, as `last_access_at` and
-- `last_command_at` do, so like them it is an observation of the node rather
-- than a change to the sandbox, and writing it must not advance `updated_at`.
CREATE OR REPLACE FUNCTION public.set_sandbox_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.updated_at IS DISTINCT FROM OLD.updated_at THEN
        RETURN NEW;
    END IF;

    IF (to_jsonb(NEW) - ARRAY['updated_at', 'last_used_at', 'last_access_at', 'last_command_at', 'next_auto_event_at'])
       IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['updated_at', 'last_used_at', 'last_access_at', 'last_command_at', 'next_auto_event_at']) THEN
        NEW.updated_at := NOW();
    ELSE
        NEW.updated_at := OLD.updated_at;
    END IF;

    RETURN NEW;
END;
$$;

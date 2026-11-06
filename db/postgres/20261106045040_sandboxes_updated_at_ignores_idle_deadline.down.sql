SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

CREATE OR REPLACE FUNCTION public.set_sandbox_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.updated_at IS DISTINCT FROM OLD.updated_at THEN
        RETURN NEW;
    END IF;

    IF (to_jsonb(NEW) - ARRAY['updated_at', 'last_used_at', 'last_access_at', 'last_command_at'])
       IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['updated_at', 'last_used_at', 'last_access_at', 'last_command_at']) THEN
        NEW.updated_at := NOW();
    ELSE
        NEW.updated_at := OLD.updated_at;
    END IF;

    RETURN NEW;
END;
$$;

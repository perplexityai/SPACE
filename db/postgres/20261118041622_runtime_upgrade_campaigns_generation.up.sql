SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A campaign's generation, for the admin API's If-Match. An operator acting
-- on a stale read of the campaign then gets a refusal instead of, for example,
-- resuming a campaign the failure budget paused after the read.
--
-- The schema raises it, not the writers: a BEFORE UPDATE trigger sets it to
-- one more than the old row's whenever any other column changes, and keeps the
-- old row's value otherwise. That covers every writer the same way: the
-- operator's pause and resume, the reconciler moving the phase or pausing on
-- the failure budget, binaries from any release, and manual SQL. A value the
-- writer assigns is overridden, so a writer can't skip a raise or set the
-- column back. An UPDATE that changes nothing else leaves it as it was.
--
-- Columns that don't count as a change are excluded by name: generation
-- itself; updated_at, which set_updated_at sets on every update; and
-- created_by_user_id and updated_by_user_id, so the ON DELETE SET NULL
-- cascades from users don't raise it. Every other column counts, including
-- ones added later, so a new column is protected without changing this
-- function. Exempting a column means replacing the function in a new
-- migration. Item progress is in runtime_upgrade_items and doesn't touch the
-- campaign row. Because updated_at is excluded, the order in which this
-- trigger and set_updated_at fire doesn't matter.
--
-- No release before this migration writes runtime_upgrade_campaigns, so the
-- table is empty when it applies. The constant default doesn't rewrite rows,
-- and the CHECK has nothing to scan, so it's validated here rather than left
-- NOT VALID.
ALTER TABLE runtime_upgrade_campaigns
    ADD COLUMN generation BIGINT NOT NULL DEFAULT 0,
    -- squawk-ignore constraint-missing-not-valid
    ADD CONSTRAINT runtime_upgrade_campaigns_generation_check CHECK (generation >= 0);

CREATE FUNCTION runtime_upgrade_campaigns_raise_generation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF (to_jsonb(NEW) - ARRAY['generation', 'updated_at', 'created_by_user_id', 'updated_by_user_id'])
       IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['generation', 'updated_at', 'created_by_user_id', 'updated_by_user_id']) THEN
        NEW.generation := OLD.generation + 1;
    ELSE
        NEW.generation := OLD.generation;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER runtime_upgrade_campaigns_raise_generation
    BEFORE UPDATE ON runtime_upgrade_campaigns
    FOR EACH ROW EXECUTE FUNCTION runtime_upgrade_campaigns_raise_generation();

COMMENT ON COLUMN runtime_upgrade_campaigns.generation IS
    'Raised by one by the runtime_upgrade_campaigns_raise_generation trigger on every update that changes another column (not updated_at or the user columns); the admin API compares it with If-Match';

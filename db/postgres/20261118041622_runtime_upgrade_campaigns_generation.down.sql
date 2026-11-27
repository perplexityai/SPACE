-- Roll back campaign readers and writers before removing the column.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Refuse while any campaign exists. Running the up migration again re-adds the
-- column at its default of 0, so every campaign restarts at generation 0, and
-- an If-Match read before this down would match a campaign that changed since.
-- A campaign at 0 isn't safe either: once the column is gone, nothing raises
-- the generation, so a change made between this down and the reapply also
-- lands back at 0. Only an empty table can't hand a stale value a match. Roll
-- forward instead, or retire every campaign before rolling back past here,
-- finished ones included, which loses their history. Finish or abort each
-- campaign and wait for its items' leases to lapse, as
-- 20261026184921_runtime_upgrade_campaigns.down.sql requires, then delete its
-- row by direct SQL, which cascades to its items. Deleting before the leases
-- lapse removes the items that down's lease check reads while a worker may
-- still be upgrading their subjects.
--
-- The first check runs before the ACCESS EXCLUSIVE lock, so a refusal doesn't
-- block traffic. The second, identical one runs after the lock, which keeps
-- any campaign from being inserted between the check and the drop. That
-- relies on READ COMMITTED, Postgres's default, which the migrator doesn't
-- override: it gives the second check a fresh snapshot. A failed check rolls
-- the whole migration back.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_upgrade_campaigns) THEN
        RAISE EXCEPTION 'runtime_upgrade_campaigns has campaigns, and rolling '
            'back this migration would restart their generations at 0, so a '
            'stale If-Match could match a campaign that changed; roll forward, '
            'or retire every campaign before rolling back this migration';
    END IF;
END;
$$;

LOCK TABLE runtime_upgrade_campaigns IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_upgrade_campaigns) THEN
        RAISE EXCEPTION 'runtime_upgrade_campaigns has campaigns, and rolling '
            'back this migration would restart their generations at 0, so a '
            'stale If-Match could match a campaign that changed; roll forward, '
            'or retire every campaign before rolling back this migration';
    END IF;
END;
$$;

-- Dropping the column doesn't drop the trigger, whose function assigns
-- NEW.generation, so every later update of a campaign would fail. Drop the
-- trigger and its function first.
DROP TRIGGER runtime_upgrade_campaigns_raise_generation ON runtime_upgrade_campaigns;
DROP FUNCTION runtime_upgrade_campaigns_raise_generation();

-- Dropping the column drops its CHECK.
ALTER TABLE runtime_upgrade_campaigns
    DROP COLUMN generation;

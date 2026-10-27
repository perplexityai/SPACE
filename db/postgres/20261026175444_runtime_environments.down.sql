-- Roll back catalog readers and writers before dropping the table.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Refuse while any environment is retired. The other columns come back as
-- nodes publish again, apart from created_at, but retirement is an operator
-- decision: dropping it would bring a retired environment back into usable
-- sets after the next up.
-- Clearing retired_at needs direct SQL by a cluster administrator. Holding the
-- lock keeps a retirement from landing between the check and the drop.
LOCK TABLE runtime_environments IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_environments WHERE retired_at IS NOT NULL) THEN
        RAISE EXCEPTION 'runtime_environments has retired environments, and '
            'dropping it would discard their retirement; record them, then set '
            'retired_at to NULL on every row before rolling back this migration';
    END IF;
END;
$$;

DROP TABLE runtime_environments;

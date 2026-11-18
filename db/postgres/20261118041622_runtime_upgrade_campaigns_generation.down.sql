-- Roll back campaign readers and writers before removing the column. Running
-- the up migration again restarts every campaign at generation 0, so an
-- If-Match read before this down can match a campaign that changed since.
SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- Dropping the column doesn't drop the trigger, whose function assigns
-- NEW.generation, so every later update of a campaign would fail. Drop the
-- trigger and its function first.
DROP TRIGGER runtime_upgrade_campaigns_raise_generation ON runtime_upgrade_campaigns;
DROP FUNCTION runtime_upgrade_campaigns_raise_generation();

-- Dropping the column drops its CHECK.
ALTER TABLE runtime_upgrade_campaigns
    DROP COLUMN generation;

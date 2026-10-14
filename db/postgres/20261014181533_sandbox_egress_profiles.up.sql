SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

-- Selected operator profile names, fixed at create and preserved by tenant
-- policy writes. An empty array means none; the constant default avoids
-- rewriting existing rows.
ALTER TABLE sandboxes
    ADD COLUMN egress_profiles TEXT[] NOT NULL DEFAULT '{}'::text[];

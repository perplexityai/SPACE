SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- The resource an admin request acted on, recorded by handlers that act on
-- another organization's resources: the organization that owns it, what kind
-- of resource it is (for example `api_key` or `sandbox`), and its id. The
-- request's method and path stay the rest of the record.
--
-- Nullable with no default, backfill or index: rows written before this
-- migration, and requests whose handler names no target, leave all three NULL.
-- The application owns the set of target types, so no CHECK lists them.
ALTER TABLE admin_audit_events
    ADD COLUMN target_org_id uuid,
    ADD COLUMN target_type text,
    ADD COLUMN target_id text;

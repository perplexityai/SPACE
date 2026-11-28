SET LOCAL lock_timeout = '200ms';
SET LOCAL statement_timeout = '5s';

-- Restore the foreign keys into `sandboxes`. The tables are empty and nothing
-- else uses them: a later migration's down re-created them, so each check
-- validates no rows. Re-adding a key takes SHARE ROW EXCLUSIVE on `sandboxes`,
-- which blocks its writers while this waits, so take it first under the same
-- 200ms bound as the up. A down is not retried: if the lock timeout fires, run
-- it again.
LOCK TABLE sandboxes IN SHARE ROW EXCLUSIVE MODE;
-- squawk-ignore adding-foreign-key-constraint
ALTER TABLE sandbox_encrypted_fields ADD CONSTRAINT sandbox_encrypted_fields_sandbox_id_org_id_fkey FOREIGN KEY (sandbox_id, org_id) REFERENCES public.sandboxes(id, org_id) ON DELETE CASCADE;

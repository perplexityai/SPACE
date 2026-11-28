SET LOCAL lock_timeout = '200ms';
SET LOCAL statement_timeout = '5s';

-- Restore the foreign keys into `org_master_keys`. The tables are empty and
-- nothing else uses them: a later migration's down re-created them, so each
-- check validates no rows. Re-adding a key takes SHARE ROW EXCLUSIVE on
-- `org_master_keys`, which blocks its writers while this waits, so take it
-- first under the same 200ms bound as the up. A down is not retried: if the
-- lock timeout fires, run it again.
LOCK TABLE org_master_keys IN SHARE ROW EXCLUSIVE MODE;
-- squawk-ignore adding-foreign-key-constraint
ALTER TABLE sandbox_encrypted_fields ADD CONSTRAINT sandbox_encrypted_fields_master_key_id_org_id_fkey FOREIGN KEY (master_key_id, org_id) REFERENCES public.org_master_keys(id, org_id) ON DELETE RESTRICT;

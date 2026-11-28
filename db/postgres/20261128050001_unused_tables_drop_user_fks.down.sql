SET LOCAL lock_timeout = '200ms';
SET LOCAL statement_timeout = '5s';

-- Restore the foreign keys into `users`. The tables are empty and nothing else
-- uses them: a later migration's down re-created them, so each check validates
-- no rows. Re-adding a key takes SHARE ROW EXCLUSIVE on `users`, which blocks
-- its writers while this waits, so take it first under the same 200ms bound as
-- the up. A down is not retried: if the lock timeout fires, run it again.
LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE;
-- squawk-ignore adding-foreign-key-constraint
ALTER TABLE user_credentials ADD CONSTRAINT user_credentials_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;
-- squawk-ignore adding-foreign-key-constraint
ALTER TABLE sandbox_archives ADD CONSTRAINT sandbox_archives_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE RESTRICT;

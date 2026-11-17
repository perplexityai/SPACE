SET LOCAL lock_timeout = '1s';
SET LOCAL statement_timeout = '5s';

-- A campaign's generation, for the admin API's If-Match. Writers must raise it
-- by exactly one in the same transaction as every change they make to a
-- campaign row, whoever makes it: an operator's pause or resume, or the
-- reconciler moving the phase or pausing on the failure budget. The ON DELETE
-- SET NULL cascades on created_by_user_id and updated_by_user_id don't raise
-- it; they change no field If-Match protects. Item progress doesn't touch the
-- campaign row. An operator acting on a stale read of the campaign then gets a
-- refusal instead of, for example, resuming a campaign the failure budget
-- paused after the read. Nothing in the schema raises it: the campaign store
-- does, and a write that bypasses the store, such as manual SQL, must too.
--
-- No code writes runtime_upgrade_campaigns yet, so the table is empty. The
-- constant default doesn't rewrite rows, and the CHECK has nothing to scan, so
-- it's validated here rather than left NOT VALID.
ALTER TABLE runtime_upgrade_campaigns
    ADD COLUMN generation BIGINT NOT NULL DEFAULT 0,
    -- squawk-ignore constraint-missing-not-valid
    ADD CONSTRAINT runtime_upgrade_campaigns_generation_check CHECK (generation >= 0);

COMMENT ON COLUMN runtime_upgrade_campaigns.generation IS
    'Writers raise it by exactly one in the same transaction as every change they make to the row (user-deletion cascades do not); the admin API compares it with If-Match';

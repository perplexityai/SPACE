//! Test-only schema helper for the `runtime_environment_states` memory-retirement
//! columns (`db/postgres/20261124192451_runtime_environment_states_memory_retired.{up,down}.sql`).
//!
//! Compiled for this crate's tests and behind the `test-support` feature;
//! enable it only from `[dev-dependencies]`. While the migration lands in its
//! own PR, isolated test databases apply or revert the exact columns,
//! constraints, comments and narrowed `updated_at` trigger here so runtime
//! suites exercise both the pre-migration schema and the migrated schema before
//! and after the migration merges to `main`.

use pplx_space_sql_locator::sql;
use sqlx::{Executor, PgPool};

/// Apply the `runtime_environment_states` memory-retirement columns, `FOREIGN KEY`
/// and `CHECK` constraints, comments and narrowed `updated_at` trigger
/// idempotently to an isolated test database, matching
/// `20261124192451_runtime_environment_states_memory_retired.up.sql`.
pub async fn apply_memory_retirement_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    pool.execute(sql!(
        "ALTER TABLE runtime_environment_states
             ADD COLUMN IF NOT EXISTS memory_retired BOOLEAN NOT NULL DEFAULT FALSE,
             ADD COLUMN IF NOT EXISTS memory_updated_at TIMESTAMPTZ,
             ADD COLUMN IF NOT EXISTS memory_updated_by_user_id UUID,
             ADD COLUMN IF NOT EXISTS memory_reason TEXT;

         DO $$
         BEGIN
             IF NOT EXISTS (
                 SELECT 1
                 FROM pg_constraint
                 WHERE conrelid = 'runtime_environment_states'::regclass
                   AND conname = 'runtime_environment_states_memory_updated_by_user_id_fkey'
             ) THEN
                 ALTER TABLE runtime_environment_states
                     ADD CONSTRAINT runtime_environment_states_memory_updated_by_user_id_fkey
                         FOREIGN KEY (memory_updated_by_user_id) REFERENCES users(id) ON DELETE SET NULL
                         NOT VALID;
             END IF;

             IF NOT EXISTS (
                 SELECT 1
                 FROM pg_constraint
                 WHERE conrelid = 'runtime_environment_states'::regclass
                   AND conname = 'runtime_environment_states_memory_state_check'
             ) THEN
                 ALTER TABLE runtime_environment_states
                     ADD CONSTRAINT runtime_environment_states_memory_state_check
                         CHECK (NOT memory_retired OR state <> 'allowed')
                         NOT VALID;
             END IF;

             IF NOT EXISTS (
                 SELECT 1
                 FROM pg_constraint
                 WHERE conrelid = 'runtime_environment_states'::regclass
                   AND conname = 'runtime_environment_states_memory_change_check'
             ) THEN
                 ALTER TABLE runtime_environment_states
                     ADD CONSTRAINT runtime_environment_states_memory_change_check
                         CHECK (
                             (
                                 NOT memory_retired
                                 AND memory_updated_at IS NULL
                                 AND memory_updated_by_user_id IS NULL
                                 AND memory_reason IS NULL
                             )
                             OR (
                                 memory_updated_at IS NOT NULL
                                 AND memory_reason IS NOT NULL
                                 AND char_length(memory_reason) BETWEEN 1 AND 1024
                             )
                         )
                         NOT VALID;
             END IF;
         END $$;

         CREATE OR REPLACE TRIGGER runtime_environment_states_set_updated_at
             BEFORE UPDATE OF state, updated_by_user_id, reason
             ON runtime_environment_states
             FOR EACH ROW EXECUTE FUNCTION set_updated_at();

         COMMENT ON COLUMN runtime_environment_states.memory_retired IS
             'Whether checkpoint memory from this environment is retired; never TRUE while state is allowed';
         COMMENT ON COLUMN runtime_environment_states.memory_updated_at IS
             'When memory_retired was last changed; NULL before any change';
         COMMENT ON COLUMN runtime_environment_states.memory_updated_by_user_id IS
             'Actor recorded for the last memory_retired change; NULL when unattributed, after user deletion, or before any change';
         COMMENT ON COLUMN runtime_environment_states.memory_reason IS
             'Reason (1..=1024 characters) for the last memory_retired change; NULL before any change';"
    ))
    .await?;
    Ok(())
}

/// Revert an isolated test database to the pre-migration
/// `runtime_environment_states` shape, matching
/// `20261124192451_runtime_environment_states_memory_retired.down.sql` so
/// pre-migration compatibility tests keep exercising the unmigrated schema once
/// the migration merges.
pub async fn revert_memory_retirement_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    pool.execute(sql!(
        "CREATE OR REPLACE TRIGGER runtime_environment_states_set_updated_at
             BEFORE UPDATE ON runtime_environment_states
             FOR EACH ROW EXECUTE FUNCTION set_updated_at();

         ALTER TABLE runtime_environment_states
             DROP COLUMN IF EXISTS memory_retired,
             DROP COLUMN IF EXISTS memory_updated_at,
             DROP COLUMN IF EXISTS memory_updated_by_user_id,
             DROP COLUMN IF EXISTS memory_reason;"
    ))
    .await?;
    Ok(())
}

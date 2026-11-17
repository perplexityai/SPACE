//! Node-manager's durable retained-device release receipts.
//! The caller owns the connection lock; retiring an execution does not delete
//! its receipt or authorize a replacement execution to overwrite it.

use rusqlite::{Connection, OptionalExtension, params};

pub struct RetainedBlockBinding {
    pub execution_id: String,
    pub binding: Vec<u8>,
}

pub fn retained_block_binding(
    db: &Connection,
    sandbox: &str,
) -> Result<Option<RetainedBlockBinding>, rusqlite::Error> {
    db.query_row(
        "SELECT execution_id, binding FROM retained_block_bindings WHERE sandbox_id = ?1",
        params![sandbox],
        |row| {
            Ok(RetainedBlockBinding {
                execution_id: row.get(0)?,
                binding: row.get(1)?,
            })
        },
    )
    .optional()
}

pub fn save_retained_block_binding(
    db: &Connection,
    sandbox: &str,
    execution: &str,
    binding: &[u8],
) -> Result<bool, rusqlite::Error> {
    Ok(db.execute(
        "INSERT INTO retained_block_bindings (sandbox_id, execution_id, binding)
         SELECT ?1, ?2, ?3 WHERE EXISTS (
             SELECT 1 FROM sandbox_executions WHERE sandbox_id = ?1 AND execution_id = ?2)
         ON CONFLICT (sandbox_id) DO UPDATE SET binding = excluded.binding
         WHERE retained_block_bindings.execution_id = excluded.execution_id",
        params![sandbox, execution, binding],
    )? == 1)
}

pub fn clear_retained_block_binding(
    db: &Connection,
    sandbox: &str,
    execution: &str,
) -> Result<(), rusqlite::Error> {
    db.execute(
        "DELETE FROM retained_block_bindings WHERE sandbox_id = ?1 AND execution_id = ?2",
        params![sandbox, execution],
    )?;
    Ok(())
}

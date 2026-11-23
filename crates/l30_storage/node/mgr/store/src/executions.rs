//! `sandbox_executions`: each sandbox's durable execution fence.

use pplx_space_node_mgr_protocol::execution_fence::ExecutionFence;
use pplx_space_sandbox_model::SandboxActivationAttemptId;
use pplx_space_sql_locator::sql;
use rusqlite::{Connection, OptionalExtension as _, params};

/// The fence recorded for `sandbox_id`. An unparsable identity is a
/// conversion failure of column 0.
pub fn execution_fence(
    conn: &Connection,
    sandbox_id: &str,
) -> rusqlite::Result<Option<ExecutionFence>> {
    let raw: Option<(String, Option<String>)> = conn
        .query_row(
            sql!("SELECT execution_id, previous_id FROM sandbox_executions WHERE sandbox_id = ?1"),
            params![sandbox_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let parse = |raw: String| {
        raw.parse::<SandboxActivationAttemptId>().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    };
    raw.map(|(current, previous)| {
        Ok(ExecutionFence::new(
            parse(current)?,
            previous.map(parse).transpose()?,
        ))
    })
    .transpose()
}

/// Record `fence` as `sandbox_id`'s execution fence, replacing the one before.
pub fn bind_execution(
    conn: &Connection,
    sandbox_id: &str,
    fence: ExecutionFence,
) -> rusqlite::Result<()> {
    conn.execute(
        sql!(
            "INSERT INTO sandbox_executions (sandbox_id, execution_id, previous_id) VALUES (?1, ?2, ?3)
             ON CONFLICT (sandbox_id) DO UPDATE SET execution_id = excluded.execution_id,
                                                   previous_id = excluded.previous_id"
        ),
        params![sandbox_id, fence.current().to_string(), fence.previous().map(|id| id.to_string())],
    )?;
    Ok(())
}

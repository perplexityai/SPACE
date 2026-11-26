//! Writes of a node's drain columns that check only its id. The caller decides
//! what to write and holds whatever lock makes that decision current, such as
//! the admin handlers' `FOR UPDATE` of the node's row; node drain's phase
//! compare-and-sets (`set_drain_phase`) stay with their caller.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::NodeDrainMode;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

/// Write `message` into `node_id`'s `drain_last_error`, for the node-drain
/// reconciler to pick up. Returns whether a row was written.
pub async fn set_drain_last_error(
    executor: impl Executor<'_>,
    node_id: NodeId,
    message: &str,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!("UPDATE nodes SET drain_last_error = $1 WHERE id = $2"))
        .bind(message)
        .bind(node_id)
        .execute(executor.into_sqlx())
        .await?;
    Ok(result.rows_affected() > 0)
}

/// The drain columns [`update_drain_columns`] writes. It always writes
/// `drain_phase` and clears `drain_last_error`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainColumnsUpdate {
    pub phase: Option<&'static str>,
    /// Stamp `drain_started_at` with the database clock.
    pub set_started_at: bool,
    /// Clear `drain_started_at` and `drain_mode`. `set_started_at` wins over
    /// it for `drain_started_at`.
    pub clear_started_at: bool,
    /// `None` keeps `terminate_decrement_capacity`.
    pub terminate_decrement_capacity: Option<bool>,
    /// `None` keeps `drain_mode` unless `clear_started_at` clears it.
    pub drain_mode: Option<NodeDrainMode>,
}

/// Write `update` into `node_id`'s drain columns. Returns whether a row was
/// written.
pub async fn update_drain_columns(
    executor: impl Executor<'_>,
    node_id: NodeId,
    update: DrainColumnsUpdate,
) -> Result<bool, DbError> {
    let DrainColumnsUpdate {
        phase,
        set_started_at,
        clear_started_at,
        terminate_decrement_capacity,
        drain_mode,
    } = update;
    let result = sqlx::query(sql!(
        "UPDATE nodes
            SET drain_phase = $2,
                drain_started_at = CASE
                    WHEN $3 THEN NOW()
                    WHEN $4 THEN NULL
                    ELSE drain_started_at
                END,
                drain_last_error = NULL,
                terminate_decrement_capacity = COALESCE($5, terminate_decrement_capacity),
                drain_mode = CASE
                    WHEN $4 THEN NULL
                    ELSE COALESCE($6, drain_mode)
                END
          WHERE id = $1"
    ))
    .bind(node_id)
    .bind(phase)
    .bind(set_started_at)
    .bind(clear_started_at)
    .bind(terminate_decrement_capacity)
    .bind(drain_mode)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

//! `sandbox_port_exposures`: one row per `(sandbox, protocol, guest port)`,
//! the port a caller asked to open or close and what its node last applied.
//!
//! Every verb is one statement on the executor its caller passes, usually
//! inside a transaction that has already locked the `sandboxes` row; none
//! begins or commits a transaction. An upsert that inserts a new row also
//! takes the foreign key's `FOR KEY SHARE` lock on the `sandboxes` row.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
use sqlx::PgExecutor;
use time::OffsetDateTime;

/// One `sandbox_port_exposures` row, as the open and close upserts return it.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct PortExposureRecord {
    pub sandbox_id: SandboxId,
    pub protocol: String,
    pub guest_port: i32,
    pub visibility: String,
    pub source: String,
    pub desired_state: String,
    pub apply_state: String,
    pub last_applied_node_id: Option<NodeId>,
    pub last_error: Option<String>,
    pub last_bridge_status: Option<String>,
    pub last_applied_bridge_generation: Option<String>,
    pub last_observed_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// Record that `protocol`/`guest_port` should be open with `visibility` and
/// `source` (an intent), queueing it for the node to apply. An existing row
/// keeps its last-applied observation; only its error is cleared.
pub async fn upsert_open(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
    visibility: &str,
    source: &str,
) -> Result<PortExposureRecord, sqlx::Error> {
    sqlx::query_as::<_, PortExposureRecord>(sql!(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id,
             protocol,
             guest_port,
             visibility,
             source,
             desired_state,
             apply_state,
             last_applied_node_id,
             last_error
         )
         VALUES ($1, $2, $3, $4, $5, 'open', 'pending', NULL, NULL)
         ON CONFLICT (sandbox_id, protocol, guest_port) DO UPDATE
         SET visibility = EXCLUDED.visibility,
             source = EXCLUDED.source,
             desired_state = 'open',
             apply_state = 'pending',
             last_error = NULL,
             updated_at = NOW()
         RETURNING sandbox_id,
                   protocol,
                   guest_port,
                   visibility,
                   source,
                   desired_state,
                   apply_state,
                   last_applied_node_id,
                   last_error,
                   last_bridge_status,
                   last_applied_bridge_generation,
                   last_observed_at,
                   created_at,
                   updated_at"
    ))
    .bind(sandbox_id)
    .bind(protocol)
    .bind(guest_port)
    .bind(visibility)
    .bind(source)
    .fetch_one(executor)
    .await
}

/// Record that `protocol`/`guest_port` should be closed (an intent), queueing
/// it for the node to apply. A port never opened gets a `private`, `api` row
/// so the close is still applied.
pub async fn upsert_closed(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
) -> Result<PortExposureRecord, sqlx::Error> {
    sqlx::query_as::<_, PortExposureRecord>(sql!(
        "INSERT INTO sandbox_port_exposures (
             sandbox_id,
             protocol,
             guest_port,
             visibility,
             source,
             desired_state,
             apply_state,
             last_applied_node_id,
             last_error
         )
         VALUES ($1, $2, $3, 'private', 'api', 'closed', 'pending', NULL, NULL)
         ON CONFLICT (sandbox_id, protocol, guest_port) DO UPDATE
         SET desired_state = 'closed',
             apply_state = 'pending',
             last_error = NULL,
             updated_at = NOW()
         RETURNING sandbox_id,
                   protocol,
                   guest_port,
                   visibility,
                   source,
                   desired_state,
                   apply_state,
                   last_applied_node_id,
                   last_error,
                   last_bridge_status,
                   last_applied_bridge_generation,
                   last_observed_at,
                   created_at,
                   updated_at"
    ))
    .bind(sandbox_id)
    .bind(protocol)
    .bind(guest_port)
    .fetch_one(executor)
    .await
}

/// Forget what the node last applied for one open TCP exposure after the
/// proxy failed to reach it, recording `reason` as its error and queueing it
/// to be applied again. Returns whether an open exposure matched.
pub async fn invalidate_open_tcp_exposure_after_proxy_failure(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
    guest_port: i32,
    reason: &str,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "UPDATE sandbox_port_exposures
         SET apply_state = 'pending',
             last_applied_node_id = NULL,
             last_error = $3,
             last_bridge_status = NULL,
             last_applied_bridge_generation = NULL,
             last_observed_at = NULL,
             updated_at = NOW()
         WHERE sandbox_id = $1
           AND protocol = 'tcp'
           AND guest_port = $2
           AND desired_state = 'open'"
    ))
    .bind(sandbox_id)
    .bind(guest_port)
    .bind(reason)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Forget what any node last applied for the sandbox's open exposures and
/// queue them to be applied again (an operation step). Callers run it in
/// the transaction that records the end of the runtime those observations
/// described, such as an execution finished or retired, a quarantine
/// recovered, a failed row repaired or an activation rolled back.
/// Transitions written as one statement in [`crate::suspend`],
/// [`crate::orphan`] and [`crate::ingest`] reset the same columns in a CTE
/// of their own, so change them together.
/// [`crate::lost::write_off_lost_sandboxes`] leaves exposures alone.
pub async fn invalidate_open_observations(
    executor: impl PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandbox_port_exposures
         SET apply_state = 'pending',
             last_applied_node_id = NULL,
             last_error = NULL,
             last_bridge_status = NULL,
             last_applied_bridge_generation = NULL,
             last_observed_at = NULL,
             updated_at = NOW()
         WHERE sandbox_id = $1
           AND desired_state = 'open'"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// Every port exposure of `sandbox_id`, by guest port then protocol.
pub async fn list(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<PortExposureRecord>, sqlx::Error> {
    sqlx::query_as::<_, PortExposureRecord>(sql!(
        "SELECT sandbox_id,
                    protocol,
                    guest_port,
                    visibility,
                    source,
                    desired_state,
                    apply_state,
                    last_applied_node_id,
                    last_error,
                    last_bridge_status,
                    last_applied_bridge_generation,
                    last_observed_at,
                    created_at,
                    updated_at
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
             ORDER BY guest_port ASC, protocol ASC"
    ))
    .bind(sandbox_id)
    .fetch_all(executor)
    .await
}

/// The exposures of `sandbox_id` whose desired state is `open`, by guest port
/// then protocol.
pub async fn list_open(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<PortExposureRecord>, sqlx::Error> {
    sqlx::query_as::<_, PortExposureRecord>(sql!(
        "SELECT sandbox_id,
                    protocol,
                    guest_port,
                    visibility,
                    source,
                    desired_state,
                    apply_state,
                    last_applied_node_id,
                    last_error,
                    last_bridge_status,
                    last_applied_bridge_generation,
                    last_observed_at,
                    created_at,
                    updated_at
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
               AND desired_state = 'open'
             ORDER BY guest_port ASC, protocol ASC"
    ))
    .bind(sandbox_id)
    .fetch_all(executor)
    .await
}

/// The TCP exposure of `guest_port` on `sandbox_id`, in any desired state;
/// `None` when there is none.
pub async fn get_tcp(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    guest_port: i32,
) -> Result<Option<PortExposureRecord>, sqlx::Error> {
    sqlx::query_as::<_, PortExposureRecord>(sql!(
        "SELECT sandbox_id,
                    protocol,
                    guest_port,
                    visibility,
                    source,
                    desired_state,
                    apply_state,
                    last_applied_node_id,
                    last_error,
                    last_bridge_status,
                    last_applied_bridge_generation,
                    last_observed_at,
                    created_at,
                    updated_at
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
               AND protocol = 'tcp'
               AND guest_port = $2"
    ))
    .bind(sandbox_id)
    .bind(guest_port)
    .fetch_optional(executor)
    .await
}

/// Whether any exposure of `sandbox_id` wants to be open.
pub async fn any_open(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM sandbox_port_exposures
             WHERE sandbox_id = $1 AND desired_state = 'open'
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(executor)
    .await
}

/// Whether `sandbox_id` already wants `protocol`/`guest_port` open.
pub async fn open_exists(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
                 SELECT 1 FROM sandbox_port_exposures
                 WHERE sandbox_id = $1
                   AND protocol = $2
                   AND guest_port = $3
                   AND desired_state = 'open'
             )"
    ))
    .bind(sandbox_id)
    .bind(protocol)
    .bind(guest_port)
    .fetch_one(executor)
    .await
}

/// How many exposures of `sandbox_id` other than `protocol`/`guest_port` want
/// to be open: the slots the capacity check counts against an open of that
/// port.
pub async fn count_other_open(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
               AND desired_state = 'open'
               AND NOT (protocol = $2 AND guest_port = $3)"
    ))
    .bind(sandbox_id)
    .bind(protocol)
    .bind(guest_port)
    .fetch_one(executor)
    .await
}

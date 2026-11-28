//! `sandbox_port_exposures`: one row per `(sandbox, protocol, guest port)`,
//! the port a caller asked to open or close and what its node last applied.
//!
//! Every verb is one statement on the executor its caller passes, usually
//! inside a transaction that has already locked the `sandboxes` row; none
//! begins or commits a transaction. An upsert that inserts a new row also
//! takes the foreign key's `FOR KEY SHARE` lock on the `sandboxes` row.
//!
//! The verbs: the intents [`upsert_open`] and [`upsert_closed`]; the apply
//! outcomes, [`mark_applied`] and [`mark_failed`] for an apply the api sends
//! the node and [`record_apply_states`] for the states the node publishes;
//! the invalidations [`invalidate_open_tcp_exposure_after_proxy_failure`]
//! and [`invalidate_open_observations`]; and the reads [`list`],
//! [`list_open`], [`get_tcp`], [`any_open`], [`open_exists`],
//! [`count_other_open`] and [`to_apply_on_node`].
//!
//! [`mark_applied`] and [`mark_failed`] record a node's apply outcome as
//! guarded transitions: each re-checks in its `WHERE` that the exposure is
//! still open, and the failure also that no fresh success on the same node
//! has landed. Matching no row is the losing writer's normal outcome.
//! [`record_apply_states`] is the node's report of what it applied, fenced
//! on the sandbox being active on that node; [`to_apply_on_node`] is the read
//! that tells the node what to apply.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sql_locator::sql;
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
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
    visibility: &str,
    source: &str,
) -> Result<PortExposureRecord, DbError> {
    Ok(sqlx::query_as::<_, PortExposureRecord>(sql!(
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
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Record that `protocol`/`guest_port` should be closed (an intent), queueing
/// it for the node to apply. A port never opened gets a `private`, `api` row
/// so the close is still applied.
pub async fn upsert_closed(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
) -> Result<PortExposureRecord, DbError> {
    Ok(sqlx::query_as::<_, PortExposureRecord>(sql!(
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
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// What a node reported about an exposure's in-guest bridge when it applied
/// one: the bridge status and, when the guest agent sent one, its service
/// generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BridgeObservation<'a> {
    pub bridge_status: &'a str,
    pub service_generation: Option<&'a str>,
}

/// Observed projection: records that `node_id` applied the open TCP exposure
/// of `guest_port`, with the `observation` it reported, observed now by the
/// database clock. Returns the updated row, or
/// `None` when the exposure is no longer open (or never existed).
///
/// Guarded by `desired_state = 'open'`: a close that landed while the node
/// was applying the open wins.
pub async fn mark_applied(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    guest_port: i32,
    node_id: NodeId,
    observation: BridgeObservation<'_>,
) -> Result<Option<PortExposureRecord>, DbError> {
    Ok(sqlx::query_as::<_, PortExposureRecord>(sql!(
        "UPDATE sandbox_port_exposures
         SET apply_state = 'applied',
             last_applied_node_id = $3,
             last_error = NULL,
             last_bridge_status = $4,
             last_applied_bridge_generation = $5,
             last_observed_at = NOW(),
             updated_at = NOW()
         WHERE sandbox_id = $1
           AND protocol = 'tcp'
           AND guest_port = $2
           AND desired_state = 'open'
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
    .bind(guest_port)
    .bind(node_id)
    .bind(observation.bridge_status)
    .bind(observation.service_generation)
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Observed projection: records that applying the open TCP exposure of
/// `guest_port` on `node_id` failed with `error`. A reported `observation`
/// replaces the stored bridge status, and its service generation when it has
/// one, and marks the row observed now by the database clock.
/// Returns whether a row was marked.
///
/// Guarded by `desired_state = 'open'`, and by the row not already being
/// applied on `node_id` with a working bridge (`direct` or
/// `loopback_forwarded`) observed at most `freshness_secs` ago: a fresh
/// success there wins over this failure.
pub async fn mark_failed(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    guest_port: i32,
    node_id: NodeId,
    error: &str,
    observation: Option<BridgeObservation<'_>>,
    freshness_secs: i64,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "UPDATE sandbox_port_exposures
         SET apply_state = 'failed',
             last_error = $4,
             last_bridge_status = COALESCE($5::TEXT, last_bridge_status),
             last_applied_bridge_generation =
                 COALESCE($6::TEXT, last_applied_bridge_generation),
             last_observed_at =
                 CASE WHEN $5::TEXT IS NOT NULL THEN NOW() ELSE last_observed_at END,
             updated_at = NOW()
         WHERE sandbox_id = $1
           AND protocol = 'tcp'
           AND guest_port = $2
           AND desired_state = 'open'
           AND NOT (
               apply_state = 'applied'
               AND last_applied_node_id IS NOT DISTINCT FROM $3
               AND last_bridge_status IN ('direct', 'loopback_forwarded')
               AND last_observed_at >= NOW() - ($7::BIGINT * INTERVAL '1 second')
           )"
    ))
    .bind(sandbox_id)
    .bind(guest_port)
    .bind(node_id)
    .bind(error)
    .bind(observation.map(|observation| observation.bridge_status))
    .bind(observation.and_then(|observation| observation.service_generation))
    .bind(freshness_secs)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Forget what the node last applied for one open TCP exposure after the
/// proxy failed to reach it, recording `reason` as its error and queueing it
/// to be applied again. Returns whether an open exposure matched.
pub async fn invalidate_open_tcp_exposure_after_proxy_failure(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    guest_port: i32,
    reason: &str,
) -> Result<bool, DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Forget what any node last applied for the sandbox's open exposures and
/// queue them to be applied again (an operation step). Callers run it in
/// the transaction that records the end of the runtime those observations
/// described, such as an execution finished or retired, a quarantine
/// recovered, a failed row repaired or an activation rolled back.
/// Transitions written as one statement in [`crate::suspend`],
/// [`crate::orphan`], [`crate::ingest`] and
/// [`crate::direct::service_route_reset`] reset the same columns in a CTE
/// of their own, so change them together.
/// [`crate::lost::write_off_lost_sandboxes`] leaves exposures alone. The
/// caller must pass its open transaction.
pub async fn invalidate_open_observations<C>(
    tx: &mut C,
    sandbox_id: SandboxId,
) -> Result<(), DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
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
    .execute(tx.into_sqlx())
    .await?;
    Ok(())
}

/// Every port exposure of `sandbox_id`, by guest port then protocol.
pub async fn list(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<PortExposureRecord>, DbError> {
    Ok(sqlx::query_as::<_, PortExposureRecord>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The exposures of `sandbox_id` whose desired state is `open`, by guest port
/// then protocol.
pub async fn list_open(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Vec<PortExposureRecord>, DbError> {
    Ok(sqlx::query_as::<_, PortExposureRecord>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// The TCP exposure of `guest_port` on `sandbox_id`, in any desired state;
/// `None` when there is none.
pub async fn get_tcp(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    guest_port: i32,
) -> Result<Option<PortExposureRecord>, DbError> {
    Ok(sqlx::query_as::<_, PortExposureRecord>(sql!(
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
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// Whether any exposure of `sandbox_id` wants to be open.
pub async fn any_open(executor: impl Executor<'_>, sandbox_id: SandboxId) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
        "SELECT EXISTS (
             SELECT 1 FROM sandbox_port_exposures
             WHERE sandbox_id = $1 AND desired_state = 'open'
         )"
    ))
    .bind(sandbox_id)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// Whether `sandbox_id` already wants `protocol`/`guest_port` open.
pub async fn open_exists(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(sql!(
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
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// How many exposures of `sandbox_id` other than `protocol`/`guest_port` want
/// to be open: the slots the capacity check counts against an open of that
/// port.
pub async fn count_other_open(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    protocol: &str,
    guest_port: i32,
) -> Result<i64, DbError> {
    Ok(sqlx::query_scalar::<_, i64>(sql!(
        "SELECT COUNT(*)
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1
               AND desired_state = 'open'
               AND NOT (protocol = $2 AND guest_port = $3)"
    ))
    .bind(sandbox_id)
    .bind(protocol)
    .bind(guest_port)
    .fetch_one(executor.into_sqlx())
    .await?)
}

/// One exposure a node is asked to apply: its port and desired state.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct PortExposureToApply {
    pub sandbox_id: SandboxId,
    pub protocol: String,
    pub guest_port: i32,
    pub desired_state: String,
}

/// The non-ECW exposures of those `sandbox_ids` active on `node_id` that the
/// node still has to apply, by sandbox, guest port, then protocol. A closed
/// exposure is included until it is applied; an open one also while the
/// node's last observation of it is missing, unhealthy, more than 60 seconds
/// old on the database clock, or from another node.
pub async fn to_apply_on_node(
    executor: impl Executor<'_>,
    sandbox_ids: &[SandboxId],
    node_id: NodeId,
) -> Result<Vec<PortExposureToApply>, DbError> {
    Ok(sqlx::query_as::<_, PortExposureToApply>(sql!(
        "SELECT spe.sandbox_id,
                    spe.protocol,
                    spe.guest_port,
                    spe.desired_state
             FROM sandbox_port_exposures spe
             JOIN sandboxes s ON s.id = spe.sandbox_id
             WHERE spe.sandbox_id = ANY($1)
               AND s.active_node_id = $2
               AND spe.source <> 'ecw'
               AND (
                   spe.apply_state != 'applied'
                   OR (
                       spe.desired_state = 'open'
                       AND (
                           spe.last_bridge_status IS NULL
                           OR spe.last_bridge_status NOT IN
                               ('direct', 'loopback_forwarded')
                           OR spe.last_observed_at IS NULL
                           OR spe.last_observed_at <
                               NOW() - INTERVAL '60 seconds'
                           OR spe.last_applied_node_id IS DISTINCT FROM $2
                       )
                   )
               )
             ORDER BY spe.sandbox_id, spe.guest_port ASC, spe.protocol ASC"
    ))
    .bind(sandbox_ids)
    .bind(node_id)
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// What a node reports it applied for one exposure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyStateReport<'a> {
    pub sandbox_id: SandboxId,
    pub protocol: &'a str,
    pub guest_port: i32,
    pub apply_state: &'a str,
    /// Recorded as the exposure's error when `apply_state` is `failed`; any
    /// other report clears the stored error.
    pub error: Option<&'a str>,
    pub bridge_status: Option<&'a str>,
    pub service_generation: Option<&'a str>,
}

/// A reported exposure that [`record_apply_states`] did not update.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct MissedApplyState {
    pub sandbox_id: SandboxId,
    pub protocol: String,
    pub guest_port: i32,
}

/// Record what `node_id` reports it applied (an observed projection), for
/// each report whose exposure exists on a sandbox active on `node_id`, and
/// stamp each such exposure observed now on the database clock. An
/// `applied` report makes `node_id` the exposure's last-applied node. A
/// `failed` report records its error as the exposure's error, and any other
/// report clears the stored error. A missing bridge status or generation
/// keeps the stored one. Returns the reports that matched no such exposure,
/// the ones naming a sandbox placed elsewhere included; none is an error.
pub async fn record_apply_states(
    executor: impl Executor<'_>,
    node_id: NodeId,
    reports: &[ApplyStateReport<'_>],
) -> Result<Vec<MissedApplyState>, DbError> {
    let sandbox_ids = reports
        .iter()
        .map(|report| report.sandbox_id.as_uuid())
        .collect::<Vec<_>>();
    let protocols = reports
        .iter()
        .map(|report| report.protocol)
        .collect::<Vec<_>>();
    let guest_ports = reports
        .iter()
        .map(|report| report.guest_port)
        .collect::<Vec<_>>();
    let apply_states = reports
        .iter()
        .map(|report| report.apply_state)
        .collect::<Vec<_>>();
    let errors = reports
        .iter()
        .map(|report| report.error)
        .collect::<Vec<_>>();
    let bridge_statuses = reports
        .iter()
        .map(|report| report.bridge_status)
        .collect::<Vec<_>>();
    let service_generations = reports
        .iter()
        .map(|report| report.service_generation)
        .collect::<Vec<_>>();
    Ok(sqlx::query_as::<_, MissedApplyState>(sql!(
        "WITH input AS (
                 SELECT *
                 FROM UNNEST(
                     $1::UUID[],
                     $2::TEXT[],
                     $3::INT[],
                     $4::TEXT[],
                     $5::TEXT[],
                     $7::TEXT[],
                     $8::TEXT[]
                 ) AS input(sandbox_id, protocol, guest_port, apply_state, error,
                            bridge_status, service_generation)
             ),
             updated AS (
                 UPDATE sandbox_port_exposures spe
                 SET apply_state = input.apply_state,
                     last_applied_node_id =
                         CASE WHEN input.apply_state = 'applied'
                              THEN $6
                              ELSE spe.last_applied_node_id
                         END,
                     last_error =
                         CASE WHEN input.apply_state = 'failed'
                              THEN input.error
                              ELSE NULL
                         END,
                     last_bridge_status =
                         COALESCE(input.bridge_status, spe.last_bridge_status),
                     last_applied_bridge_generation =
                         COALESCE(input.service_generation, spe.last_applied_bridge_generation),
                     last_observed_at = NOW(),
                     updated_at = NOW()
                 FROM input
                 JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE spe.sandbox_id = input.sandbox_id
                   AND spe.protocol = input.protocol
                   AND spe.guest_port = input.guest_port
                   AND spe.sandbox_id = s.id
                   AND s.active_node_id = $6
                 RETURNING spe.sandbox_id, spe.protocol, spe.guest_port
             )
             SELECT input.sandbox_id, input.protocol, input.guest_port
             FROM input
             LEFT JOIN updated
               ON updated.sandbox_id = input.sandbox_id
              AND updated.protocol = input.protocol
              AND updated.guest_port = input.guest_port
             WHERE updated.sandbox_id IS NULL"
    ))
    .bind(sandbox_ids)
    .bind(protocols)
    .bind(guest_ports)
    .bind(apply_states)
    .bind(errors)
    .bind(node_id)
    .bind(bridge_statuses)
    .bind(service_generations)
    .fetch_all(executor.into_sqlx())
    .await?)
}

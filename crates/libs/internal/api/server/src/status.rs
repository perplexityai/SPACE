use pplx_space_common_core::types::{NodeId, SubstrateKind};

use super::*;
use pplx_space_common_core::sql;
use std::sync::LazyLock;

/// Built once: every input is bound, so the text is the same on every publish.
/// Mirrors `sandbox_quarantine_store`'s composed queries in this crate.
///
/// The interpolated term comes from `pplx_space_sandbox_portability` rather
/// than being spelled here because that crate is the single definition of "are
/// this sandbox's bytes recoverable elsewhere", and a deletion path is exactly
/// where a second spelling does damage.
static PRUNE_ABSENT_NODE_SANDBOXES_QUERY: LazyLock<String> = LazyLock::new(|| {
    let released_with_durable_artifact =
        pplx_space_sandbox_portability::stopped_artifact_durable("s");
    sql!(
        format!(
            "WITH removed AS (
             DELETE FROM sandbox_observed_states sos
             WHERE sos.node_id = $1
               AND NOT (sos.sandbox_id = ANY($2))
             RETURNING sos.sandbox_id
         ),
         removed_elements AS (
             SELECT 'sandbox:' || sandbox_id::text AS element
             FROM removed
         ),
         deleted_status AS (
             DELETE FROM node_facet_observed_status nfos
             USING removed_elements
             WHERE nfos.node_id = $1
               AND nfos.facet = $3
               AND nfos.element = removed_elements.element
         ),
         deleted_conditions AS (
             DELETE FROM node_facet_conditions nfc
             USING removed_elements
             WHERE nfc.node_id = $1
               AND nfc.facet = $3
               AND nfc.element = removed_elements.element
         ),
         marked_deleted AS (
             UPDATE sandboxes s
             SET lifecycle = 'deleted',
                 placement = 'unplaced',
                 active_node_id = NULL,
                 target_node_id = NULL,
                 placement_changed_at = NOW(),
                 lifecycle_changed_at = NOW(),
                 updated_at = NOW(),
                 command_ready_pending = FALSE,
                 restore_lease_expires_at = NULL,
                 restore_lease_token = NULL
             WHERE s.active_node_id = $1
               AND s.managed_by = $4
               AND s.lifecycle NOT IN ('deleted', 'deleting')
               AND NOT {released_with_durable_artifact}
               AND NOT (s.id = ANY($2))
             RETURNING s.id AS sandbox_id
         )
         DELETE FROM sandbox_snapshot_inventory
         WHERE sandbox_id IN (SELECT sandbox_id FROM removed)
            OR sandbox_id IN (SELECT sandbox_id FROM marked_deleted)"
        )
        .as_str()
    )
});

/// Reconcile a node's complete sandbox census against the cluster's own state.
///
/// Two readings of the same absence, with very different weight:
///
/// * **Forget the observation.** `sandbox_observed_states` and the
///   network-policy facet rows for sandboxes the node no longer reports are
///   deleted. This is what lets a drain discount a phantom `active_node_id` —
///   see `pplx_space_common_svc::sql::sandbox_residency`.
/// * **Tombstone the sandbox** (`marked_deleted`). Absence from a *complete*
///   census is read as proof the sandbox is unrecoverable, so the central row
///   is moved to `lifecycle = 'deleted'`.
///
/// The second reading is only sound while the node's local row is the
/// sandbox's sole existence, and two of the terms that keep it that way read
/// like general safety properties without being ones.
///
/// `managed_by = $4`, bound to `SubstrateKind::Space`, is a *substrate* filter.
/// `DirectSubstrate::managed_by()` returns `"space"`
/// (`api/src/controllers/substrate/direct.rs`) while
/// `KubernetesSubstrate::managed_by()` returns `"k8s"`
/// (`.../substrate/kubernetes/adapter/substrate_adapter.rs`), and `sandboxes.managed_by` is
/// bound from that adapter at create time
/// (`api/src/controllers/sandbox/queries.rs`) — so on a Kubernetes deployment
/// this arm matches nothing, and the cluster controller's
/// `orphaned_sandboxes` reconciler classifies a vanished runtime from pod
/// evidence instead. Every row this arm can actually reach is a Direct one.
///
/// A *settled* `suspended` row is outside the arm because its `active_node_id`
/// is NULL — a placement fact, not a judgement about its durable snapshot.
/// Do not read that as an invariant covering suspend generally: on the Direct
/// substrate the nulling is a **separate** write from the one recording the
/// artifact pin (`persist_optional_current_local_artifact` touches only the
/// three `current_local_artifact_*` columns, and the lifecycle flip is applied
/// by the caller after `suspend_execution` returns), so a suspend in flight is
/// briefly pinned and still `active_node_id = $1`.
///
/// So neither term covers a row whose node-local presence was released *on
/// purpose* after its bytes were made durable, which is why
/// `stopped_artifact_durable` is a term of its own rather than an inference
/// from the two above. Its doc carries the evidence it requires, and the
/// contract its future producer owes it.
pub(super) async fn prune_absent_node_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    present_sandbox_ids: &[SandboxId],
) -> Result<(), Status> {
    sqlx::query(sqlx::AssertSqlSafe(
        PRUNE_ABSENT_NODE_SANDBOXES_QUERY.as_str(),
    ))
    .bind(node_id)
    .bind(present_sandbox_ids)
    .bind(FACET_NETWORK_POLICY)
    .bind(SubstrateKind::Space)
    .execute(&mut *conn)
    .await
    .map_err(|err| {
        Status::internal(format!(
            "prune absent published sandbox observations: {err}"
        ))
    })?;
    Ok(())
}

/// Whether this publication's sandbox inventory was accepted as a complete
/// census and pruned against — the server's verdict, not the node's
/// `sandbox_inventory_complete` claim.
///
/// A type rather than a `bool` because both the claim and the verdict are
/// booleans in scope at the one call site, and they diverge exactly where it
/// matters: an inventory flagged complete that names a sandbox id the server
/// cannot parse skips the prune, so it must not stamp
/// `sandbox_inventory_pruned_at` either. A timestamp written from the wire flag
/// would certify a census the server rejected, and the drain gates read that
/// timestamp as permission to discount sandbox rows.
///
/// Construct it from the branch that performs the prune, never from the
/// request, so the column and `sandbox_observed_states` cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SandboxInventoryPrune {
    /// The inventory was accepted and pruned against: stamp the column.
    Applied,
    /// No prune ran: preserve whatever a previous publication recorded.
    Skipped,
}

impl SandboxInventoryPrune {
    /// Whether `sandbox_inventory_pruned_at` advances to `NOW()`.
    pub(super) const fn stamps_pruned_at(self) -> bool {
        matches!(self, Self::Applied)
    }
}

/// Whether this publication mirrored the node's pending local-backup set into
/// `node_local_backup_pending`.
///
/// The sibling of [`SandboxInventoryPrune`], and a type for the same reason:
/// both are booleans in scope at the one call site, they disagree often (a
/// node image predating the report field prunes its census every tick and
/// never mirrors), and passing the wrong one would stamp a gate certifying a
/// mirror that never ran. `crate::node_local_backup_pending` documents what
/// the gate does and does not permit.
///
/// Construct it from the branch that performed the set-replace, never from the
/// request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalBackupInventoryReport {
    /// The queue was set-replaced against this census: stamp the gate.
    Mirrored,
    /// This publication could not have mirrored — a facet tick that carries
    /// no sandbox inventory — so preserve whatever a previous publication
    /// recorded. The gate keeps ageing from the last census that did mirror
    /// rather than being blanked by an unrelated facet's tick.
    Absent,
    /// This publication **could** have mirrored and did not: the server
    /// pruned the node's sandbox inventory, so it was a real Full census,
    /// but the pending-backup report was missing, unusable, or incomplete.
    ///
    /// Clear the gate rather than preserve it. The gate is permission to
    /// believe an *empty* queue, and the queue is now known to be
    /// un-refreshed: an artifact that became pending since the last
    /// successful mirror is absent from it, and a reader applying a
    /// freshness window would still see a recent timestamp and trust the
    /// emptiness. Preserving would leave the node looking certified for the
    /// remainder of that window on evidence the server knows is stale.
    ///
    /// The cost is deliberate and on the safe side: one declined census
    /// drops the reader to its history-predicate fallback until a census
    /// mirrors again, which is slower but correct, rather than fast and
    /// wrong.
    Invalidated,
}

impl LocalBackupInventoryReport {
    /// Whether `local_backup_inventory_reported_at` advances to `NOW()`.
    pub(super) const fn stamps_reported_at(self) -> bool {
        matches!(self, Self::Mirrored)
    }

    /// Whether the gate is cleared outright rather than preserved.
    pub(super) const fn clears_reported_at(self) -> bool {
        matches!(self, Self::Invalidated)
    }
}

/// Refresh a node's `node_status_snapshots` row from an accepted publication.
///
/// `reachable` and `stale_after` advance on every call, so they record that the
/// node answered and bind nothing about how current its sandbox census is.
/// `sandbox_inventory_pruned_at` is the census-freshness signal, and it moves
/// only when `inventory_prune` says a prune ran — see
/// [`SandboxInventoryPrune`], and
/// `pplx_space_common_svc::sql::sandbox_residency` for the gates that read it.
/// `local_backup_inventory_reported_at` is the same shape one column over, for
/// the pending-backup mirror — see [`LocalBackupInventoryReport`].
/// Execution collection has its own receipt time: only a supplied outcome
/// advances it. Heartbeats and other facets preserve the previous evidence.
///
/// `reported_window` is how long past now the node counts as fresh, from
/// [`reported_freshness`](pplx_space_node_api_client::node_status_freshness::reported_freshness).
/// A publication that reports no usable interval keeps the row's stored window
/// (`stale_after - observed_at`), and a node's first such publication gets
/// [`DEFAULT_NODE_STATUS_FRESHNESS`](pplx_space_node_api_client::node_status_freshness::DEFAULT_NODE_STATUS_FRESHNESS).
/// The stored window is usually the last reported interval's, but the Direct
/// substrate's create-failure mark writes a 10 s one, and that's the one kept
/// until a publication reports an interval again: the node reads stale
/// sooner, never later.
pub(super) async fn upsert_node_status_from_publish(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    reported_window: Option<std::time::Duration>,
    node_status: Option<PublishedNodeStatusDb>,
    inventory_prune: SandboxInventoryPrune,
    local_backup_report: LocalBackupInventoryReport,
    execution_observation: Option<crate::execution_observation::PublishedExecutionObservation>,
) -> Result<(), Status> {
    let (
        scheduling_status,
        scheduling_reason,
        network_slots_ready,
        network_pool_min_ready,
        network_slots_degraded,
        network_slots_quarantined,
        network_slots_total,
        network_slots_leased,
        network_pool_max_slots,
        node_ctl,
    ) = match node_status {
        Some(status) => (
            Some(status.scheduling_status),
            Some(status.scheduling_reason),
            Some(status.network_slots_ready),
            Some(status.network_pool_min_ready),
            Some(status.network_slots_degraded),
            Some(status.network_slots_quarantined),
            Some(status.network_slots_total),
            Some(status.network_slots_leased),
            Some(status.network_pool_max_slots),
            status.node_ctl,
        ),
        None => (None, None, None, None, None, None, None, None, None, None),
    };
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id,
             observed_at,
             stale_after,
             reachable,
             error,
             role,
             node_id_reported,
             scheduling_status,
             scheduling_reason,
             network_slots_ready,
             network_pool_min_ready,
             network_slots_degraded,
             network_slots_quarantined,
             network_slots_total,
             network_slots_leased,
             network_pool_max_slots,
             health_healthy,
             services,
             node_ctl,
             sandbox_inventory_pruned_at,
             local_backup_inventory_reported_at,
             execution_policy,
             execution_policy_received_at,
             execution_inventory_status,
             execution_inventory_received_at,
             updated_at
         )
         SELECT n.id,
                NOW(),
                NOW() + make_interval(
                    secs => COALESCE($2::DOUBLE PRECISION, $18::DOUBLE PRECISION)
                ),
                TRUE,
                NULL,
                n.role,
                NULL,
                $3::TEXT,
                $4::TEXT,
                $5::INT,
                $6::INT,
                $7::INT,
                $8::INT,
                $9::INT,
                $10::INT,
                $11::INT,
                CASE WHEN $12::JSONB IS NULL THEN NULL ELSE TRUE END,
                '[]'::JSONB,
                $12::JSONB,
                CASE WHEN $13::BOOLEAN THEN NOW() ELSE NULL END,
                CASE WHEN $14::BOOLEAN THEN NOW() ELSE NULL END,
                $17::JSONB,
                CASE WHEN $17::JSONB IS NOT NULL THEN clock_timestamp() ELSE NULL END,
                $16::TEXT,
                CASE WHEN $16::TEXT IS NOT NULL THEN clock_timestamp() ELSE NULL END,
                NOW()
         FROM nodes n
         WHERE n.id = $1
         ON CONFLICT (node_id) DO UPDATE
         SET observed_at = EXCLUDED.observed_at,
             -- No usable interval: keep the row's stored window.
             stale_after = CASE
                 WHEN $2::DOUBLE PRECISION IS NULL
                 THEN EXCLUDED.observed_at
                     + (node_status_snapshots.stale_after - node_status_snapshots.observed_at)
                 ELSE EXCLUDED.stale_after
             END,
             reachable = TRUE,
             error = NULL,
             role = EXCLUDED.role,
             scheduling_status = COALESCE(
                 EXCLUDED.scheduling_status,
                 node_status_snapshots.scheduling_status
             ),
             scheduling_reason = COALESCE(
                 EXCLUDED.scheduling_reason,
                 node_status_snapshots.scheduling_reason
             ),
             network_slots_ready = COALESCE(
                 EXCLUDED.network_slots_ready,
                 node_status_snapshots.network_slots_ready
             ),
             network_pool_min_ready = COALESCE(
                 EXCLUDED.network_pool_min_ready,
                 node_status_snapshots.network_pool_min_ready
             ),
             network_slots_degraded = COALESCE(
                 EXCLUDED.network_slots_degraded,
                 node_status_snapshots.network_slots_degraded
             ),
             network_slots_quarantined = COALESCE(
                 EXCLUDED.network_slots_quarantined,
                 node_status_snapshots.network_slots_quarantined
             ),
             network_slots_total = COALESCE(
                 EXCLUDED.network_slots_total,
                 node_status_snapshots.network_slots_total
             ),
             network_slots_leased = COALESCE(
                 EXCLUDED.network_slots_leased,
                 node_status_snapshots.network_slots_leased
             ),
             network_pool_max_slots = COALESCE(
                 EXCLUDED.network_pool_max_slots,
                 node_status_snapshots.network_pool_max_slots
             ),
             health_healthy = COALESCE(
                 EXCLUDED.health_healthy,
                 node_status_snapshots.health_healthy
             ),
             services = CASE
                 WHEN EXCLUDED.health_healthy IS NULL
                 THEN node_status_snapshots.services
                 ELSE EXCLUDED.services
             END,
             node_ctl = COALESCE(EXCLUDED.node_ctl, node_status_snapshots.node_ctl),
             -- Preserve, never overwrite: this runs on every publication, and
             -- the facets that do not prune send NULL here. A plain
             -- `= EXCLUDED.x` would blank the census timestamp on the template
             -- facet's tick, moments after the census facet set it.
             sandbox_inventory_pruned_at = COALESCE(
                 EXCLUDED.sandbox_inventory_pruned_at,
                 node_status_snapshots.sandbox_inventory_pruned_at
             ),
             -- Three-way, unlike the column above. A facet tick carries no
             -- pending-backup report and must leave the gate alone, same as
             -- the prune stamp. But a *census* that pruned the inventory and
             -- still could not mirror has told us the queue is stale, and
             -- preserving a recent timestamp there would leave a reader's
             -- freshness window certifying an empty queue the server knows
             -- it did not refresh. `$15` distinguishes the two.
             local_backup_inventory_reported_at = CASE
                 WHEN $15::BOOLEAN THEN NULL
                 ELSE COALESCE(
                     EXCLUDED.local_backup_inventory_reported_at,
                     node_status_snapshots.local_backup_inventory_reported_at
                 )
             END,
             execution_policy = CASE
                 WHEN $16::TEXT IS NOT NULL THEN EXCLUDED.execution_policy
                 ELSE node_status_snapshots.execution_policy
             END,
             execution_policy_received_at = CASE
                 WHEN $16::TEXT IS NOT NULL THEN EXCLUDED.execution_policy_received_at
                 ELSE node_status_snapshots.execution_policy_received_at
             END,
             execution_inventory_status = COALESCE(
                 EXCLUDED.execution_inventory_status,
                 node_status_snapshots.execution_inventory_status
             ),
             execution_inventory_received_at = CASE
                 WHEN $16::TEXT IS NOT NULL THEN clock_timestamp()
                 ELSE node_status_snapshots.execution_inventory_received_at
             END,
             updated_at = NOW()"
    ))
    .bind(node_id)
    .bind(reported_window.map(|window| window.as_secs_f64()))
    .bind(scheduling_status)
    .bind(scheduling_reason)
    .bind(network_slots_ready)
    .bind(network_pool_min_ready)
    .bind(network_slots_degraded)
    .bind(network_slots_quarantined)
    .bind(network_slots_total)
    .bind(network_slots_leased)
    .bind(network_pool_max_slots)
    .bind(node_ctl.map(Json))
    .bind(inventory_prune.stamps_pruned_at())
    .bind(local_backup_report.stamps_reported_at())
    .bind(local_backup_report.clears_reported_at())
    .bind(
        execution_observation
            .as_ref()
            .map(|observation| observation.status.as_str()),
    )
    .bind(
        execution_observation
            .and_then(|observation| observation.policy)
            .map(Json),
    )
    .bind(
        pplx_space_node_api_client::node_status_freshness::DEFAULT_NODE_STATUS_FRESHNESS
            .as_secs_f64(),
    )
    .execute(&mut *conn)
    .await
    .map_err(|err| Status::internal(format!("upsert node status from publish: {err}")))?;
    Ok(())
}

pub(super) fn db_role_from_proto(role: i32) -> Result<SpaceNodeRole, Status> {
    if role == ProtoNodeRole::Worker as i32 {
        Ok(SpaceNodeRole::Worker)
    } else if role == ProtoNodeRole::Builder as i32 {
        Ok(SpaceNodeRole::Builder)
    } else if role == ProtoNodeRole::Cluster as i32 {
        Ok(SpaceNodeRole::Cluster)
    } else {
        Err(Status::invalid_argument(format!(
            "unknown node role {role}"
        )))
    }
}

pub(super) fn optional_db_role_from_proto(
    role: Option<i32>,
) -> Result<Option<SpaceNodeRole>, Status> {
    role.map(db_role_from_proto).transpose()
}

pub(super) fn db_role_from_proto_or_default(
    role: Option<i32>,
    default: SpaceNodeRole,
) -> Result<SpaceNodeRole, Status> {
    optional_db_role_from_proto(role).map(|role| role.unwrap_or(default))
}

pub(super) fn proto_role_from_db(role: &str) -> Result<i32, Status> {
    let role = SpaceNodeRole::parse(role).map_err(|error| {
        Status::internal(format!("unknown node role in database {role}: {error}"))
    })?;
    Ok(proto_role_from_node_role(role))
}

pub(super) fn proto_role_from_node_role(role: SpaceNodeRole) -> i32 {
    match role {
        SpaceNodeRole::Worker => ProtoNodeRole::Worker as i32,
        SpaceNodeRole::Builder => ProtoNodeRole::Builder as i32,
        SpaceNodeRole::Cluster => ProtoNodeRole::Cluster as i32,
    }
}

pub(super) fn format_time(value: OffsetDateTime) -> String {
    value
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

pub(super) fn format_optional_time(value: Option<OffsetDateTime>) -> Option<String> {
    value.map(format_time)
}

pub(super) fn node_record(row: NodeRow) -> Result<NodeRecord, Status> {
    Ok(NodeRecord {
        id: row.id,
        name: row.name,
        address: row.address,
        node_api_endpoint: row.node_api_endpoint,
        role: proto_role_from_db(&row.role)?,
        managed_by: row.managed_by,
        state: row.state,
        last_heartbeat: format_optional_time(row.last_heartbeat),
        created_at: format_time(row.created_at),
        updated_at: format_time(row.updated_at),
        deleted_at: format_optional_time(row.deleted_at),
        metadata_json: row.metadata_json,
    })
}

pub(super) fn reconcile_retry_delay_secs(attempts_before_failure: i32) -> i64 {
    let attempts = attempts_before_failure.saturating_add(1).clamp(1, 16);
    let delay = 1_i64
        .checked_shl((attempts - 1).cast_unsigned())
        .unwrap_or(NODE_RECONCILE_MAX_RETRY_DELAY_SECS);
    delay.min(NODE_RECONCILE_MAX_RETRY_DELAY_SECS)
}

#[cfg(test)]
#[path = "status_tests.rs"]
mod status_tests;

#[cfg(test)]
#[path = "status_freshness_tests.rs"]
mod status_freshness_tests;

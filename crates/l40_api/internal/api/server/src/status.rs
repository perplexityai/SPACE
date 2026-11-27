use pplx_space_accounts::NodeId;

use super::*;

/// Reconcile a node's complete sandbox census against the cluster's own state,
/// through [`pplx_space_sandbox_store::ingest::prune_census_absent_sandboxes`].
///
/// Two readings of the same absence, with very different weight:
///
/// * **Forget the observation.** `sandbox_observed_states` and the
///   network-policy facet rows for sandboxes the node no longer reports are
///   deleted. This is what lets a drain discount a phantom `active_node_id` —
///   see `pplx_space_sandbox_residency`.
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
    locked_candidate_ids: &[SandboxId],
) -> Result<(), Status> {
    pplx_space_sandbox_store::ingest::prune_census_absent_sandboxes(
        &mut *conn,
        node_id,
        locked_candidate_ids,
        FACET_NETWORK_POLICY,
    )
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
    /// What happens to `local_backup_inventory_reported_at`.
    pub(super) const fn reported_at(self) -> pplx_space_node_status_store::LocalBackupReportedAt {
        match self {
            Self::Mirrored => pplx_space_node_status_store::LocalBackupReportedAt::Stamp,
            Self::Absent => pplx_space_node_status_store::LocalBackupReportedAt::Preserve,
            Self::Invalidated => pplx_space_node_status_store::LocalBackupReportedAt::Withdraw,
        }
    }
}

/// Refresh a node's `node_status_snapshots` row from an accepted publication.
///
/// `reachable` and `stale_after` advance on every call, so they record that the
/// node answered and bind nothing about how current its sandbox census is.
/// `sandbox_inventory_pruned_at` is the census-freshness signal, and it moves
/// only when `inventory_prune` says a prune ran — see
/// [`SandboxInventoryPrune`], and
/// `pplx_space_sandbox_residency` for the gates that read it.
/// `local_backup_inventory_reported_at` is the same shape one column over, for
/// the pending-backup mirror — see [`LocalBackupInventoryReport`].
/// Execution collection has its own receipt time: only a supplied outcome
/// advances it. Heartbeats and other facets preserve the previous evidence.
/// The environment arrays have a third, written only as
/// [`EnvironmentRefs`](crate::environment_observation::EnvironmentRefs) says.
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
    let environments = execution_observation.as_ref().map_or(
        &crate::environment_observation::EnvironmentRefs::Preserve,
        |observation| &observation.environments,
    );
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
    pplx_space_node_status_store::upsert_from_publish(
        &mut *conn,
        &pplx_space_node_status_store::NodeStatusPublication {
            node_id,
            reported_window_secs: reported_window.map(|window| window.as_secs_f64()),
            scheduling_status: scheduling_status.as_deref(),
            scheduling_reason: scheduling_reason.as_deref(),
            network_slots_ready,
            network_pool_min_ready,
            network_slots_degraded,
            network_slots_quarantined,
            network_slots_total,
            network_slots_leased,
            network_pool_max_slots,
            node_ctl: node_ctl.as_ref(),
            stamps_sandbox_inventory_pruned_at: inventory_prune.stamps_pruned_at(),
            local_backup_inventory_reported_at: local_backup_report.reported_at(),
            execution_inventory_status: execution_observation
                .as_ref()
                .map(|observation| observation.status.as_str()),
            execution_policy: execution_observation
                .as_ref()
                .and_then(|observation| observation.policy.as_ref()),
            default_window_secs:
                pplx_space_node_api_client::node_status_freshness::DEFAULT_NODE_STATUS_FRESHNESS
                    .as_secs_f64(),
            environment_refs: environments.write(),
        },
    )
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

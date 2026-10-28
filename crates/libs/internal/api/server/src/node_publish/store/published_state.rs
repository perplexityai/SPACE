use pplx_space_common_core::types::{NodeId, SnapshotId};

use pplx_space_common_core::sql;
use std::collections::BTreeSet;

use sqlx::types::Json;
use tonic::Status;
use uuid::Uuid;

use pplx_space_cluster_trigger_queue::enqueue_triggers_conn;
#[cfg(test)]
use pplx_space_common_core::types::NodeRole;
use pplx_space_common_core::types::{
    ArtifactKind, SandboxId, SandboxLifecycle, SandboxQuarantineReasonCode,
    SandboxTransitionTargetState,
};
use pplx_space_common_svc::types::{
    FACET_NETWORK_POLICY, FACET_SANDBOX_DESIRED_STATE, FACET_TEMPLATE_BASE_SNAPSHOT_POLICY,
    FACET_TEMPLATE_DESIRED_STATE, NetworkPolicyTarget, ReconcileReason,
};
use pplx_space_events::{
    SandboxEvent, SandboxEventDetails, SandboxEventSource, SandboxEventTrigger, SandboxEventType,
};
use pplx_space_sandbox_portability::{
    started_suspend_revert_eligibility, suspended_projection_lifecycle_eligible,
};

use crate::node_publish::store::{
    absent_sandboxes, activity, degraded_observations, failure_episodes, lifecycle_transitions,
    network_policy, observed_state,
    quarantine_claims::{self, ClaimedQuarantineProjection},
    quarantine_persistence, sandbox_facet_observations, snapshot_inventory,
    suspend_finalization::{self, SuspendedFinalizationCandidate},
    template_components, template_inventory, template_network_policy, template_pruning,
};
pub(crate) use crate::node_publish::validate::fields::snapshot_execution_metadata;
#[cfg(test)]
use crate::node_publish::validate::quarantine::{
    QuarantineRecoveryProjectionState, quarantine_recovery_projection,
};
use crate::node_publish::validate::quarantine_claim::QuarantineClaim;
use crate::node_publish::validate::sandbox_inventory::{
    DegradedSandboxClaim, LegacySandboxInput, ValidatedSandboxInventory,
};
use crate::node_publish::validate::templates::ValidatedTemplateInventory;
use crate::proto::PublishedSandboxState;
#[cfg(test)]
use crate::proto::PublishedTemplateState;
#[cfg(test)]
use crate::sandbox_quarantine_store;

const NODE_RECONCILE_QUEUE_FIRST_SEEN_ON_CONFLICT: &str = "
ON CONFLICT (node_id, facet, element) DO UPDATE
SET generation = node_reconcile_queue.generation + 1,
    requested_at = EXCLUDED.requested_at,
    next_attempt_at = NOW(),
    attempts = 0,
    last_error = '',
    dead_at = NULL,
    dead_reason = '',
    updated_at = NOW()";

#[derive(Clone, Debug, serde::Serialize)]
struct QuarantineProjectionSqlInput<'a> {
    sandbox_id: SandboxId,
    failure_reason: Option<&'a str>,
    source: &'static str,
    reason_code: &'static str,
    quarantine_id: Uuid,
    history_id: Uuid,
}

impl<'a> QuarantineProjectionSqlInput<'a> {
    fn materialize(claim: &'a QuarantineClaim) -> Self {
        Self {
            sandbox_id: claim.sandbox_id(),
            failure_reason: claim.failure_reason(),
            source: claim.source().as_str(),
            reason_code: claim.reason_code().as_str(),
            quarantine_id: Uuid::now_v7(),
            history_id: Uuid::now_v7(),
        }
    }
}

/// Longest prefix of an invalid capture host CPU that reaches the log. The
/// value is node-supplied and the begin request isn't authorized yet when it's
/// parsed, so the log carries a bounded prefix, not the whole string.
const MAX_LOGGED_CAPTURE_HOST_CPU_BYTES: usize = 64;

/// Parses the capture host CPU of an artifact backup. The CPU is placement
/// evidence, not part of the snapshot's identity, so a value that isn't a valid
/// label is recorded as unknown rather than failing the backup it rides on.
/// An empty value means unknown too, and isn't worth a warning. A disk artifact
/// holds no vCPU state, so it has no capture CPU whatever the sender says.
pub(crate) fn capture_host_cpu(
    raw: Option<&str>,
    artifact_kind: ArtifactKind,
    sandbox_id: SandboxId,
) -> Option<pplx_space_common_core::types::HostCpu> {
    if artifact_kind == ArtifactKind::Disk {
        return None;
    }
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    pplx_space_common_core::types::HostCpu::parse(raw)
        .inspect_err(|error| {
            let logged = pplx_space_common_svc::truncate_utf8_for_log(
                raw,
                MAX_LOGGED_CAPTURE_HOST_CPU_BYTES,
            );
            tracing::warn!(
                %sandbox_id,
                capture_host_cpu = ?logged.value,
                truncated = logged.truncated,
                %error,
                "ignoring invalid capture host CPU on artifact backup"
            );
        })
        .ok()
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn sandbox_policy_reconcile_refs_for_new_sandboxes(
    sandbox_ids: &[SandboxId],
) -> Vec<(&'static str, String)> {
    let mut refs = Vec::new();
    for sandbox_id in sandbox_ids.iter().collect::<BTreeSet<_>>().into_iter() {
        let sandbox_id = *sandbox_id;
        refs.push((FACET_SANDBOX_DESIRED_STATE, sandbox_id.to_string()));
        refs.push((
            FACET_NETWORK_POLICY,
            NetworkPolicyTarget::sandbox(sandbox_id).as_element_string(),
        ));
    }
    refs
}

fn template_policy_reconcile_refs_for_new_templates(
    templates: &[String],
) -> Vec<(&'static str, String)> {
    templates
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .flat_map(|template| {
            [
                (FACET_TEMPLATE_DESIRED_STATE, template.to_string()),
                (FACET_TEMPLATE_BASE_SNAPSHOT_POLICY, template.to_string()),
                (FACET_NETWORK_POLICY, format!("template:{template}")),
            ]
        })
        .collect()
}

async fn queue_policy_reconcile_refs_for_new_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    refs: Vec<(&'static str, String)>,
    trigger_reason: ReconcileReason,
) -> Result<(), Status> {
    if refs.is_empty() {
        return Ok(());
    }

    let facets = refs
        .iter()
        .map(|(facet, _)| (*facet).to_string())
        .collect::<Vec<_>>();
    let elements = refs
        .iter()
        .map(|(_, element)| element.clone())
        .collect::<Vec<_>>();
    let query = format!(
        "WITH refs AS (
             SELECT DISTINCT facet, element
             FROM UNNEST($2::TEXT[], $3::TEXT[]) AS refs(facet, element)
         )
         INSERT INTO node_reconcile_queue (node_id, facet, element, requested_at, updated_at)
         SELECT $1, facet, element, NOW(), NOW()
         FROM refs
         {NODE_RECONCILE_QUEUE_FIRST_SEEN_ON_CONFLICT}",
    );
    sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node_id)
        .bind(&facets)
        .bind(&elements)
        .execute(&mut *conn)
        .await
        .map_err(|err| {
            Status::internal(format!("enqueue first-seen inventory reconcile: {err}"))
        })?;

    enqueue_triggers_conn(conn, [node_id], trigger_reason)
        .await
        .map_err(|err| Status::internal(format!("enqueue first-seen inventory trigger: {err}")))?;

    Ok(())
}

#[cfg(test)]
async fn publish_template_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    publisher_role: NodeRole,
    states: &[PublishedTemplateState],
) -> Result<u64, Status> {
    let inventory = ValidatedTemplateInventory::try_from_proto(publisher_role, states)?;
    publish_validated_template_states(conn, node_id, inventory).await
}

/// Apply checked template evidence on the caller's connection. This phase uses
/// ordinary write locks in statement order, with no separate transaction or
/// explicit row-lock query. Zero matching inventory rows is a normal outcome.
pub(crate) async fn publish_validated_template_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    inventory: ValidatedTemplateInventory,
) -> Result<u64, Status> {
    let (absent_templates, rows) = inventory.into_legacy_rows();
    let mut accepted = template_pruning::remove_absent(conn, node_id, &absent_templates).await?;

    if rows.is_empty() {
        return Ok(accepted);
    }

    let rows_json = Json(serde_json::Value::Array(rows));
    let accepted_rows = template_inventory::upsert(conn, node_id, rows_json.clone()).await?;

    template_components::replace(conn, node_id, rows_json.clone()).await?;

    // Node publication deliberately never writes `template_releases`. Release
    // rows are fleet-shared, and any per-node write to them (even a guarded
    // no-op rewrite) takes row locks that are held until the multi-statement
    // publish transaction commits, convoying every other publisher. Release
    // execution metadata is written once, by `RegisterTemplateRelease`, at
    // registration time.

    let first_seen_templates = accepted_rows
        .iter()
        .filter(|(_, _, _, first_seen, _)| *first_seen)
        .map(|(template, _, _, _, _)| template.clone())
        .collect::<Vec<_>>();
    let first_seen_present_templates = accepted_rows
        .iter()
        .filter(|(_, _, _, first_seen, desired_state)| *first_seen && desired_state == "present")
        .map(|(template, _, _, _, _)| template.clone())
        .collect::<Vec<_>>();
    let first_seen_absent_desired_refs = first_seen_templates
        .iter()
        .filter(|template| !first_seen_present_templates.contains(template))
        .map(|template| (FACET_TEMPLATE_DESIRED_STATE, template.clone()))
        .collect::<Vec<_>>();
    let mut first_seen_refs =
        template_policy_reconcile_refs_for_new_templates(&first_seen_present_templates);
    first_seen_refs.extend(first_seen_absent_desired_refs);
    queue_policy_reconcile_refs_for_new_inventory(
        conn,
        node_id,
        first_seen_refs,
        ReconcileReason::NodeTemplateInventoryPublished,
    )
    .await?;

    template_network_policy::observe(conn, node_id, &rows_json).await?;

    accepted = accepted.saturating_add(usize_to_u64(accepted_rows.len()));
    Ok(accepted)
}

/// Record on the sandbox event timeline that a node-published status
/// converged the control-plane lifecycle row. Node-side events
/// (`runtime_activated`, `snapshot_restore_completed`, ...) only say the
/// sandbox is healthy on the node; this is the only emitter that knows the
/// `sandboxes.lifecycle` SSOT actually flipped, and via which path
/// (`reason=node_published_state` vs the caller's normal completion). Like
/// the failed-demotion warn log below, this fires before the surrounding
/// publish transaction commits; a failed commit can orphan an event.
fn emit_lifecycle_convergence_events(
    node_id: NodeId,
    sandbox_ids: &[SandboxId],
    event_type: SandboxEventType,
    from_state: Option<&str>,
    to_state: &str,
) {
    for sandbox_id in sandbox_ids {
        let mut details = SandboxEventDetails::default()
            .reason("node_published_state")
            .operation("node_state_publish")
            .trigger(SandboxEventTrigger::System)
            .with_to_state(to_state)
            .source_node_id(node_id.to_string());
        if let Some(from_state) = from_state {
            details = details.with_from_state(from_state);
        }
        SandboxEvent::new(*sandbox_id, event_type, SandboxEventSource::Api).emit_with(details);
    }
}

fn emit_node_published_state_events(
    node_id: NodeId,
    sandboxes: &[(SandboxId, String)],
    event_type: SandboxEventType,
    reason: &'static str,
    to_state: &'static str,
) {
    for (sandbox_id, from_state) in sandboxes {
        SandboxEvent::new(*sandbox_id, event_type, SandboxEventSource::Api).emit_with(
            SandboxEventDetails::default()
                .reason(reason)
                .operation("node_state_publish")
                .trigger(SandboxEventTrigger::System)
                .with_from_state(from_state.as_str())
                .with_to_state(to_state)
                .source_node_id(node_id.to_string()),
        );
    }
}

#[derive(Debug)]
pub struct SandboxPublicationOutcome {
    pub accepted: u64,
    /// Emit only after the node-publication transaction commits successfully.
    pub post_commit_events: Vec<PostCommitSandboxEvent>,
    pub post_commit_state_convergence: Vec<StateConvergenceSample>,
    /// Sandboxes this publication could take only as lifecycle state, from
    /// either shape: the node set `state_degraded`, or an optional field
    /// failed to parse and the richer observation was dropped to keep the
    /// lifecycle. Both mean the node's account of that sandbox is incomplete.
    ///
    /// Reported because a caller may need to refuse work that depends on the
    /// node having described a sandbox fully — the pending-local-backup mirror
    /// declines a report that names no artifact for a sandbox in this set,
    /// since the entry it would have carried may be exactly what the
    /// degradation dropped. Sandboxes whose identity did not parse are absent
    /// here, having no id to record under; they force the census prune to be
    /// skipped instead.
    pub state_only_sandbox_ids: BTreeSet<SandboxId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StateConvergenceSample {
    pub state: String,
    pub changed_at: time::OffsetDateTime,
}

#[derive(Debug)]
pub struct PostCommitSandboxEvent {
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    prior_lifecycle: SandboxLifecycle,
    target_lifecycle: SandboxTransitionTargetState,
    reason_code: SandboxQuarantineReasonCode,
    error: Option<String>,
}

impl PostCommitSandboxEvent {
    pub fn emit(self, node_id: NodeId) {
        let mut details = SandboxEventDetails::default()
            .reason(self.reason_code.as_str())
            .operation("node_state_publish")
            .trigger(SandboxEventTrigger::System)
            .with_from_state(self.prior_lifecycle.as_str())
            .with_to_state(self.target_lifecycle.as_str())
            .source_node_id(node_id.to_string());
        if let Some(error) = self.error {
            details = details.error(error);
        }
        SandboxEvent::new(self.sandbox_id, self.event_type, SandboxEventSource::Api)
            .emit_with(details);
    }
}

/// Publishes a node's sandbox observations.
///
/// Callers that need atomic lifecycle convergence should run this inside a
/// transaction. Node-published suspended projection uses multiple statements
/// over one locked candidate set and relies on transaction rollback on error.
pub async fn publish_sandbox_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    states: &[PublishedSandboxState],
) -> Result<SandboxPublicationOutcome, Status> {
    let inventory = ValidatedSandboxInventory::from_proto(states);
    publish_validated_sandbox_states(conn, node_id, inventory)
        .await
        .map_err(Status::from)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplySandboxClaimsError {
    #[error(transparent)]
    Absent(#[from] absent_sandboxes::RemoveAbsentSandboxesError),
    #[error(transparent)]
    Degraded(#[from] degraded_observations::UpsertDegradedObservationsError),
    #[error(transparent)]
    Activity(#[from] activity::UpdateActivityError),
    #[error(transparent)]
    ObservedState(#[from] observed_state::UpsertObservedStateError),
    #[error(transparent)]
    Restored(#[from] lifecycle_transitions::ConvergeRestoredError),
    #[error(transparent)]
    Stopped(#[from] lifecycle_transitions::ConvergeStoppedError),
    #[error(transparent)]
    Failed(#[from] lifecycle_transitions::ConvergeFailedError),
    #[error(transparent)]
    NetworkPolicyAuthority(#[from] network_policy::UpdateAuthorityError),
    #[error(transparent)]
    NetworkPolicy(#[from] network_policy::UpsertPolicyError),
    #[error(transparent)]
    NetworkPolicyCondition(#[from] network_policy::ClearApplyFailureError),
    #[error(transparent)]
    SuspendFinalization(#[from] suspend_finalization::FinalizeSuspensionError),
    #[error(transparent)]
    QuarantineState(#[from] quarantine_persistence::RecordStateError),
    #[error(transparent)]
    QuarantineHistory(#[from] quarantine_persistence::RecordHistoryError),
    #[error(transparent)]
    QuarantineSelection(#[from] quarantine_claims::SelectQuarantineClaimsError),
    #[error(transparent)]
    QuarantineProjection(#[from] quarantine_claims::ApplyQuarantineClaimsError),
    #[error(transparent)]
    FailureEpisodes(#[from] failure_episodes::ReconcileFailureEpisodesError),
    #[error(transparent)]
    SnapshotInventory(#[from] snapshot_inventory::ReplaceSnapshotInventoryError),
    #[error(transparent)]
    SuspendHandoff(#[from] sandbox_facet_observations::UpsertSuspendHandoffError),
    #[error(transparent)]
    RecoveryObservation(#[from] sandbox_facet_observations::ReconcileRecoveryObservationError),
    #[error(transparent)]
    ActivationCompletion(#[from] crate::sandbox_quarantine_store::ConsumeActivationReceiptsError),
    #[error(transparent)]
    ActivationRollbackSelection(
        #[from] crate::sandbox_quarantine_store::SelectActivationRollbacksError,
    ),
    #[error(transparent)]
    ActivationRollback(#[from] crate::sandbox_quarantine_store::ApplyActivationRollbacksError),
    #[error(transparent)]
    PauseRecovery(#[from] crate::sandbox_quarantine_store::RecoverPauseFailedError),
    #[error(transparent)]
    SuspendedProjection(#[from] crate::sandbox_quarantine_store::SelectSuspendedProjectionsError),
    #[error(transparent)]
    RecoveryHistory(#[from] crate::sandbox_quarantine_store::RecordQuarantineRecoveryError),
    #[error(transparent)]
    Legacy(#[from] Status),
}

/// Apply parsed node claims on the caller's connection. Lifecycle guards and
/// row locks remain in their existing statement order; an unmatched guard is
/// an ordinary outcome, not authority to retry an unguarded write.
pub(crate) async fn publish_validated_sandbox_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    inventory: ValidatedSandboxInventory,
) -> Result<SandboxPublicationOutcome, ApplySandboxClaimsError> {
    for diagnostic in inventory.diagnostics() {
        diagnostic.emit(node_id);
    }
    let LegacySandboxInput {
        absent_ids,
        rows,
        state_only,
        failure_episode_rows,
        snapshot_rows,
        quarantine_inputs,
        publishes_network_policy,
    } = inventory.into_legacy_input();
    let mut post_commit_events = Vec::new();
    let mut post_commit_state_convergence = Vec::new();

    let mut accepted = absent_sandboxes::remove(&mut *conn, node_id, &absent_ids).await?;

    let degraded_accepted = degraded_observations::upsert(&mut *conn, node_id, &state_only).await?;
    accepted = accepted.saturating_add(degraded_accepted);

    failure_episodes::reconcile(&mut *conn, node_id, &failure_episode_rows).await?;

    if rows.is_empty() {
        return Ok(SandboxPublicationOutcome {
            accepted,
            post_commit_events,
            post_commit_state_convergence,
            state_only_sandbox_ids: state_only
                .iter()
                .map(DegradedSandboxClaim::sandbox_id)
                .collect(),
        });
    }

    let rows_json = Json(serde_json::Value::Array(rows));
    let accepted_rows = observed_state::upsert(conn, node_id, rows_json.clone()).await?;

    let inserted_count = accepted_rows.iter().filter(|row| row.inserted).count();
    let updated_count = accepted_rows.iter().filter(|row| row.updated).count();
    let unchanged_count = accepted_rows
        .len()
        .saturating_sub(inserted_count.saturating_add(updated_count));
    tracing::debug!(
        %node_id,
        accepted = accepted_rows.len(),
        inserted = inserted_count,
        updated = updated_count,
        unchanged = unchanged_count,
        "published sandbox observed state diff applied"
    );

    let accepted_ids = accepted_rows
        .iter()
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    sandbox_facet_observations::upsert_suspend_handoff(conn, node_id, rows_json.clone()).await?;

    post_commit_state_convergence.extend(accepted_rows.iter().filter_map(|row| {
        if !row.status_changed {
            return None;
        }
        Some(StateConvergenceSample {
            state: row.published_status.clone()?,
            changed_at: row.state_changed_at?,
        })
    }));
    sandbox_facet_observations::reconcile_quarantine_recovery(conn, node_id, rows_json.clone())
        .await?;
    activity::update(conn, node_id, rows_json.clone()).await?;

    let first_seen_sandbox_ids = accepted_rows
        .iter()
        .filter(|row| row.assigned_to_node && row.first_seen)
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    let restored_ids =
        lifecycle_transitions::converge_restored(conn, node_id, rows_json.clone()).await?;
    emit_lifecycle_convergence_events(
        node_id,
        &restored_ids,
        SandboxEventType::RestoreCompleted,
        Some("restoring"),
        "started",
    );

    crate::sandbox_quarantine_store::consume_activation_completion_receipts(
        conn,
        node_id,
        rows_json.clone(),
    )
    .await?;

    let rollback_candidates =
        crate::sandbox_quarantine_store::select_activation_rollback_candidates(
            conn,
            node_id,
            rows_json.clone(),
        )
        .await?;
    let applied_rollbacks = crate::sandbox_quarantine_store::apply_activation_rollback_candidates(
        conn,
        node_id,
        rollback_candidates,
    )
    .await?;
    post_commit_events.extend(applied_rollbacks.into_iter().map(|rollback| {
        PostCommitSandboxEvent {
            sandbox_id: rollback.sandbox_id,
            event_type: SandboxEventType::ActivationRollbackCompleted,
            prior_lifecycle: rollback.prior_lifecycle,
            target_lifecycle: rollback.target_state,
            reason_code: rollback.reason_code,
            error: rollback.failure_message,
        }
    }));

    let suspended_projection =
        project_node_published_suspended_sandboxes(conn, node_id, rows_json.clone()).await?;
    let recovered_ids = suspended_projection
        .recoveries
        .iter()
        .map(|(sandbox_id, _, _)| *sandbox_id)
        .collect::<BTreeSet<_>>();
    let suspended_ids = suspended_projection
        .rows
        .iter()
        .map(|(sandbox_id, _)| *sandbox_id)
        .filter(|sandbox_id| !recovered_ids.contains(sandbox_id))
        .collect::<Vec<_>>();
    emit_lifecycle_convergence_events(
        node_id,
        &suspended_ids,
        SandboxEventType::SuspendCompleted,
        None,
        "suspended",
    );
    post_commit_events.extend(suspended_projection.recoveries.iter().map(
        |(sandbox_id, prior_lifecycle, reason_code)| PostCommitSandboxEvent {
            sandbox_id: *sandbox_id,
            event_type: SandboxEventType::QuarantineRecovered,
            prior_lifecycle: *prior_lifecycle,
            target_lifecycle: SandboxTransitionTargetState::Suspended,
            reason_code: *reason_code,
            error: None,
        },
    ));

    let stopped_ids =
        lifecycle_transitions::converge_stopped(conn, node_id, rows_json.clone()).await?;
    emit_lifecycle_convergence_events(
        node_id,
        &stopped_ids,
        SandboxEventType::StopCompleted,
        None,
        "stopped",
    );

    let failed_sandboxes =
        lifecycle_transitions::converge_failed(conn, node_id, rows_json.clone()).await?;
    emit_node_published_state_events(
        node_id,
        &failed_sandboxes,
        SandboxEventType::SandboxFailed,
        "node_published_failed_state",
        "failed",
    );
    if !failed_sandboxes.is_empty() {
        tracing::warn!(
            node_id = %node_id,
            sandboxes_marked_failed = failed_sandboxes.len(),
            "node-published failed sandbox state marked control-plane lifecycle failed"
        );
    }

    let mut quarantined_sandboxes = Vec::new();
    if !quarantine_inputs.is_empty() {
        let quarantine_sql_inputs = quarantine_inputs
            .iter()
            .map(QuarantineProjectionSqlInput::materialize)
            .collect::<Vec<_>>();
        let quarantine_rows_json = Json(
            serde_json::to_value(&quarantine_sql_inputs)
                .map_err(|err| Status::internal(format!("serialize quarantine input: {err}")))?,
        );
        let claimed =
            quarantine_claims::select(conn, node_id, quarantine_rows_json.clone()).await?;
        if !claimed.is_empty() {
            let claimed_sql = claimed
                .iter()
                .map(ClaimedQuarantineProjection::sql_projection)
                .collect::<Vec<_>>();
            let claimed_json = Json(serde_json::to_value(&claimed_sql).map_err(|err| {
                Status::internal(format!("serialize claimed quarantine input: {err}"))
            })?);
            let updated_ids = quarantine_claims::apply(conn, node_id, claimed_json.clone()).await?;
            let updated_ids = updated_ids.into_iter().collect::<BTreeSet<_>>();
            let updated = claimed
                .into_iter()
                .filter(|row| updated_ids.contains(&row.sandbox_id))
                .collect::<Vec<_>>();
            if !updated.is_empty() {
                let updated_sql = updated
                    .iter()
                    .map(ClaimedQuarantineProjection::sql_projection)
                    .collect::<Vec<_>>();
                let updated_json = Json(serde_json::to_value(&updated_sql).map_err(|err| {
                    Status::internal(format!("serialize updated quarantine input: {err}"))
                })?);
                quarantine_persistence::record_state(conn, node_id, updated_json.clone()).await?;
                quarantine_persistence::record_history(conn, node_id, updated_json).await?;
                quarantined_sandboxes = updated
                    .iter()
                    .map(|row| (row.sandbox_id, row.from_lifecycle.clone()))
                    .collect();
            }
        }
    }
    emit_node_published_state_events(
        node_id,
        &quarantined_sandboxes,
        SandboxEventType::SandboxQuarantined,
        "node_published_quarantined_state",
        "quarantined",
    );
    if !quarantined_sandboxes.is_empty() {
        tracing::warn!(
            node_id = %node_id,
            sandboxes_marked_quarantined = quarantined_sandboxes.len(),
            "node-published quarantined sandbox state marked control-plane lifecycle quarantined"
        );
    }

    let pause_failed_recoveries =
        crate::sandbox_quarantine_store::recover_pause_failed_from_inventory(
            conn,
            node_id,
            rows_json.clone(),
        )
        .await?;
    post_commit_events.extend(pause_failed_recoveries.into_iter().map(|recovery| {
        PostCommitSandboxEvent {
            sandbox_id: recovery.sandbox_id,
            event_type: SandboxEventType::QuarantineRecovered,
            prior_lifecycle: SandboxLifecycle::Quarantined,
            target_lifecycle: recovery.target_state,
            reason_code: SandboxQuarantineReasonCode::PauseFailed,
            error: None,
        }
    }));

    queue_policy_reconcile_refs_for_new_inventory(
        conn,
        node_id,
        sandbox_policy_reconcile_refs_for_new_sandboxes(&first_seen_sandbox_ids),
        ReconcileReason::NodeSandboxInventoryPublished,
    )
    .await?;

    if publishes_network_policy {
        network_policy::update_authority(conn, node_id, rows_json.clone()).await?;
        network_policy::upsert(conn, node_id, rows_json.clone()).await?;
        network_policy::clear_apply_failure(conn, node_id, rows_json.clone()).await?;
    }

    snapshot_inventory::replace(conn, &accepted_ids, &snapshot_rows).await?;
    accepted = accepted.saturating_add(usize_to_u64(accepted_ids.len()));
    Ok(SandboxPublicationOutcome {
        accepted,
        post_commit_events,
        post_commit_state_convergence,
        state_only_sandbox_ids: state_only
            .iter()
            .map(DegradedSandboxClaim::sandbox_id)
            .collect(),
    })
}

struct SuspendedProjectionOutcome {
    rows: Vec<(SandboxId, bool)>,
    recoveries: Vec<(SandboxId, SandboxLifecycle, SandboxQuarantineReasonCode)>,
}

/// Must be called inside the caller-owned node-publish transaction. Candidate
/// selection, execution retirement, receipt recording, and quarantine removal
/// all share the same row locks and rollback boundary.
async fn project_node_published_suspended_sandboxes(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: Json<serde_json::Value>,
) -> Result<SuspendedProjectionOutcome, ApplySandboxClaimsError> {
    let candidates = crate::sandbox_quarantine_store::select_suspended_projection_candidates(
        conn, node_id, rows_json,
    )
    .await?;
    if candidates.is_empty() {
        return Ok(SuspendedProjectionOutcome {
            rows: Vec::new(),
            recoveries: Vec::new(),
        });
    }
    let finalization_candidates = candidates
        .iter()
        .map(|candidate| SuspendedFinalizationCandidate {
            sandbox_id: candidate.sandbox_id,
            clear_marker: candidate.clear_marker,
            published_artifact: candidate.published_artifact.clone(),
            published_execution_identity: candidate.published_execution_identity.clone(),
        })
        .collect::<Vec<_>>();
    let rows = suspend_finalization::apply(conn, &finalization_candidates).await?;
    let projected_ids = rows
        .iter()
        .map(|(sandbox_id, _)| *sandbox_id)
        .collect::<BTreeSet<_>>();
    let mut recoveries = Vec::new();
    for candidate in candidates {
        let Some(recovery) = candidate.recovery else {
            continue;
        };
        if !projected_ids.contains(&candidate.sandbox_id) {
            continue;
        }
        crate::sandbox_quarantine_store::record_quarantine_recovery(
            conn,
            node_id,
            candidate.sandbox_id,
            &recovery,
        )
        .await?;
        recoveries.push((
            candidate.sandbox_id,
            recovery.prior_lifecycle,
            recovery.reason_code,
        ));
    }
    Ok(SuspendedProjectionOutcome { rows, recoveries })
}

pub(crate) async fn project_ready_suspend_snapshot(
    conn: &mut sqlx::PgConnection,
    snapshot_id: SnapshotId,
) -> Result<Option<(SandboxId, NodeId, String)>, Status> {
    let eligibility = started_suspend_revert_eligibility("s");
    let clear_marker = eligibility.clear_marker;
    let eligible = eligibility.eligible;
    let committed_suspend_evidence = "
        s.current_local_artifact_id = sn.local_artifact_id
        AND s.current_local_artifact_kind = sn.artifact_kind
        AND s.current_local_artifact_origin = 'suspend'
    ";
    let lifecycle_eligible =
        suspended_projection_lifecycle_eligible("s", committed_suspend_evidence);
    let candidate_query = format!(
        "SELECT s.id,
                {clear_marker} AS clear_marker,
                sn.source_node_id,
                s.lifecycle
           FROM snapshots sn
           JOIN sandboxes s ON s.id = sn.sandbox_id
          WHERE sn.id = $1
            AND sn.state = 'ready'
            AND sn.type = 'full'
            AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
            AND sn.source_node_id IS NOT NULL
            AND s.active_node_id = sn.source_node_id
            AND {lifecycle_eligible}
            AND EXISTS (
                SELECT 1
                  FROM snapshot_storage_locations ssl
                 WHERE ssl.snapshot_id = sn.id
            )
            AND {eligible}
          FOR UPDATE OF s",
    );
    let candidates: Vec<(SandboxId, bool, NodeId, String)> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(candidate_query.as_str())))
            .bind(snapshot_id)
            .fetch_all(&mut *conn)
            .await
            .map_err(|err| Status::internal(format!("select ready suspend projection: {err}")))?;
    let Some((sandbox_id, clear_marker, source_node_id, from_lifecycle)) =
        candidates.first().cloned()
    else {
        return Ok(None);
    };

    let projected = suspend_finalization::apply(
        conn,
        &[SuspendedFinalizationCandidate {
            sandbox_id,
            clear_marker,
            published_artifact: None,
            published_execution_identity: None,
        }],
    )
    .await?;
    if projected
        .iter()
        .any(|(projected_id, _)| *projected_id == sandbox_id)
    {
        Ok(Some((sandbox_id, source_node_id, from_lifecycle)))
    } else {
        Ok(None)
    }
}

pub async fn prune_absent_node_templates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    present_keys: &[(String, String)],
) -> Result<(), Status> {
    template_pruning::prune_census(conn, node_id, present_keys).await?;
    Ok(())
}

#[cfg(test)]
#[path = "../../published_state_lifecycle_race_tests.rs"]
mod published_state_lifecycle_race_tests;
#[cfg(test)]
#[path = "../../published_state_tests.rs"]
mod published_state_tests;

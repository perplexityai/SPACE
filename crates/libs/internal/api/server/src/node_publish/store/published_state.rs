use pplx_space_common_core::types::{NodeId, SnapshotId};

use std::collections::BTreeSet;

use sqlx::types::Json;
use tonic::Status;
use uuid::Uuid;

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

use crate::node_publish::plan::sandboxes::SandboxPlan;
use crate::node_publish::plan::suspended::SuspendedClaim;
use crate::node_publish::store::sandbox_input::LegacySandboxInput;
use crate::node_publish::store::{
    absent_sandboxes, activity, degraded_observations, failure_episodes,
    inventory_reconcile::{self, enqueue as queue_policy_reconcile_refs_for_new_inventory},
    lifecycle_transitions, network_policy, observed_state,
    quarantine_claims::{self, ClaimedQuarantineProjection},
    quarantine_persistence, ready_suspend, sandbox_facet_observations, snapshot_inventory,
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
    DegradedSandboxClaim, ValidatedSandboxInventory,
};
use crate::node_publish::validate::templates::ValidatedTemplateInventory;
use crate::proto::PublishedSandboxState;
#[cfg(test)]
use crate::proto::PublishedTemplateState;
#[cfg(test)]
use crate::sandbox_quarantine_store;

#[path = "published_state/sandbox_application.rs"]
mod sandbox_application;

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

#[cfg(test)]
async fn publish_template_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    publisher_role: NodeRole,
    states: &[PublishedTemplateState],
) -> Result<u64, Status> {
    let inventory = ValidatedTemplateInventory::try_from_proto(publisher_role, states)?;
    publish_validated_template_states(conn, node_id, &inventory)
        .await
        .map_err(Status::from)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyTemplateClaimsError {
    #[error(transparent)]
    Absent(#[from] template_pruning::RemoveAbsentTemplatesError),
    #[error(transparent)]
    Inventory(#[from] template_inventory::UpsertTemplateInventoryError),
    #[error(transparent)]
    Components(#[from] template_components::ReplaceTemplateComponentsError),
    #[error(transparent)]
    InventoryReconcile(#[from] inventory_reconcile::EnqueueInventoryReconcileError),
    #[error(transparent)]
    NetworkPolicy(#[from] template_network_policy::ObserveTemplatePolicyError),
}

/// Apply checked template evidence on the caller's connection. This phase uses
/// ordinary write locks in statement order, with no separate transaction or
/// explicit row-lock query. Zero matching inventory rows is a normal outcome.
pub(crate) async fn publish_validated_template_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    inventory: &ValidatedTemplateInventory,
) -> Result<u64, ApplyTemplateClaimsError> {
    let (absent_templates, rows) = inventory.legacy_rows();
    let mut accepted = template_pruning::remove_absent(conn, node_id, &absent_templates).await?;

    if rows.is_empty() {
        return Ok(accepted);
    }

    let rows_json = Json(serde_json::Value::Array(rows));
    let accepted_rows = template_inventory::upsert(conn, node_id, &rows_json).await?;

    template_components::replace(conn, node_id, &rows_json).await?;

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
    let plan = SandboxPlan::from(inventory);
    publish_validated_sandbox_states(conn, node_id, &plan)
        .await
        .map_err(Status::from)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EncodeQuarantineInputError {
    #[error("serialize quarantine input: {0}")]
    Input(#[source] serde_json::Error),
    #[error("serialize claimed quarantine input: {0}")]
    Claimed(#[source] serde_json::Error),
    #[error("serialize updated quarantine input: {0}")]
    Updated(#[source] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplySandboxClaimsError {
    #[error(transparent)]
    InventoryReconcile(#[from] inventory_reconcile::EnqueueInventoryReconcileError),
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
    QuarantineEncoding(#[from] EncodeQuarantineInputError),
}

/// Apply parsed node claims on the caller's connection. Lifecycle guards and
/// row locks remain in their existing statement order; an unmatched guard is
/// an ordinary outcome, not authority to retry an unguarded write.
pub(crate) async fn publish_validated_sandbox_states(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    plan: &SandboxPlan,
) -> Result<SandboxPublicationOutcome, ApplySandboxClaimsError> {
    for diagnostic in &plan.diagnostics {
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
    } = LegacySandboxInput::from(plan);
    let mut post_commit_events = Vec::new();
    let mut post_commit_state_convergence = Vec::new();

    let mut accepted = absent_sandboxes::remove(&mut *conn, node_id, absent_ids).await?;

    let degraded_accepted = degraded_observations::upsert(&mut *conn, node_id, state_only).await?;
    accepted = accepted.saturating_add(degraded_accepted);

    failure_episodes::reconcile(&mut *conn, node_id, failure_episode_rows).await?;

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
    let accepted_rows = observed_state::upsert(conn, node_id, &rows_json).await?;

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
    sandbox_facet_observations::upsert_suspend_handoff(conn, node_id, &plan.suspend_handoffs)
        .await?;

    post_commit_state_convergence.extend(accepted_rows.iter().filter_map(|row| {
        if !row.status_changed {
            return None;
        }
        Some(StateConvergenceSample {
            state: row.published_status.clone()?,
            changed_at: row.state_changed_at?,
        })
    }));
    sandbox_facet_observations::reconcile_quarantine_recovery(
        conn,
        node_id,
        &plan.recovery_observations,
        &plan.clear_recovery_observations,
    )
    .await?;
    activity::update(conn, node_id, &plan.activities).await?;

    let first_seen_sandbox_ids = accepted_rows
        .iter()
        .filter(|row| row.assigned_to_node && row.first_seen)
        .map(|row| row.sandbox_id)
        .collect::<Vec<_>>();
    sandbox_application::converge_lifecycles(conn, node_id, plan, &mut post_commit_events).await?;
    sandbox_application::project_quarantine(conn, node_id, quarantine_inputs).await?;
    sandbox_application::recover_pause_failed(
        conn,
        node_id,
        &plan.pause_failed_recoveries,
        &mut post_commit_events,
    )
    .await?;

    queue_policy_reconcile_refs_for_new_inventory(
        conn,
        node_id,
        sandbox_policy_reconcile_refs_for_new_sandboxes(&first_seen_sandbox_ids),
        ReconcileReason::NodeSandboxInventoryPublished,
    )
    .await?;

    if publishes_network_policy {
        network_policy::update_authority(conn, node_id, &rows_json).await?;
        network_policy::upsert(conn, node_id, &rows_json).await?;
        network_policy::clear_apply_failure(conn, node_id, &rows_json).await?;
    }

    snapshot_inventory::replace(conn, &accepted_ids, snapshot_rows).await?;
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
    claims: &[SuspendedClaim],
) -> Result<SuspendedProjectionOutcome, ApplySandboxClaimsError> {
    let candidates = crate::sandbox_quarantine_store::select_suspended_projection_candidates(
        conn, node_id, claims,
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
    ready_suspend::project(conn, snapshot_id)
        .await
        .map_err(Status::from)
}

#[cfg(test)]
#[path = "../../published_state_lifecycle_race_tests.rs"]
mod published_state_lifecycle_race_tests;
#[cfg(test)]
#[path = "../../published_state_tests.rs"]
mod published_state_tests;

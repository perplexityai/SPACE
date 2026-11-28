//! Transaction-scoped Postgres operations for node-authored quarantine recovery.

use pplx_space_accounts::NodeId;

use pplx_space_k8s_ids::PodUid;
use pplx_space_sandbox_metrics::publication::{
    StatePublicationComponent, StatePublicationDisposition, StatePublicationField,
};
use pplx_space_sandbox_model::{
    ExecutionGeneration, QuarantineRecoveryPlanRejectionCause, SandboxActivationAttemptId,
    SandboxCleanupEpisodeId, SandboxId, SandboxLifecycle, SandboxQuarantineHistoryAction,
    SandboxQuarantineReasonCode, SandboxQuarantineRecoveryPolicy, SandboxQuarantineSource,
    SandboxTransitionReceiptKind, SandboxTransitionTargetState,
};
use pplx_space_sandbox_quarantine_store::node_recovery_receipt::{self, NodeRecovery};
use pplx_space_sandbox_store::k8s_executions::quarantine_rollback;
use pplx_space_sandbox_store::quarantine_publication::{
    self, PauseFailedRecoveryCandidate, SuspendedProjectionBinds,
};
use pplx_space_sandbox_store::quarantine_recovery_authority;
use pplx_space_snapshot_model::{LocalArtifactId, SnapshotId};
use sqlx::PgPool;
use tonic::Status;
use uuid::Uuid;

pub(super) use pplx_space_sandbox_store::ingest::{
    PublishedExecutionIdentity, PublishedSuspendArtifact,
};

#[derive(Debug)]
pub(super) struct QuarantineRecoveryCandidate {
    pub(super) quarantine_id: Uuid,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) receipt_id: SandboxCleanupEpisodeId,
    pub(super) prior_lifecycle: SandboxLifecycle,
}

#[derive(Debug)]
pub(super) struct SuspendedProjectionCandidate {
    pub(super) sandbox_id: SandboxId,
    pub(super) clear_marker: bool,
    pub(super) published_artifact: Option<PublishedSuspendArtifact>,
    pub(super) published_execution_identity: Option<PublishedExecutionIdentity>,
    pub(super) captured_artifact: Option<LocalArtifactId>,
    pub(super) recovery: Option<QuarantineRecoveryCandidate>,
}

#[derive(Debug)]
pub(super) struct PauseFailedQuarantineRecovery {
    pub(super) sandbox_id: SandboxId,
    pub(super) target_state: SandboxTransitionTargetState,
}

#[derive(Debug)]
pub(super) struct ActivationRollbackCandidate {
    pub(super) sandbox_id: SandboxId,
    pub(super) receipt_id: SandboxCleanupEpisodeId,
    pub(super) execution_generation: i64,
    pub(super) activation_attempt_id: SandboxActivationAttemptId,
    pub(super) target_state: SandboxTransitionTargetState,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) prior_lifecycle: SandboxLifecycle,
    pub(super) failure_message: Option<String>,
}

#[derive(Debug)]
pub(super) struct QuarantineRecoveryPlanRow {
    pub(super) recovery_id: SandboxCleanupEpisodeId,
    pub(super) execution_generation: i64,
    pub(super) reason_code: SandboxQuarantineReasonCode,
    pub(super) snapshot_id: SnapshotId,
    pub(super) local_artifact_id: Option<LocalArtifactId>,
    pub(super) storage_uri: String,
    pub(super) bytes_uploaded: i64,
}

fn reject_quarantine_recovery_plan(
    sandbox_id: SandboxId,
    node_id: NodeId,
    cause: QuarantineRecoveryPlanRejectionCause,
    message: String,
) -> Status {
    crate::metrics::internal_api::quarantine_recovery_plan_rejected(cause);
    tracing::info!(
        %sandbox_id,
        %node_id,
        %cause,
        "quarantine recovery plan rejected"
    );
    Status::failed_precondition(message)
}

/// The one message every rejection that turns on node ownership returns.
///
/// The lookup is deliberately not scoped to the requesting node, so the cause
/// counter and the log can say whether the sandbox has no quarantine at all, no
/// active node, or an active node that is someone else. Returning those three
/// apart on the wire would make this endpoint an oracle: a node walking sandbox
/// ids would learn which ones exist, are quarantined out of `restoring`, and sit
/// on another node. The metric carries the distinction; the caller does not.
///
/// Every rejection *after* these three may carry its own message, and that is safe
/// only because these run first: a caller that is not the owning node never
/// reaches them, so `no_active_execution`, `unsupported_reason`, and the rest are
/// only ever seen by a node that already proved ownership of the sandbox. Moving
/// any of them above the ownership check turns that message into the oracle this
/// helper exists to close.
fn no_recovery_authority_message(sandbox_id: SandboxId) -> String {
    format!("sandbox {sandbox_id} has no active quarantine on this node")
}

pub(super) async fn get_quarantine_recovery_plan(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> Result<QuarantineRecoveryPlanRow, Status> {
    let recovery =
        quarantine_recovery_authority::get(pool, sandbox_id, SandboxQuarantineSource::SbxManager)
            .await
            .map_err(|error| Status::internal(format!("quarantine recovery lookup: {error}")))?
            .ok_or_else(|| {
                reject_quarantine_recovery_plan(
                    sandbox_id,
                    node_id,
                    QuarantineRecoveryPlanRejectionCause::NoActiveQuarantine,
                    no_recovery_authority_message(sandbox_id),
                )
            })?;

    match recovery.active_node_id {
        None => {
            return Err(reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::NoActiveNode,
                no_recovery_authority_message(sandbox_id),
            ));
        }
        Some(active_node_id) if active_node_id != node_id => {
            return Err(reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::WrongNode,
                no_recovery_authority_message(sandbox_id),
            ));
        }
        Some(_) => {}
    }
    let execution_generation = recovery.execution_generation.ok_or_else(|| {
        reject_quarantine_recovery_plan(
            sandbox_id,
            node_id,
            QuarantineRecoveryPlanRejectionCause::NoActiveExecution,
            format!("sandbox {sandbox_id} has no active execution"),
        )
    })?;

    let reason_code: SandboxQuarantineReasonCode =
        recovery.reason_code.parse().map_err(|error| {
            reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::UnsupportedReason,
                format!("sandbox {sandbox_id} has unsupported quarantine reason: {error}"),
            )
        })?;
    let policy = reason_code.automatic_recovery_policy().ok_or_else(|| {
        reject_quarantine_recovery_plan(
            sandbox_id,
            node_id,
            QuarantineRecoveryPlanRejectionCause::NotAutomaticallyRecoverable,
            format!("quarantine reason {reason_code} is not automatically recoverable"),
        )
    })?;
    let exact_artifact = match (
        recovery.current_local_artifact_id,
        recovery.current_local_artifact_kind.as_deref(),
        recovery.current_local_artifact_origin.as_deref(),
    ) {
        (Some(id), Some("full"), Some("suspend")) => Some(id),
        _ => None,
    };
    let snapshot = match (policy, exact_artifact) {
        (
            SandboxQuarantineRecoveryPolicy::ExactSuspendArtifact
            | SandboxQuarantineRecoveryPolicy::ExactOrPreQuarantineSuspendArtifact,
            Some(local_artifact_id),
        ) => {
            pplx_space_snapshot_store::quarantine_recovery::exact_artifact_snapshot(
                pool,
                sandbox_id,
                local_artifact_id,
            )
            .await
        }
        (SandboxQuarantineRecoveryPolicy::ExactSuspendArtifact, None) => {
            return Err(reject_quarantine_recovery_plan(
                sandbox_id,
                node_id,
                QuarantineRecoveryPlanRejectionCause::NoExactSuspendArtifact,
                format!("sandbox {sandbox_id} has no exact suspend artifact identity"),
            ));
        }
        (SandboxQuarantineRecoveryPolicy::ExactOrPreQuarantineSuspendArtifact, None) => {
            pplx_space_snapshot_store::quarantine_recovery::pre_quarantine_snapshot(
                pool,
                sandbox_id,
                recovery.entered_at,
            )
            .await
        }
    }
    .map_err(|error| Status::internal(format!("recovery snapshot lookup: {error}")))?;
    let snapshot = snapshot.ok_or_else(|| {
        reject_quarantine_recovery_plan(
            sandbox_id,
            node_id,
            QuarantineRecoveryPlanRejectionCause::NoCompatibleSnapshot,
            format!("sandbox {sandbox_id} has no recovery-policy-compatible suspend artifact"),
        )
    })?;

    Ok(QuarantineRecoveryPlanRow {
        recovery_id: SandboxCleanupEpisodeId::from_uuid(recovery.quarantine_id),
        execution_generation,
        reason_code,
        snapshot_id: snapshot.snapshot_id,
        local_artifact_id: snapshot.local_artifact_id,
        storage_uri: snapshot.storage_uri,
        bytes_uploaded: snapshot.size_bytes.unwrap_or(0),
    })
}

/// Acknowledge node-completed activations by releasing only the exact active
/// execution attempt that produced the receipt. This is the durable counterpart
/// to the API handler's best-effort fast-path release. If a newer suspend owns
/// the lifecycle, the exact activation attempt is still released, but its stale
/// target lifecycle is not projected onto the sandbox.
/// Route-less legacy residents retain ownership until post-ack route publication:
/// clearing it earlier would make that publication CAS permanently lose its fence.
pub(super) async fn consume_activation_completion_receipts(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: serde_json::Value,
) -> Result<Vec<SandboxId>, Status> {
    pplx_space_sandbox_store::ingest::release_completed_activations(conn, node_id, rows_json)
        .await
        .map_err(|error| {
            Status::internal(format!("consume activation completion receipts: {error}"))
        })
}

/// Lock only rollback receipts that still own the active central execution.
/// Both execution and activation-attempt identities must match. Execution
/// generations are deliberately reused by retries, so generation alone is not
/// a freshness fence.
pub(super) async fn select_activation_rollback_candidates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: serde_json::Value,
) -> Result<Vec<ActivationRollbackCandidate>, Status> {
    let rows = quarantine_rollback::lock_activation_rollback_candidates(
        conn,
        node_id,
        rows_json,
        SandboxTransitionReceiptKind::ActivationRollback,
    )
    .await
    .map_err(|error| Status::internal(format!("select activation rollback receipts: {error}")))?;

    let mut candidates = Vec::with_capacity(rows.len());
    for quarantine_rollback::ActivationRollbackCandidateRow {
        sandbox_id,
        receipt_id,
        execution_generation,
        activation_attempt_id,
        target_state,
        reason_code,
        prior_lifecycle,
        failure_message,
    } in rows
    {
        // The stored column has no CHECK, so it can hold text no lifecycle
        // names. That is one row's problem: skip its receipt and leave its
        // lifecycle as stored rather than fail the node's whole publication.
        let Ok(prior_lifecycle) = prior_lifecycle.parse::<SandboxLifecycle>() else {
            skip_non_canonical_stored_lifecycle(
                node_id,
                sandbox_id,
                StatePublicationDisposition::OmitTransitionReceipt,
            );
            continue;
        };
        candidates.push(ActivationRollbackCandidate {
            sandbox_id,
            receipt_id,
            execution_generation,
            activation_attempt_id,
            target_state: target_state.parse().map_err(|error| {
                Status::internal(format!(
                    "parse activation rollback target for {sandbox_id}: {error}"
                ))
            })?,
            reason_code: reason_code.parse().map_err(|error| {
                Status::internal(format!(
                    "parse activation rollback reason for {sandbox_id}: {error}"
                ))
            })?,
            prior_lifecycle,
            failure_message,
        });
    }
    Ok(candidates)
}

/// Count and log one candidate a publication projection skipped because of
/// its stored `sandboxes.lifecycle`. Pause-failed recovery skips a selected
/// row whose text is not exactly `quarantined`, which its exact write could
/// not move. Activation rollback skips a row whose trimmed, lower-cased text
/// names no lifecycle: its writes would match, but it needs the prior
/// lifecycle to decide restore retirement and to emit its event. The skipped
/// projection writes nothing for the row and leaves its lifecycle as stored;
/// the publication's other writes for that row, such as its observed state
/// and activity clocks, are not skipped.
fn skip_non_canonical_stored_lifecycle(
    node_id: NodeId,
    sandbox_id: SandboxId,
    disposition: StatePublicationDisposition,
) {
    pplx_space_sandbox_metrics::publication::degraded(
        StatePublicationComponent::Api,
        StatePublicationField::StoredLifecycle,
        disposition,
    );
    tracing::warn!(
        %node_id,
        %sandbox_id,
        %disposition,
        "skipping a sandbox whose stored lifecycle is not canonical without rejecting the publication"
    );
}

/// Consume rollback receipts after their generation fence has been locked.
/// Each statement is intentionally explicit: execution retirement, port
/// invalidation, and lifecycle convergence share the caller's transaction and
/// any invariant failure rolls all of them back.
///
/// The rolled-back execution is retired as `replaced`, which makes
/// `BeginArtifactBackup` refuse any capture of its generation (see
/// `backup_generation_fence`): the node gave up that activation, so a runtime
/// of it that still runs, such as one a kubelet retry of its pod brought up,
/// holds nothing the sandbox needs. A rollback to `suspended` is the one
/// exception, as it is for the API's own restore revert. It retires as
/// `replaced` only with abandoned-restore `retirement` enabled, for a row still
/// `restoring`, not an evacuation restore, whose execution the restore created,
/// and whose restore selection is proven to choose a ready, durably stored
/// pre-restore source. That retirement also clears `placement_changed_at`, as
/// the API's revert does: the restore's pod placement moved the clock past the
/// pre-restore source, and the suspended selector takes a markerless snapshot
/// only when it's newer than the last placement. Otherwise the execution reads
/// `suspended`, and the fence admits a capture that may be the only copy of the
/// sandbox's state: while retirement is off, a routing replica from before the
/// restoring gate may have served the relaunched runtime, and an execution the
/// restore reused may hold state of its own. The orphaned-restore-pod GC leaves
/// such a pod alone (`SkipClass::CaptureAdmitted`), so the capture comes from
/// the pod's teardown suspend when the next restore's pre-claim lingering-pod
/// sweep deletes the pod, or from an idle auto-suspend of the relaunched
/// runtime. A restore on that pod's node, its source node, then boots the
/// capture from the local bytes the teardown left; one on another node that
/// stages its selected storage URI doesn't, and a later restore may select the
/// capture.
pub(super) async fn apply_activation_rollback_candidates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    candidates: Vec<ActivationRollbackCandidate>,
    retirement: pplx_space_sandbox_model::AbandonedRestoreRetirement,
) -> Result<Vec<ActivationRollbackCandidate>, Status> {
    let mut applied = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let restore = candidate.target_state == SandboxTransitionTargetState::Suspended;
        let retires_restore = restore
            && retirement.is_enabled()
            && candidate.prior_lifecycle == SandboxLifecycle::Restoring
            && pplx_space_sandbox_store::quarantine::rollback_restore_retirable(
                &mut *conn,
                candidate.sandbox_id,
                candidate.execution_generation,
            )
            .await
            .map_err(|error| {
                Status::internal(format!("read activation rollback restore source: {error}"))
            })?;
        let execution_state = if restore && !retires_restore {
            "suspended"
        } else {
            "replaced"
        };
        let retired = quarantine_rollback::retire_rolled_back_execution(
            &mut *conn,
            candidate.sandbox_id,
            candidate.execution_generation,
            execution_state,
            candidate.activation_attempt_id,
        )
        .await
        .map_err(|error| Status::internal(format!("retire rollback execution: {error}")))?;
        if !retired {
            continue;
        }

        pplx_space_sandbox_store::port_exposures::invalidate_open_observations(
            &mut *conn,
            candidate.sandbox_id,
        )
        .await
        .map_err(|error| Status::internal(format!("invalidate rollback port state: {error}")))?;
        let converged = pplx_space_sandbox_store::quarantine::converge_activation_rollback(
            &mut *conn,
            node_id,
            candidate.sandbox_id,
            candidate.target_state,
            retires_restore,
        )
        .await
        .map_err(|error| Status::internal(format!("converge activation rollback: {error}")))?;
        if !converged {
            return Err(Status::aborted(format!(
                "activation rollback receipt {} lost sandbox ownership",
                candidate.receipt_id
            )));
        }
        applied.push(candidate);
    }
    Ok(applied)
}

/// Clear an sbx-manager-authored pause failure after the same node publishes a
/// definitive healthy runtime state. The caller's publication transaction
/// keeps the selected sandbox and quarantine rows locked through retirement.
///
/// The selection compares the stored lifecycle trimmed and lower-cased, but
/// the recovery's write compares it exactly. A selected row stored as a
/// non-canonical spelling of `quarantined`, such as ` Quarantined`, is skipped
/// before the write, and its lifecycle and quarantine are left as stored. Any
/// other miss of the write still fails the publication, since the write may
/// have applied in part.
pub(super) async fn recover_pause_failed_from_inventory(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: serde_json::Value,
) -> Result<Vec<PauseFailedQuarantineRecovery>, Status> {
    let candidates = quarantine_publication::lock_pause_failed_recovery_candidates(
        &mut *conn,
        node_id,
        rows_json,
        pplx_space_sandbox_store::ingest::PAUSE_FAILED_QUARANTINE_SOURCE,
        pplx_space_sandbox_store::ingest::PAUSE_FAILED_QUARANTINE_REASON,
    )
    .await
    .map_err(|error| {
        Status::internal(format!(
            "select pause-failed quarantine recoveries: {error}"
        ))
    })?;

    let mut recoveries = Vec::with_capacity(candidates.len());
    for PauseFailedRecoveryCandidate {
        id: sandbox_id,
        quarantine_id,
        published_lifecycle: target_state,
        lifecycle: stored_lifecycle,
    } in candidates
    {
        if !matches!(
            stored_lifecycle.parse::<SandboxLifecycle>(),
            Ok(SandboxLifecycle::Quarantined)
        ) {
            skip_non_canonical_stored_lifecycle(
                node_id,
                sandbox_id,
                StatePublicationDisposition::SkipPauseFailedRecovery,
            );
            continue;
        }
        let target_state = target_state.parse().map_err(|error| {
            Status::internal(format!(
                "parse pause-failed quarantine recovery target for {sandbox_id}: {error}"
            ))
        })?;
        let action = match target_state {
            SandboxTransitionTargetState::Started => {
                SandboxQuarantineHistoryAction::RecoveredToStarted
            }
            SandboxTransitionTargetState::Paused => {
                SandboxQuarantineHistoryAction::RecoveredToPaused
            }
            _ => {
                return Err(Status::internal(format!(
                    "unsupported node-reported quarantine recovery target {target_state}"
                )));
            }
        };
        let recovered = pplx_space_sandbox_store::ingest::recover_published_pause_failed(
            &mut *conn,
            node_id,
            sandbox_id,
            quarantine_id,
            target_state,
            action,
        )
        .await
        .map_err(|error| Status::internal(format!("record node quarantine recovery: {error}")))?;
        if !recovered {
            return Err(Status::aborted(format!(
                "pause-failed recovery CAS failed for {sandbox_id}"
            )));
        }

        recoveries.push(PauseFailedQuarantineRecovery {
            sandbox_id,
            target_state,
        });
    }
    Ok(recoveries)
}

/// Select node-published suspended projections and generation-fenced
/// quarantine recovery receipts in the node-publish transaction.
pub(super) async fn select_suspended_projection_candidates(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: serde_json::Value,
) -> Result<Vec<SuspendedProjectionCandidate>, Status> {
    let rows = quarantine_publication::lock_suspended_projection_candidates(
        &mut *conn,
        node_id,
        rows_json,
        SuspendedProjectionBinds {
            recovery_receipt_kind: SandboxTransitionReceiptKind::QuarantineRecovery,
            quarantine_source: SandboxQuarantineSource::SbxManager,
            exact_policy: SandboxQuarantineRecoveryPolicy::ExactSuspendArtifact,
            exact_or_pre_quarantine_policy:
                SandboxQuarantineRecoveryPolicy::ExactOrPreQuarantineSuspendArtifact,
        },
    )
    .await
    .map_err(|error| {
        Status::internal(format!("select suspended projection candidates: {error}"))
    })?;

    rows.into_iter()
        .map(
            |(
                sandbox_id,
                clear_marker,
                quarantine_id,
                reason,
                receipt_id,
                prior_lifecycle,
                published_artifact_id,
                published_artifact_kind,
                published_artifact_origin,
                published_execution_generation,
                published_pod_uid,
                published_pod_uid_was_missing,
                captured_artifact_id,
            )| {
                let published_artifact = match (
                    published_artifact_id,
                    published_artifact_kind,
                    published_artifact_origin,
                ) {
                    (None, None, None) => None,
                    (Some(id), Some(kind), Some(origin)) => Some(PublishedSuspendArtifact {
                        id: LocalArtifactId::from_uuid(id),
                        kind: kind.parse().map_err(|error| {
                            Status::internal(format!(
                                "parse suspended projection artifact kind for {sandbox_id}: {error}"
                            ))
                        })?,
                        origin: origin.parse().map_err(|error| {
                            Status::internal(format!(
                                "parse suspended projection artifact origin for {sandbox_id}: {error}"
                            ))
                        })?,
                    }),
                    _ => {
                        return Err(Status::internal(format!(
                            "suspended projection candidate {sandbox_id} has a partial published artifact"
                        )));
                    }
                };
                let published_execution_identity = match (
                    published_execution_generation,
                    published_pod_uid,
                    published_pod_uid_was_missing,
                ) {
                    (None, None, None) => None,
                    (Some(generation), Some(pod_uid), Some(pod_uid_was_missing)) => {
                        Some(PublishedExecutionIdentity {
                        generation: ExecutionGeneration::new(generation).map_err(|error| {
                            Status::internal(format!(
                                "parse suspended projection generation for {sandbox_id}: {error}"
                            ))
                        })?,
                        pod_uid: PodUid::parse(pod_uid).map_err(|error| {
                            Status::internal(format!(
                                "parse suspended projection pod UID for {sandbox_id}: {error}"
                            ))
                        })?,
                        pod_uid_was_missing,
                    })
                    }
                    _ => {
                        return Err(Status::internal(format!(
                            "suspended projection candidate {sandbox_id} has a partial execution identity"
                        )));
                    }
                };
                let recovery = match (reason, receipt_id) {
                    (None, None) => None,
                    (Some(reason), Some(receipt_id)) => Some(QuarantineRecoveryCandidate {
                        quarantine_id: quarantine_id.ok_or_else(|| {
                            Status::internal(format!(
                                "quarantine recovery candidate {sandbox_id} has no episode"
                            ))
                        })?,
                        reason_code: reason.parse().map_err(|error| {
                            Status::internal(format!(
                                "parse quarantine recovery reason for {sandbox_id}: {error}"
                            ))
                        })?,
                        receipt_id,
                        prior_lifecycle,
                    }),
                    _ => {
                        return Err(Status::internal(format!(
                            "quarantine recovery candidate {sandbox_id} has a partial receipt"
                        )));
                    }
                };
                Ok(SuspendedProjectionCandidate {
                    sandbox_id,
                    clear_marker,
                    published_artifact,
                    published_execution_identity,
                    captured_artifact: captured_artifact_id.map(LocalArtifactId::from_uuid),
                    recovery,
                })
            },
        )
        .collect()
}

/// Record a receipt only after finalization succeeded, then retire the exact
/// central quarantine episode that authorized the node-local recovery.
pub(super) async fn record_quarantine_recovery(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    sandbox_id: SandboxId,
    recovery: &QuarantineRecoveryCandidate,
) -> Result<(), Status> {
    let history_id = recovery.receipt_id.as_uuid();
    let quarantine_id = recovery.quarantine_id;
    let inserted = node_recovery_receipt::record_node_recovery(
        &mut *conn,
        &NodeRecovery {
            history_id,
            sandbox_id,
            quarantine_id,
            quarantined_by: SandboxQuarantineSource::SbxManager,
            reason_code: recovery.reason_code,
            recorded_by: SandboxQuarantineSource::NodeStatePublish,
            action: SandboxQuarantineHistoryAction::RecoveredToSuspended,
            node_id,
        },
    )
    .await
    .map_err(|error| Status::internal(format!("record quarantine recovery: {error}")))?;
    if inserted != 1 {
        return Err(Status::aborted(format!(
            "quarantine recovery history CAS affected {inserted} rows for {sandbox_id}"
        )));
    }
    let removed =
        node_recovery_receipt::retire_node_recovered_episode(&mut *conn, sandbox_id, quarantine_id)
            .await
            .map_err(|error| Status::internal(format!("retire quarantine recovery: {error}")))?;
    if removed != 1 {
        return Err(Status::aborted(format!(
            "quarantine recovery retirement CAS affected {removed} rows for {sandbox_id}"
        )));
    }
    Ok(())
}

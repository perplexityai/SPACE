use super::{
    ApplySandboxClaimsError, BTreeSet, ClaimedQuarantineProjection, EncodeQuarantineInputError,
    Json, NodeId, PostCommitSandboxEvent, QuarantineClaim, QuarantineProjectionSqlInput,
    SandboxEventType, SandboxLifecycle, SandboxQuarantineReasonCode, SandboxTransitionTargetState,
    emit_lifecycle_convergence_events, emit_node_published_state_events, lifecycle_transitions,
    project_node_published_suspended_sandboxes, quarantine_claims, quarantine_persistence,
};
use crate::node_publish::plan::sandboxes::SandboxPlan;
use crate::sandbox_quarantine_store;

/// Preserve lifecycle statement and event order on the publication connection.
/// Each guarded writer retains its own row locks and zero-match semantics.
pub(super) async fn converge_lifecycles(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: &Json<serde_json::Value>,
    plan: &SandboxPlan,
    post_commit_events: &mut Vec<PostCommitSandboxEvent>,
) -> Result<(), ApplySandboxClaimsError> {
    let restored_ids = lifecycle_transitions::converge_restored(conn, node_id, rows_json).await?;
    emit_lifecycle_convergence_events(
        node_id,
        &restored_ids,
        SandboxEventType::RestoreCompleted,
        Some("restoring"),
        "started",
    );

    sandbox_quarantine_store::consume_activation_completion_receipts(
        conn,
        node_id,
        &plan.activation_completions,
    )
    .await?;
    let rollback_candidates = sandbox_quarantine_store::select_activation_rollback_candidates(
        conn,
        node_id,
        &plan.activation_rollbacks,
    )
    .await?;
    let applied_rollbacks = sandbox_quarantine_store::apply_activation_rollback_candidates(
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
        project_node_published_suspended_sandboxes(conn, node_id, rows_json).await?;
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

    let stopped_ids = lifecycle_transitions::converge_stopped(conn, node_id, &plan.stopped).await?;
    emit_lifecycle_convergence_events(
        node_id,
        &stopped_ids,
        SandboxEventType::StopCompleted,
        None,
        "stopped",
    );
    let failed_sandboxes =
        lifecycle_transitions::converge_failed(conn, node_id, &plan.failed).await?;
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
    Ok(())
}

/// Claim and record quarantine evidence while retaining the caller's locks.
/// Claims that lose their guard produce no history or state record.
pub(super) async fn project_quarantine(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    quarantine_inputs: &[QuarantineClaim],
) -> Result<(), ApplySandboxClaimsError> {
    let mut quarantined_sandboxes = Vec::new();
    if !quarantine_inputs.is_empty() {
        let quarantine_sql_inputs = quarantine_inputs
            .iter()
            .map(QuarantineProjectionSqlInput::materialize)
            .collect::<Vec<_>>();
        let quarantine_rows_json = Json(
            serde_json::to_value(&quarantine_sql_inputs)
                .map_err(EncodeQuarantineInputError::Input)?,
        );
        let claimed = quarantine_claims::select(conn, node_id, quarantine_rows_json).await?;
        if !claimed.is_empty() {
            let claimed_sql = claimed
                .iter()
                .map(ClaimedQuarantineProjection::sql_projection)
                .collect::<Vec<_>>();
            let claimed_json = Json(
                serde_json::to_value(&claimed_sql).map_err(EncodeQuarantineInputError::Claimed)?,
            );
            let updated_ids = quarantine_claims::apply(conn, node_id, claimed_json).await?;
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
                let updated_json = Json(
                    serde_json::to_value(&updated_sql)
                        .map_err(EncodeQuarantineInputError::Updated)?,
                );
                quarantine_persistence::record_state(conn, node_id, &updated_json).await?;
                quarantine_persistence::record_history(conn, node_id, &updated_json).await?;
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
    Ok(())
}

/// Append successful pause-failure recovery events in guarded-write order.
/// The caller emits the returned events only after committing its transaction.
pub(super) async fn recover_pause_failed(
    conn: &mut sqlx::PgConnection,
    node_id: NodeId,
    rows_json: &Json<serde_json::Value>,
    post_commit_events: &mut Vec<PostCommitSandboxEvent>,
) -> Result<(), ApplySandboxClaimsError> {
    let recoveries =
        sandbox_quarantine_store::recover_pause_failed_from_inventory(conn, node_id, rows_json)
            .await?;
    post_commit_events.extend(
        recoveries
            .into_iter()
            .map(|recovery| PostCommitSandboxEvent {
                sandbox_id: recovery.sandbox_id,
                event_type: SandboxEventType::QuarantineRecovered,
                prior_lifecycle: SandboxLifecycle::Quarantined,
                target_lifecycle: recovery.target_state,
                reason_code: SandboxQuarantineReasonCode::PauseFailed,
                error: None,
            }),
    );
    Ok(())
}

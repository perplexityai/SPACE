//! Guarded writers of `sandboxes.state`: activation, completion, suspend, and rollback.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationKind {
    ColdStart,
    Restore,
}

impl ActivationKind {
    pub const fn activation_state(self) -> SandboxState {
        match self {
            Self::ColdStart => SandboxState::Starting,
            Self::Restore => SandboxState::Restoring,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BeginActivationError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox state {state} cannot begin a {kind:?} activation")]
    UnsupportedState {
        kind: ActivationKind,
        state: SandboxState,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RollbackActivationEpisodeError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox state {state} is not an activation state")]
    UnsupportedActivationState { state: SandboxState },
    #[error("an activation episode cannot roll back to quarantined")]
    QuarantinedTarget,
}

#[derive(Debug, thiserror::Error)]
pub enum SupersedeActivationError {
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    #[error("sandbox quarantine recovery is already in progress")]
    QuarantineRecoveryInProgress,
    #[error(transparent)]
    StateWriteNotApplied(#[from] SandboxStateWriteNotApplied),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use = "callers must distinguish an applied state write from a superseded one"]
pub enum SandboxStateWriteOutcome {
    Applied,
    Missing,
    BlockedByQuarantine,
    SupersededByCleanupEpisode,
}

impl SandboxStateWriteOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Missing => "missing",
            Self::BlockedByQuarantine => "blocked_by_quarantine",
            Self::SupersededByCleanupEpisode => "superseded_by_cleanup_episode",
        }
    }

    pub const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }

    pub fn require_applied(self) -> std::result::Result<(), SandboxStateWriteNotApplied> {
        if self.is_applied() {
            Ok(())
        } else {
            Err(SandboxStateWriteNotApplied { outcome: self })
        }
    }
}

impl fmt::Display for SandboxStateWriteOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("sandbox state write was not applied: {outcome}")]
pub struct SandboxStateWriteNotApplied {
    pub outcome: SandboxStateWriteOutcome,
}

pub fn update_state(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> Result<SandboxStateWriteOutcome> {
    if matches!(state, SandboxState::Quarantined | SandboxState::Failed) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if state == SandboxState::Deleting && conn.is_autocommit() {
        let tx = immediate_transaction(conn)?;
        let outcome = update_state(&tx, id, state)?;
        tx.commit()?;
        return Ok(outcome);
    }
    let previous_state = current_state_label(conn, id)?;
    let previous_state_value = previous_state
        .as_deref()
        .and_then(|state| state.parse::<SandboxState>().ok());
    match generic_state_write_refusal(conn, id, previous_state_value, state)? {
        Some(StateWriteRefusal::Quarantined) => {
            tracing::warn!(
                sandbox_id = %id,
                target_state = %state,
                "ignored generic sandbox state update because sandbox is quarantined"
            );
            return Ok(SandboxStateWriteOutcome::BlockedByQuarantine);
        }
        // The generic guards never answer `FromState`.
        Some(StateWriteRefusal::CleanupOwned | StateWriteRefusal::FromState) => {
            tracing::warn!(
                sandbox_id = %id,
                target_state = %state,
                "ignored generic sandbox state update because cleanup owns the transition"
            );
            return Ok(SandboxStateWriteOutcome::SupersededByCleanupEpisode);
        }
        None => {}
    }

    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?1 = 'restoring' THEN restore_origin ELSE NULL END,
             restore_phase = CASE WHEN ?1 = 'restoring' THEN restore_phase ELSE NULL END,
             restore_storage_uri = CASE WHEN ?1 = 'restoring' THEN restore_storage_uri ELSE NULL END,
             transition_receipt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_id
                 ELSE NULL
             END,
             transition_receipt_kind = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_kind
                 ELSE NULL
             END,
             transition_receipt_target_state = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_target_state
                 ELSE NULL
             END,
             transition_receipt_execution_generation = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_execution_generation
                 ELSE NULL
             END,
             transition_receipt_activation_attempt_id = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_activation_attempt_id
                 ELSE NULL
             END,
             transition_receipt_reason_code = CASE
                 WHEN ?1 = transition_receipt_target_state THEN transition_receipt_reason_code
                 ELSE NULL
             END,
             updated_at = ?2
         WHERE id = ?3
           AND (?1 = 'deleting' OR state != 'quarantined')",
        params![state.as_str(), now()?, id],
    )?;
    if changed != 0 && state == SandboxState::Deleting {
        teardown_jobs::fail_active(conn, id, TeardownSuspendFailure::Deleting)?;
    }
    Ok(if changed == 0 {
        SandboxStateWriteOutcome::Missing
    } else {
        SandboxStateWriteOutcome::Applied
    })
}

pub(crate) fn restore_checkpoint_state(
    conn: &Connection,
    id: &str,
    stable_state: SandboxState,
) -> Result<bool> {
    if !matches!(stable_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'checkpointing'",
        params![stable_state.as_str(), now()?, id],
    )?;
    Ok(changed == 1)
}

/// Fence policy installation and Paused -> Resuming in one transaction.
/// An absent generation matches only an identity-free legacy resident; it is
/// never a wildcard for an execution that has acquired a generation.
pub(crate) fn begin_resident_resume_with_policy(
    conn: &Connection,
    id: &str,
    generation: Option<ExecutionGeneration>,
    policy: &AutoLifecyclePolicy,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let Some(sandbox) = get_sandbox(&tx, id)? else {
        return Ok(false);
    };
    if sandbox.state != SandboxState::Paused
        || sandbox.space_api_route_identity.execution_generation != generation
    {
        return Ok(false);
    }
    if update_state(&tx, id, SandboxState::Resuming)? != SandboxStateWriteOutcome::Applied {
        return Ok(false);
    }
    install_activation_auto_lifecycle(&tx, id, policy)?;
    tx.commit()?;
    Ok(true)
}

pub(crate) fn restore_resident_resume_state(
    conn: &Connection,
    id: &str,
    stable_state: SandboxState,
) -> Result<bool> {
    if !matches!(stable_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let tx = immediate_transaction(conn)?;
    let changed = rollback_activation_state(&tx, id, SandboxState::Resuming, stable_state)?;
    if changed && stable_state == SandboxState::Started {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    tx.commit()?;
    Ok(changed)
}

pub(crate) fn recover_interrupted_pause_state(
    conn: &Connection,
    id: &str,
    target: PauseFailedRecoveryTarget,
) -> Result<bool> {
    let target_state = target.sandbox_state();
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'pausing'",
        params![target_state.as_str(), now()?, id],
    )?;
    Ok(changed == 1)
}

/// Transfer lifecycle ownership from an armed activation to an explicit
/// stop/delete teardown. The caller is responsible for completing that
/// teardown; the activation guard becomes a superseded no-op.
#[cfg(test)]
fn update_state_superseding_activation(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> std::result::Result<(), SupersedeActivationError> {
    debug_assert!(matches!(
        state,
        SandboxState::Stopping | SandboxState::Deleting
    ));
    let tx = immediate_transaction(conn)?;
    update_state_superseding_activation_in_transaction(&tx, id, state)?;
    tx.commit()?;
    Ok(())
}

pub(crate) fn update_state_superseding_activation_in_transaction(
    conn: &Connection,
    id: &str,
    state: SandboxState,
) -> std::result::Result<(), SupersedeActivationError> {
    debug_assert!(matches!(
        state,
        SandboxState::Stopping | SandboxState::Deleting
    ));
    if let Some(episode) = cleanup_episode(conn, id)? {
        if state == SandboxState::Stopping
            && (episode.entered_from_state == Some(SandboxState::Quarantined)
                || episode.quarantine_recovery.is_some())
        {
            return Err(SupersedeActivationError::QuarantineRecoveryInProgress);
        }
        conn.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, episode.id.to_string()],
        )?;
        conn.execute(
            "UPDATE sandboxes
                SET cleanup_completion = NULL,
                    cleanup_keep_filesystem = NULL
              WHERE id = ?1
                AND cleanup_completion = ?2
                AND cleanup_keep_filesystem = ?3",
            params![id, episode.completion.as_str(), episode.keep_filesystem],
        )?;
    }
    update_state(conn, id, state)?.require_applied()?;
    Ok(())
}

#[cfg(test)]
pub fn mark_stopping_superseding_activation(
    conn: &Connection,
    id: &str,
) -> std::result::Result<(), SupersedeActivationError> {
    update_state_superseding_activation(conn, id, SandboxState::Stopping)
}

#[cfg(test)]
pub fn mark_deleting_superseding_activation(
    conn: &Connection,
    id: &str,
) -> std::result::Result<(), SupersedeActivationError> {
    update_state_superseding_activation(conn, id, SandboxState::Deleting)
}

/// Claim the caller-observed sandbox state for activation.
///
/// A false result means the row was removed or another state transition won;
/// the caller must not allocate or launch runtime resources.
pub fn begin_activation(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    kind: ActivationKind,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    begin_activation_with_route_evidence(
        conn,
        id,
        expected_state,
        kind,
        execution_generation,
        activation_attempt_id,
        ActivationRouteAdmission::default(),
    )
}

#[derive(Default)]
pub(crate) struct ActivationRouteAdmission<'a> {
    pub(crate) auto_lifecycle: Option<&'a AutoLifecyclePolicy>,
    pub(crate) operation_activation_attempt_id: Option<SandboxActivationAttemptId>,
    pub(crate) evidence: Option<crate::auto_lifecycle_delete_receipts::RouteEvidence>,
    pub(crate) open_claim: Option<&'a crate::route_activation_claims::OpenRouteActivationClaim>,
}

pub(crate) fn begin_activation_with_route_evidence(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    kind: ActivationKind,
    execution_generation: Option<ExecutionGeneration>,
    activation_attempt_id: Option<SandboxActivationAttemptId>,
    route_admission: ActivationRouteAdmission<'_>,
) -> std::result::Result<Option<CleanupEpisode>, BeginActivationError> {
    let completion = match (kind, expected_state) {
        (ActivationKind::ColdStart, SandboxState::Created) => CleanupCompletion::MarkCreated,
        (ActivationKind::ColdStart, SandboxState::Stopped) => CleanupCompletion::MarkStopped,
        (ActivationKind::ColdStart, SandboxState::Suspended) => CleanupCompletion::MarkSuspended,
        (ActivationKind::Restore, SandboxState::Suspended) => CleanupCompletion::MarkSuspended,
        _ => {
            return Err(BeginActivationError::UnsupportedState {
                kind,
                state: expected_state,
            });
        }
    };
    let activation_state = kind.activation_state();
    let tx = immediate_transaction(conn)?;
    if cleanup_episode(&tx, id)?.is_some() {
        tx.commit()?;
        return Ok(None);
    }
    let keep_filesystem = true;
    // Only activation authorizes node allocation. Filesystem-only staging keeps
    // an unbound request identity and must not become an allocation predecessor.
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?1 = 'restoring' THEN 'local' ELSE NULL END,
             restore_phase = CASE WHEN ?1 = 'restoring' THEN 'activation' ELSE NULL END,
             restore_storage_uri = NULL,
             cleanup_completion = ?2,
             cleanup_keep_filesystem = ?3,
             previous_node_execution_id = CASE WHEN node_execution_retired = 0 THEN node_execution_id END,
             node_execution_id = ?8,
             node_execution_retired = 0,
             transition_receipt_id = NULL,
             transition_receipt_kind = NULL,
             transition_receipt_target_state = NULL,
             transition_receipt_execution_generation = NULL,
             transition_receipt_reason_code = NULL,
             transition_receipt_activation_attempt_id = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?6 ELSE state_changed_at END,
             updated_at = ?6
         WHERE id = ?4
           AND state = ?5
           AND state != 'quarantined'
           AND NOT EXISTS (
               SELECT 1
                 FROM auto_lifecycle_delete_outbox
                WHERE sandbox_id = ?4
                  AND state = 'pending'
                  AND (?7 IS NULL OR execution_generation >= ?7)
           )
           AND (
               (cleanup_completion IS NULL AND cleanup_keep_filesystem IS NULL)
               OR (cleanup_completion = ?2 AND cleanup_keep_filesystem = ?3)
           )",
        params![
            activation_state.as_str(),
            completion.as_str(),
            keep_filesystem,
            id,
            expected_state.as_str(),
            now()?,
            execution_generation.map(ExecutionGeneration::get),
            SandboxActivationAttemptId::new().to_string(),
        ],
    )?;
    if changed != 1 {
        tx.commit()?;
        return Ok(None);
    }
    if let Some(policy) = route_admission.auto_lifecycle {
        install_activation_auto_lifecycle(&tx, id, policy)?;
    }
    let sandbox_id = (route_admission.evidence.is_some() || execution_generation.is_some())
        .then(|| {
            id.parse::<SandboxId>()
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })
        .transpose()?;
    match route_admission.open_claim {
        Some(open_route_activation) => {
            crate::route_activation_claims::insert_create_claim(&tx, id, open_route_activation)?;
        }
        None => {
            // Beginning any other activation supersedes an older Kubernetes
            // continuation ticket in the same transaction that claims the row.
            crate::route_activation_claims::close_claim(&tx, id)?;
        }
    }
    if let Some(route_evidence) = route_admission.evidence {
        let sandbox_id = sandbox_id.ok_or(rusqlite::Error::InvalidQuery)?;
        crate::auto_lifecycle_delete_receipts::db::record_retained_route_evidence(
            &tx,
            sandbox_id,
            route_evidence,
        )?;
    }
    let receipt_supersession = match sandbox_id {
        Some(sandbox_id) => crate::auto_lifecycle_delete_receipts::db::discard_pending_receipt_for_execution_advance(
            &tx,
            sandbox_id,
            execution_generation,
        )?,
        None => None,
    };
    let cleanup_id = SandboxCleanupEpisodeId::new();
    insert_cleanup_episode(
        &tx,
        id,
        CleanupEpisodeSpec {
            id: cleanup_id,
            completion,
            keep_filesystem,
            entered_from_state: Some(expected_state),
            activation_state,
            execution_generation,
            activation_attempt_id,
        },
    )?;
    tx.execute(
        "UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = ?3
         WHERE sandbox_id = ?1 AND cleanup_id = ?2",
        params![
            id,
            cleanup_id.to_string(),
            route_admission
                .operation_activation_attempt_id
                .map(|id| id.to_string())
        ],
    )?;
    let episode = cleanup_episode(&tx, id)?.filter(|episode| episode.id == cleanup_id);
    tx.commit()?;
    if let Some(sandbox_id) = sandbox_id {
        emit_auto_lifecycle_delete_receipt_superseded_if_present(sandbox_id, receipt_supersession);
    }
    Ok(episode)
}

/// Stamp the terminal-teardown-suspend marker on a `suspending` row whose
/// durable backup the cluster refused permanently.
///
/// Scoped to the exact staged artifact the failed backup covered, so a row
/// that has already moved on keeps its own witness. The write is conditional
/// on the reason actually changing: the shim re-polls `PrepareTeardownSuspend`
/// for as long as the pod is terminating, and each poll re-drives the suspend
/// through this same terminal job. An unconditional stamp would bump
/// `updated_at` on every poll and hold the row permanently inside the
/// cluster-deleted eviction's quiescence window — the one thing that can
/// reclaim it.
pub fn mark_teardown_suspend_backup_permanently_failed(
    conn: &Connection,
    id: &str,
    staged_artifact: LocalArtifactId,
) -> Result<bool> {
    let reason = pplx_space_sbx_mgr_client::TEARDOWN_SUSPEND_BACKUP_PERMANENTLY_FAILED_REASON;
    let changed = conn.execute(
        "UPDATE sandboxes
         SET failure_reason = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'suspending'
           AND current_local_artifact_id = ?4
           AND (failure_reason IS NULL OR failure_reason != ?1)",
        params![reason, now()?, id, staged_artifact.to_string()],
    )?;
    Ok(changed == 1)
}

pub fn mark_cross_node_restore_staging_failed(
    conn: &Connection,
    id: &str,
    failure_reason: &str,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET failure_reason = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'",
        params![failure_reason, now()?, id],
    )?;
    Ok(changed == 1)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActivationOperationError {
    #[error("activation receipt database operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("sandbox no longer exists")]
    MissingSandbox,
    #[error("activation route evidence does not match the selected execution")]
    RouteMismatch,
    #[error("activation operation was superseded")]
    Superseded,
    #[error("activation target has not been reached or is unsupported")]
    InvalidTarget,
    #[error("activation cleanup is still pending")]
    CleanupPending,
}

/// Join an armed activation without changing its route-ownership fence. The
/// update is durable before a Wake waiter can disappear; cleanup will report
/// the joined operation even if the original worker finishes asynchronously.
pub(crate) fn bind_activation_operation(
    conn: &Connection,
    id: &str,
    route: &SpaceApiRouteIdentity,
    operation: SandboxActivationAttemptId,
) -> std::result::Result<(), ActivationOperationError> {
    let tx = immediate_transaction(conn)?;
    if let Some(episode) = cleanup_episode(&tx, id)?
        && episode.requested_at.is_none()
    {
        if episode.execution_generation != route.execution_generation
            || episode.activation_attempt_id != route.activation_attempt_id
        {
            return Err(ActivationOperationError::RouteMismatch);
        }
        if episode
            .operation_activation_attempt_id
            .is_some_and(|current| current > operation)
        {
            return Err(ActivationOperationError::Superseded);
        }
        tx.execute(
            "UPDATE sandbox_cleanup_episodes SET operation_activation_attempt_id = ?3
             WHERE sandbox_id = ?1 AND cleanup_id = ?2 AND phase = 'armed'",
            params![id, episode.id.to_string(), operation.to_string()],
        )?;
    }
    tx.commit().map_err(Into::into)
}

/// A resident Wake has no cleanup episode. Persist fresh completion evidence
/// while its wake lock is held, after identity adoption and before fallible
/// gateway sync. Lifecycle completion does not depend on that network call. This
/// also closes a join whose activation completed before we bound its episode.
pub(crate) fn complete_resident_wake_operation(
    conn: &Connection,
    id: &str,
    route: &SpaceApiRouteIdentity,
    operation: SandboxActivationAttemptId,
    target: SandboxState,
) -> std::result::Result<(), ActivationOperationError> {
    let tx = immediate_transaction(conn)?;
    let row = get_sandbox(&tx, id)?.ok_or(ActivationOperationError::MissingSandbox)?;
    let actual = &row.space_api_route_identity;
    if row.state != target {
        return Err(ActivationOperationError::InvalidTarget);
    }
    if cleanup_episode(&tx, id)?.is_some() {
        return Err(ActivationOperationError::CleanupPending);
    }
    if actual.clone().try_into_complete().is_err()
        || actual.execution_generation != route.execution_generation
        || actual.route_generation != route.route_generation
        || actual.activation_attempt_id != route.activation_attempt_id
        || actual.pod_uid != route.pod_uid
        || actual.effective_substrate_kind() != route.effective_substrate_kind()
    {
        return Err(ActivationOperationError::RouteMismatch);
    }
    let generation = route
        .execution_generation
        .ok_or(ActivationOperationError::RouteMismatch)?;
    let target =
        activation_completed_target(target).ok_or(ActivationOperationError::InvalidTarget)?;
    let previous: (Option<i64>, Option<String>, Option<String>, Option<String>) = tx.query_row(
        "SELECT transition_receipt_execution_generation, transition_receipt_activation_attempt_id,
                transition_receipt_kind, transition_receipt_target_state
         FROM sandboxes WHERE id = ?1",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if previous.0 == Some(generation.get())
        && let Some(previous_id) = previous.1
    {
        let previous_operation: SandboxActivationAttemptId =
            previous_id.parse().map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
            })?;
        if previous_operation > operation {
            // Do not replace a potentially newer completion. UUID ordering is
            // only a conservative receipt-retention policy: PostgreSQL clears
            // completed owners, so clock skew can make a legitimate later
            // no-op Wake's ID lower. It must not reject that healthy Wake.
            // Its caller can release the exact lease from lifecycle readiness;
            // no activation cleanup obligation exists on this resident path.
            return tx.commit().map_err(Into::into);
        }
        if previous_operation == operation
            && previous.2.as_deref() == Some("activation_completed")
            && previous.3.as_deref() == Some(target.as_str())
        {
            return tx.commit().map_err(Into::into);
        }
    }
    tx.execute(
        "UPDATE sandboxes SET transition_receipt_id = ?2,
            transition_receipt_kind = 'activation_completed',
            transition_receipt_target_state = ?3,
            transition_receipt_execution_generation = ?4,
            transition_receipt_activation_attempt_id = ?5,
            transition_receipt_reason_code = NULL, updated_at = ?6
         WHERE id = ?1",
        params![
            id,
            SandboxCleanupEpisodeId::new().to_string(),
            target.as_str(),
            generation.get(),
            operation.to_string(),
            now()?
        ],
    )?;
    tx.commit().map_err(Into::into)
}

fn activation_completed_target(state: SandboxState) -> Option<SandboxTransitionTargetState> {
    match state {
        SandboxState::Created => Some(SandboxTransitionTargetState::Created),
        SandboxState::Started => Some(SandboxTransitionTargetState::Started),
        SandboxState::Paused => Some(SandboxTransitionTargetState::Paused),
        _ => None,
    }
}

/// Leave a durable acknowledgement for the exact control-plane activation
/// attempt before retiring its cleanup episode. The API may clear the attempt
/// eagerly, but this receipt closes the crash window between node success and
/// that best-effort control-plane write.
fn persist_activation_completed_receipt(
    tx: &rusqlite::Transaction<'_>,
    sandbox_id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    target_state: SandboxState,
) -> Result<()> {
    let Some(episode) = cleanup_episode(tx, sandbox_id)?.filter(|episode| episode.id == cleanup_id)
    else {
        return Ok(());
    };
    let (Some(execution_generation), Some(activation_attempt_id), Some(target_state)) = (
        episode.execution_generation,
        // Explicit operation ownership wins; old episodes retain legacy receipts.
        episode
            .operation_activation_attempt_id
            .or(episode.activation_attempt_id),
        activation_completed_target(target_state),
    ) else {
        return Ok(());
    };
    let receipt = SandboxTransitionReceipt::<SandboxQuarantineReasonCode>::activation_completed(
        cleanup_id,
        target_state,
        execution_generation,
        activation_attempt_id,
    );
    tx.execute(
        "UPDATE sandboxes
            SET transition_receipt_id = ?1,
                transition_receipt_kind = ?2,
                transition_receipt_target_state = ?3,
                transition_receipt_execution_generation = ?4,
                transition_receipt_activation_attempt_id = ?5,
                transition_receipt_reason_code = NULL
          WHERE id = ?6 AND state = ?3",
        params![
            receipt.id().to_string(),
            receipt.kind().as_str(),
            receipt.target_state().as_str(),
            receipt.execution_generation().get(),
            receipt.activation_attempt_id().map(|id| id.to_string()),
            sandbox_id,
        ],
    )?;
    Ok(())
}

pub fn mark_start_succeeded(
    conn: &Connection,
    id: &str,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'started',
             failure_reason = NULL,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             state_changed_at = CASE WHEN state != 'started' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'starting'
           AND (?3 IS NULL OR EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes episode
                WHERE episode.sandbox_id = sandboxes.id
                  AND episode.cleanup_id = ?3
                  AND episode.phase = 'armed'
           ))",
        params![now()?, id, cleanup_id.map(|id| id.to_string())],
    )?;
    if changed == 1
        && let Some(cleanup_id) = cleanup_id
    {
        persist_activation_completed_receipt(&tx, id, cleanup_id, SandboxState::Started)?;
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    if changed == 1 {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    if changed == 1 {
        crate::route_activation_claims::close_claim(&tx, id)?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

/// Publish a disposable clone only while its original cleanup episode is armed.
/// Publication and retiring deletion ownership are one transaction.
pub(crate) fn complete_pool_clone(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = 'suspended',
                state_changed_at = ?1,
                updated_at = ?1,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                failure_reason = NULL
          WHERE id = ?2 AND state = 'creating'
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?3
                   AND episode.phase = 'armed'
                   AND episode.activation_state = 'creating'
                   AND episode.completion = 'delete_pool_clone'
            )",
        params![now()?, id, cleanup_id.to_string()],
    )?;
    if changed == 1 {
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

pub fn complete_create(
    conn: &Connection,
    id: &str,
    cleanup_id: SandboxCleanupEpisodeId,
    publish_activation_completion: bool,
) -> Result<bool> {
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                failure_reason = NULL,
                updated_at = ?1
          WHERE id = ?2
            AND state IN ('created', 'suspended')
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?3
                   AND episode.phase = 'armed'
            )",
        params![now()?, id, cleanup_id.to_string()],
    )?;
    if changed == 1 {
        if publish_activation_completion {
            let target_state = get_sandbox(&tx, id)?
                .map(|row| row.state)
                .unwrap_or(SandboxState::Created);
            persist_activation_completed_receipt(&tx, id, cleanup_id, target_state)?;
        }
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

pub fn mark_restore_succeeded(
    conn: &Connection,
    id: &str,
    final_state: SandboxState,
    cleanup_id: Option<SandboxCleanupEpisodeId>,
) -> Result<bool> {
    if !matches!(final_state, SandboxState::Started | SandboxState::Paused) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'restoring'
           AND (?4 IS NULL OR EXISTS (
               SELECT 1 FROM sandbox_cleanup_episodes episode
                WHERE episode.sandbox_id = sandboxes.id
                  AND episode.cleanup_id = ?4
                  AND episode.phase = 'armed'
           ))",
        params![
            final_state.as_str(),
            now()?,
            id,
            cleanup_id.map(|id| id.to_string())
        ],
    )?;
    if changed == 1
        && let Some(cleanup_id) = cleanup_id
    {
        persist_activation_completed_receipt(&tx, id, cleanup_id, final_state)?;
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    if changed == 1 && final_state == SandboxState::Started {
        arm_auto_lifecycle_after_activation(&tx, id)?;
    }
    if changed == 1 {
        crate::route_activation_claims::close_claim(&tx, id)?;
    }
    tx.commit()?;
    if changed == 1 {
        emit_sandbox_state_event(
            id,
            SandboxEventType::RestoreCompleted,
            Some(SandboxState::Restoring.as_str()),
            "restore_succeeded",
            "mark_restore_succeeded",
            final_state,
            None,
        );
    }
    Ok(changed == 1)
}

fn arm_auto_lifecycle_after_activation(conn: &Connection, id: &str) -> Result<()> {
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let mut runtime_state = sandbox.auto_lifecycle_state;
    crate::auto_lifecycle::arm_inactivity_conditions(
        &mut runtime_state,
        &sandbox.auto_lifecycle,
        crate::auto_lifecycle::now_secs(),
    );
    let runtime_state_json = serde_json::to_string(&runtime_state)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    conn.execute(
        "UPDATE sandboxes
         SET auto_lifecycle_state_json = ?1,
             next_auto_event_at = ?2
         WHERE id = ?3",
        params![
            runtime_state_json,
            runtime_state.next_auto_event_at.map(SqliteU64),
            id
        ],
    )?;
    Ok(())
}

pub fn begin_suspend(conn: &Connection, id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'suspending',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             suspend_predecessor_artifact_id = CASE
                 WHEN current_local_artifact_kind = 'full'
                  AND current_local_artifact_origin = 'suspend'
                 THEN current_local_artifact_id
                 ELSE NULL
             END,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'suspending' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2 AND state = 'paused'
           AND cleanup_completion IS NULL AND cleanup_keep_filesystem IS NULL",
        params![now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn begin_teardown_suspend(
    conn: &Connection,
    id: &str,
    execution_generation: ExecutionGeneration,
) -> Result<SupersededTeardownGeneration> {
    let tx = immediate_transaction(conn)?;
    let claim = ensure_teardown_suspend_operation(&tx, id, execution_generation)?;
    mark_teardown_suspend_operation_running(&tx, id, execution_generation)?;
    let changed = tx.execute(
        "UPDATE sandboxes
         SET state = 'suspending',
             restore_origin = NULL,
             restore_phase = NULL,
             restore_storage_uri = NULL,
             suspend_predecessor_artifact_id = CASE
                 WHEN current_local_artifact_kind = 'full'
                  AND current_local_artifact_origin = 'suspend'
                 THEN current_local_artifact_id
                 ELSE NULL
             END,
             current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL,
             current_local_artifact_origin = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'suspending' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'paused'",
        params![now()?, id],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    tx.commit()?;
    emit_teardown_suspend_lease_claim(id, execution_generation, claim);
    Ok(claim)
}

pub fn rollback_suspending_to_paused(conn: &Connection, id: &str) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = 'paused',
             current_local_artifact_id = suspend_predecessor_artifact_id,
             current_local_artifact_kind = CASE
                 WHEN suspend_predecessor_artifact_id IS NULL THEN NULL ELSE 'full'
             END,
             current_local_artifact_origin = CASE
                 WHEN suspend_predecessor_artifact_id IS NULL THEN NULL ELSE 'suspend'
             END,
             suspend_predecessor_artifact_id = NULL,
             cleanup_completion = NULL,
             cleanup_keep_filesystem = NULL,
             failure_reason = NULL,
             state_changed_at = CASE WHEN state != 'paused' THEN ?1 ELSE state_changed_at END,
             updated_at = ?1
         WHERE id = ?2
           AND state = 'suspending'",
        params![now()?, id],
    )?;
    Ok(changed == 1)
}

pub fn mark_suspending_quarantined(
    conn: &Connection,
    id: &str,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Suspending,
        SandboxQuarantineSource::SbxManager,
        SandboxQuarantineReasonCode::SuspendSnapshotPublicationFailed,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

#[cfg(test)]
pub fn mark_starting_quarantined(
    conn: &Connection,
    id: &str,
    reason_code: SandboxQuarantineReasonCode,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Starting,
        SandboxQuarantineSource::SbxManager,
        reason_code,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

#[cfg(test)]
pub fn mark_restoring_quarantined(
    conn: &Connection,
    id: &str,
    reason_code: SandboxQuarantineReasonCode,
    reason: &str,
) -> std::result::Result<bool, MarkQuarantinedError> {
    mark_quarantined_from_state(
        conn,
        id,
        SandboxState::Restoring,
        SandboxQuarantineSource::SbxManager,
        reason_code,
        reason,
        SandboxQuarantineNextAction::RequiresOperatorAttention,
    )
}

#[cfg(test)]
pub fn rollback_starting(conn: &Connection, id: &str, state: SandboxState) -> Result<bool> {
    rollback_activation_state(conn, id, SandboxState::Starting, state)
}

#[cfg(test)]
pub fn rollback_restoring(conn: &Connection, id: &str, state: SandboxState) -> Result<bool> {
    rollback_activation_state(conn, id, SandboxState::Restoring, state)
}

pub fn rollback_restoring_episode(
    conn: &Connection,
    id: &str,
    state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    rollback_activation_episode(conn, id, SandboxState::Restoring, state, cleanup_id)
}

pub fn rollback_starting_episode(
    conn: &Connection,
    id: &str,
    state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    rollback_activation_episode(conn, id, SandboxState::Starting, state, cleanup_id)
}

fn rollback_activation_episode(
    conn: &Connection,
    id: &str,
    activation_state: SandboxState,
    restored_state: SandboxState,
    cleanup_id: SandboxCleanupEpisodeId,
) -> std::result::Result<bool, RollbackActivationEpisodeError> {
    if !matches!(
        activation_state,
        SandboxState::Starting | SandboxState::Restoring
    ) {
        return Err(RollbackActivationEpisodeError::UnsupportedActivationState {
            state: activation_state,
        });
    }
    if restored_state == SandboxState::Failed {
        return Err(rusqlite::Error::InvalidQuery.into());
    }
    if restored_state == SandboxState::Quarantined {
        return Err(RollbackActivationEpisodeError::QuarantinedTarget);
    }
    let tx = immediate_transaction(conn)?;
    let changed = tx.execute(
        "UPDATE sandboxes
            SET state = ?1,
                failure_reason = NULL,
                cleanup_completion = NULL,
                cleanup_keep_filesystem = NULL,
                restore_origin = NULL,
                restore_phase = NULL,
                restore_storage_uri = NULL,
                state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
                updated_at = ?2
          WHERE id = ?3
            AND state = ?4
            AND EXISTS (
                SELECT 1 FROM sandbox_cleanup_episodes episode
                 WHERE episode.sandbox_id = sandboxes.id
                   AND episode.cleanup_id = ?5
                   AND episode.phase = 'armed'
            )",
        params![
            restored_state.as_str(),
            now()?,
            id,
            activation_state.as_str(),
            cleanup_id.to_string()
        ],
    )?;
    if changed == 1 {
        tx.execute(
            "DELETE FROM sandbox_cleanup_episodes
              WHERE sandbox_id = ?1 AND cleanup_id = ?2",
            params![id, cleanup_id.to_string()],
        )?;
    }
    tx.commit()?;
    Ok(changed == 1)
}

fn rollback_activation_state(
    conn: &Connection,
    id: &str,
    expected_state: SandboxState,
    restored_state: SandboxState,
) -> Result<bool> {
    if matches!(
        restored_state,
        SandboxState::Quarantined | SandboxState::Failed
    ) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let changed = conn.execute(
        "UPDATE sandboxes
         SET state = ?1,
             failure_reason = NULL,
             restore_origin = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_origin END,
             restore_phase = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_phase END,
             restore_storage_uri = CASE WHEN ?4 = 'restoring' THEN NULL ELSE restore_storage_uri END,
             state_changed_at = CASE WHEN state IS NOT ?1 THEN ?2 ELSE state_changed_at END,
             updated_at = ?2
         WHERE id = ?3
           AND state = ?4",
        params![restored_state.as_str(), now()?, id, expected_state.as_str()],
    )?;
    Ok(changed == 1)
}

/// Called inside activation admission's transaction, before runtime launch.
/// Snapshot policy is historical; a cluster activation carries current intent.
/// Preserve local overrides and unchanged clocks, including reconnect floors.
fn install_activation_auto_lifecycle(
    conn: &Connection,
    id: &str,
    policy: &AutoLifecyclePolicy,
) -> Result<()> {
    let sandbox = get_sandbox(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    if sandbox.auto_lifecycle_authority == PolicyAuthority::Local {
        return Ok(());
    }
    let runtime_state = if sandbox.auto_lifecycle.normalized() == *policy {
        sandbox.auto_lifecycle_state
    } else {
        AutoLifecycleRuntimeState::default()
    };
    update_auto_lifecycle_with_state(conn, id, policy, &runtime_state, PolicyAuthority::Cluster)
}

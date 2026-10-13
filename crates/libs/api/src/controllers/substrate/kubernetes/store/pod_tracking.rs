//! Pod tracking cleanup: forgetting or backfilling tracked pod identity and the
//! started-suspend recovery that runs after execution cleanup.

use super::*;

impl K8sStore {
    pub(in crate::controllers::substrate::kubernetes) async fn forget_pod_tracking(
        &self,
        sandbox_id: SandboxId,
        placement_clock: PlacementClock,
    ) -> Result<(), SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(());
        }

        self.finish_active_execution_in(&mut tx, sandbox_id, "forgotten")
            .await?;
        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        let recovered_started = self
            .recover_started_after_suspend_cleanup_in(&mut tx, sandbox_id)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        if recovered_started {
            tracing::info!(
                sandbox_id = %sandbox_id,
                ?placement_clock,
                "recovered started sandbox to suspended during pod tracking cleanup"
            );
            emit_started_suspend_recovery_event(
                sandbox_id,
                StartedSuspendRecoveryOperation::PodTrackingCleanup,
            );
        }
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn forget_pod_tracking_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        cause: PodTrackingCleanupCause,
    ) -> Result<bool, SubstrateError> {
        self.finish_execution_and_clear_pod_tracking_if_generation(
            sandbox_id,
            generation,
            None,
            "forgotten",
            StartedSuspendRecoveryOperation::TerminalPodCleanup,
            cause,
            PlacementClock::Bump,
        )
        .await
    }

    /// Clear pod tracking for a sandbox that has **no** active execution,
    /// proving that inside the same locked transaction.
    ///
    /// A caller that observed no execution has no generation to CAS on, so
    /// this is what makes its teardown safe: an execution created between the
    /// caller's observation and here makes the write claim nothing, and the
    /// caller reports the supersession instead of clearing a row it never saw.
    /// The sandbox lock is the same one every multi-row execution mutation
    /// takes, so the check and the clear cannot straddle a concurrent create.
    pub(in crate::controllers::substrate::kubernetes) async fn forget_pod_tracking_if_no_active_execution(
        &self,
        sandbox_id: SandboxId,
        placement_clock: PlacementClock,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(true);
        }
        let active_exists: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active
             )"
        ))
        .bind(sandbox_id)
        .fetch_one(&mut *tx)
        .await
        .db_context("failed to re-check execution absence")?;
        if active_exists {
            return Ok(false);
        }

        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        Ok(true)
    }

    /// Fill in a `pod_uid` that was never persisted, and nothing else.
    ///
    /// [`Self::persist_pod_metadata`] assigns rather than patches: it sets
    /// `pod_ip` to whatever it is handed and recomputes `state` from it, so
    /// calling it with a UID alone clears a live pod's IP and downgrades a
    /// `running` execution to `created`. This exists because the fence's
    /// backfill has exactly one field to contribute and must not touch a row
    /// the create path may have completed in the meantime — hence
    /// `pod_uid IS NULL`, which also makes it unable to overwrite a newer UID.
    /// The attempt guard is there for the same reason the release CAS has one:
    /// a replacement can reuse the generation with a NULL UID, and writing the
    /// old pod's identity onto it would be worse than writing nothing.
    pub(in crate::controllers::substrate::kubernetes) async fn backfill_missing_pod_uid(
        &self,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        pod_uid: &str,
    ) -> Result<bool, SubstrateError> {
        let (generation, activation_attempt_id, _, route_activation_id) =
            Self::identity_binds(identity);
        let filled = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET pod_uid = $4, updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation IS NOT DISTINCT FROM $2
                AND active
                AND activation_attempt_id IS NOT DISTINCT FROM $3
                AND NULLIF(BTRIM(pod_uid), '') IS NULL
                AND route_activation_id IS NOT DISTINCT FROM $5"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(activation_attempt_id)
        .bind(pod_uid)
        .bind(route_activation_id)
        .execute(&self.pool)
        .await
        .db_context("failed to backfill pod uid")?;
        Ok(filled.rows_affected() > 0)
    }

    /// [`Self::forget_pod_tracking`] for a pod-identity-fenced teardown:
    /// generation-guarded, so a successor execution installed while the
    /// teardown was running keeps its tracking, and returning `false` instead
    /// of silently retiring it. Takes the `placement_clock` the caller's
    /// teardown mode requires — a retire must
    /// [`Preserve`](PlacementClock::Preserve) it (see that type's docs), which
    /// is the one thing [`Self::forget_pod_tracking_if_generation`] cannot
    /// express.
    pub(in crate::controllers::substrate::kubernetes) async fn forget_fenced_pod_tracking(
        &self,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        placement_clock: PlacementClock,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }
        if !self
            .finish_fenced_execution_in(&mut tx, sandbox_id, identity, "forgotten")
            .await?
        {
            return Ok(false);
        }
        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        let recovered_started = self
            .recover_started_after_suspend_cleanup_in(&mut tx, sandbox_id)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        if recovered_started {
            emit_started_suspend_recovery_event(
                sandbox_id,
                StartedSuspendRecoveryOperation::PodTrackingCleanup,
            );
        }
        Ok(true)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn converge_failed_recovery_after_pod_absence_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_pod_uid: Option<&str>,
        expected_pod_ip: Option<&str>,
        expected_k8s_node_name: Option<&str>,
    ) -> Result<FailedRecoveryPodAbsenceOutcome, SubstrateError> {
        let generation =
            ExecutionGeneration::new(generation).map_err(|error| SubstrateError::Failed {
                message: format!("invalid failed recovery generation: {error}"),
            })?;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start failed recovery pod-absence transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(FailedRecoveryPodAbsenceOutcome::NotMatched);
        }

        let cleared = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_uid = NULL,
                 pod_ip = NULL,
                 k8s_node_name = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND generation = $2
               AND active
               AND state = $3
               AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $4
               AND pod_ip IS NOT DISTINCT FROM $5::INET
               AND k8s_node_name IS NOT DISTINCT FROM $6"
        ))
        .bind(sandbox_id)
        .bind(generation.get())
        .bind(ActiveExecutionState::RecoveryFailed.as_str())
        .bind(expected_pod_uid)
        .bind(expected_pod_ip)
        .bind(expected_k8s_node_name)
        .execute(&mut *tx)
        .await
        .db_context("failed to clear missing recovery pod metadata")?
        .rows_affected()
            > 0;
        if !cleared {
            tx.commit()
                .await
                .db_context("failed to commit unmatched pod-absence transaction")?;
            return Ok(FailedRecoveryPodAbsenceOutcome::NotMatched);
        }

        let recovered = self
            .recover_started_after_failed_recovery_in(&mut tx, sandbox_id, generation)
            .await?;
        if recovered
            && !self
                .finish_active_execution_generation_in(
                    &mut tx,
                    sandbox_id,
                    generation.get(),
                    ActiveExecutionState::RecoveryFailed.as_str(),
                )
                .await?
        {
            return Err(SubstrateError::Failed {
                message: "failed to retire pod-absent converged recovery execution".to_string(),
            });
        }

        tx.commit()
            .await
            .db_context("failed to commit failed recovery pod absence")?;
        if recovered {
            emit_started_suspend_recovery_event(
                sandbox_id,
                StartedSuspendRecoveryOperation::StartedRecoveryFailure,
            );
            Ok(FailedRecoveryPodAbsenceOutcome::ConvergedSuspended)
        } else {
            Ok(FailedRecoveryPodAbsenceOutcome::ClaimPreserved)
        }
    }

    pub(in crate::controllers::substrate::kubernetes) async fn forget_pod_tracking_if_generation_and_attempt(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<bool, SubstrateError> {
        self.finish_execution_and_clear_pod_tracking_if_generation(
            sandbox_id,
            generation,
            Some(activation_attempt_id),
            "forgotten",
            StartedSuspendRecoveryOperation::TerminalPodCleanup,
            PodTrackingCleanupCause::ExplicitTeardown,
            PlacementClock::Bump,
        )
        .await
    }

    pub(in crate::controllers::substrate::kubernetes) async fn cleanup_replaced_pod_tracking_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<bool, SubstrateError> {
        self.finish_execution_and_clear_pod_tracking_if_generation(
            sandbox_id,
            generation,
            None,
            "replaced",
            StartedSuspendRecoveryOperation::StalePodReplacementCleanup,
            PodTrackingCleanupCause::ExplicitTeardown,
            PlacementClock::Bump,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_execution_and_clear_pod_tracking_if_generation(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: Option<pplx_space_common_core::types::SandboxActivationAttemptId>,
        state: &str,
        recovery_operation: StartedSuspendRecoveryOperation,
        cause: PodTrackingCleanupCause,
        placement_clock: PlacementClock,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod tracking cleanup transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }

        if cause == PodTrackingCleanupCause::MissingPodObservation
            && state == "forgotten"
            && Self::suspend_owns_pod_teardown_in(&mut tx, sandbox_id).await?
        {
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation,
                "deferring generation-scoped pod tracking cleanup: suspend finalization owns \
                 this pod teardown"
            );
            return Ok(false);
        }

        let finished = if let Some(activation_attempt_id) = activation_attempt_id {
            self.finish_active_execution_generation_attempt_in(
                &mut tx,
                sandbox_id,
                generation,
                activation_attempt_id,
                state,
            )
            .await?
        } else {
            self.finish_active_execution_generation_in(&mut tx, sandbox_id, generation, state)
                .await?
        };
        if !finished {
            tx.commit()
                .await
                .db_context("failed to commit pod tracking cleanup transaction")?;
            return Ok(false);
        }

        // Create-cleanup abandonment is delete-shaped (a never-succeeded pod)
        // and passes `Bump`. A fenced retire passes `Preserve` for the reason
        // [`PlacementClock`] documents.
        self.clear_pod_tracking_in(&mut tx, sandbox_id, placement_clock)
            .await?;
        self.recover_markerless_restoring_after_execution_cleanup_in(&mut tx, sandbox_id)
            .await?;
        // Recovery relies on the active execution being retired in this same
        // transaction before the started-row check runs.
        let recovered_started = self
            .recover_started_after_suspend_cleanup_in(&mut tx, sandbox_id)
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit pod tracking cleanup transaction")?;
        if recovered_started {
            tracing::info!(
                sandbox_id = %sandbox_id,
                "recovered started sandbox to suspended during generation-scoped pod tracking cleanup"
            );
            emit_started_suspend_recovery_event(sandbox_id, recovery_operation);
        }
        Ok(true)
    }

    pub(super) async fn clear_pod_tracking_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        placement_clock: PlacementClock,
    ) -> Result<(), SubstrateError> {
        // A pool-claimed sandbox leaves its `claimed` slot row behind after
        // bind. Retiring it here with the execution keeps the stale-claimed
        // sweep's working set bounded by live capacity instead of delete
        // churn. No-op for non-pooled sandboxes and for `cleanup_pending`
        // slots (the failed-boot path still owns those).
        let slot_deleted = WarmPoolStore::delete_claimed_slot_for_retired_execution(tx, sandbox_id)
            .await
            .db_context("failed to delete claimed pool slot")?;
        if slot_deleted {
            tracing::info!(
                sandbox_id = %sandbox_id,
                path = "pod_tracking_clear",
                "deleted claimed pool slot inline with pod teardown"
            );
        }

        // The placement-clock bump is conditional: a recoverability-preserving
        // retire keeps `placement_changed_at` so the legacy snapshot-freshness
        // gate doesn't strand a markerless row's ready snapshot. See
        // [`PlacementClock`].
        let query = match placement_clock {
            PlacementClock::Bump => sqlx::query(sql!(
                "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL,
                     placement_changed_at = NOW()
                 WHERE id = $1"
            )),
            PlacementClock::Preserve => sqlx::query(sql!(
                "UPDATE sandboxes
                 SET placement = 'unplaced',
                     active_node_id = NULL,
                     target_node_id = NULL
                 WHERE id = $1"
            )),
        };
        query
            .bind(sandbox_id)
            .execute(&mut **tx)
            .await
            .db_context("failed to mark sandbox unplaced")?;
        Ok(())
    }

    pub(super) async fn recover_markerless_restoring_after_execution_cleanup_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<(), SubstrateError> {
        // Callers hold the sandbox row lock and retire the authoritative
        // execution earlier in this transaction. Execution creation takes the
        // same lock, so a successor cannot appear between this proof and commit.
        let safe_revert_snapshot = restore_retry_suspended_revert_snapshot_exists("s");
        let query = format!(
            "UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    current_local_artifact_id = NULL,
                    current_local_artifact_kind = NULL,
                    current_local_artifact_origin = NULL,
                    command_ready_pending = FALSE,
                    restore_lease_expires_at = NULL,
                    restore_lease_token = NULL
              WHERE s.id = $1
                AND s.lifecycle = 'restoring'
                AND s.evacuation_restore_target IS NULL
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                )
                AND {safe_revert_snapshot}"
        );
        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .execute(&mut **tx)
            .await
            .db_context("failed to recover markerless restoring sandbox after execution cleanup")?;
        Ok(())
    }

    pub(super) async fn recover_started_after_suspend_cleanup_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        self.recover_started_with_current_suspend_snapshot_in(
            tx,
            sandbox_id,
            StartedSuspendRecoveryProof::ExecutionRetired,
        )
        .await
    }

    pub(super) async fn recover_started_after_failed_recovery_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> Result<bool, SubstrateError> {
        self.recover_started_with_current_suspend_snapshot_in(
            tx,
            sandbox_id,
            StartedSuspendRecoveryProof::FailedRecovery { generation },
        )
        .await
    }

    async fn recover_started_with_current_suspend_snapshot_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        proof: StartedSuspendRecoveryProof,
    ) -> Result<bool, SubstrateError> {
        let (generation, execution_proof) = match proof {
            StartedSuspendRecoveryProof::ExecutionRetired => (
                None,
                "NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.active
                 )
                 AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.pod_ip IS NOT NULL
                 )",
            ),
            StartedSuspendRecoveryProof::FailedRecovery { generation } => (
                Some(generation.get()),
                "EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.generation = args.generation
                       AND e.active
                       AND e.state = 'recovery_failed'
                       AND e.pod_uid IS NULL
                       AND e.pod_ip IS NULL
                       AND e.k8s_node_name IS NULL
                 )
                 AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions newer
                     WHERE newer.sandbox_id = s.id
                       AND newer.active
                       AND newer.generation > args.generation
                 )",
            ),
        };
        let eligibility = match proof {
            StartedSuspendRecoveryProof::ExecutionRetired => {
                started_suspend_revert_eligibility("s")
            }
            StartedSuspendRecoveryProof::FailedRecovery { .. } => {
                failed_repair_revert_eligibility("s")
            }
        };
        let clear_marker = eligibility.clear_marker;
        let eligible = eligibility.eligible;
        let artifact_marker_assignments =
            clear_current_local_artifact_marker_assignments("s", "pre.clear_marker");
        let query = format!(
            "WITH args AS (
                 SELECT $2::BIGINT AS generation
             ),
             pre AS (
                 SELECT id,
                        {clear_marker} AS clear_marker
                   FROM sandboxes s
                  WHERE s.id = $1
             )
             UPDATE sandboxes s
                SET lifecycle = 'suspended',
                    placement = 'unplaced',
                    active_node_id = NULL,
                    target_node_id = NULL,
                    placement_changed_at = NULL,
                    command_ready_pending = FALSE,
                    {artifact_marker_assignments}
               FROM pre, args
              WHERE s.id = pre.id
                AND s.lifecycle = 'started'
                AND s.placement = 'unplaced'
                AND s.active_node_id IS NULL
                AND {execution_proof}
                AND {eligible}"
        );
        let result = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(generation)
            .execute(&mut **tx)
            .await
            .db_context("failed to recover started sandbox with current suspend snapshot")?;
        Ok(result.rows_affected() > 0)
    }
}

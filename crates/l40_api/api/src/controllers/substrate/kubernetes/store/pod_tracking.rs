//! Pod tracking cleanup: forgetting or backfilling tracked pod identity and the
//! started-suspend recovery that runs after execution cleanup.

use super::*;
use pplx_space_accounts::NodeId;
use pplx_space_sandbox_store::k8s_executions::pod_tracking as k8s_execution_pod_tracking;
use pplx_space_sandbox_store::pod_tracking;

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

    /// Retire the execution a `stopped` sandbox's stop left active, refusing if
    /// any active execution postdates that stop.
    ///
    /// On Kubernetes a stop leaves its execution row `active`: the stop sends
    /// the in-VM stop and rewrites the lifecycle, and nothing retires the row
    /// until the next `start` (`execution_predates_current_stop` is how that
    /// start recognizes it). So "no active execution" is not the shape
    /// of an ordinary stopped sandbox, and a teardown that requires it never
    /// runs. What the teardown has to exclude is narrower: an execution a
    /// `start` created after the stop, whose pod may be running the user's
    /// writes. That one usually reads as created after `lifecycle_changed_at`,
    /// or the row is no longer `stopped`. One that doesn't (a start that
    /// reused the pre-stop execution, or whose insert's transaction began
    /// before the stop committed) holds a live activation attempt, assigned
    /// in the same transaction, and that refuses instead (below).
    ///
    /// The stopped-evacuation marker is required too, and it's what refuses a
    /// whole start→stop cycle that lands between the drain's last read and
    /// here. That cycle leaves the row `stopped` again with an execution that
    /// predates the *new* stop, holding writes the published artifact doesn't
    /// have, so the stop boundary alone would retire it. But the start moved
    /// the lifecycle, and `sandboxes_clear_evacuation_restore_target` clears
    /// the marker on any lifecycle change to a value other than `suspending`,
    /// `suspended` or `restoring`, so the marker is gone and the teardown
    /// refuses.
    ///
    /// A live activation attempt on the active execution refuses too. A
    /// `start` doesn't always create an execution: one that finds no pod under
    /// the pre-stop execution's name reuses that execution and creates a pod
    /// under the same name, so the stop boundary still reads it as pre-stop
    /// and the row stays `stopped` with its marker until the start succeeds.
    /// What such a start does hold, from its claim until it finishes, is an
    /// activation attempt (`ensure_active_execution_claim_in`) whose lease
    /// outlives any node operation. An attempt with no expiry refuses as well,
    /// so a row this can't read is left alone; an expired one, left by a
    /// crashed owner, doesn't.
    ///
    /// The check and the retirement share one transaction under the
    /// execution-mutation lock execution creation also takes, so a start can't
    /// slip a new execution in between. `Ok(false)` changes nothing.
    pub(in crate::controllers::substrate::kubernetes) async fn retire_executions_predating_current_stop(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start stopped execution retirement transaction")?;
        // No row to lock is no marker either, so it refuses like one whose
        // marker is gone.
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }
        let refuse = pod_tracking::stopped_execution_retirement_refused(&mut *tx, sandbox_id)
            .await
            .db_context("failed to check for an execution newer than the stop")?;
        if refuse {
            return Ok(false);
        }
        self.finish_active_execution_in(&mut tx, sandbox_id, SandboxLifecycle::Stopped.as_str())
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit stopped execution retirement")?;
        Ok(true)
    }

    /// Release `source_node_id`, the node of a `stopped` sandbox whose stopped
    /// evacuation the drain has torn down, under the sandbox row lock
    /// execution creation also takes. `Ok(false)`, changing nothing, unless
    /// the row is still `stopped`, carries the evacuation marker and a durable
    /// pinned disk artifact, has no active execution, and is still placed on
    /// `source_node_id` or already released; see
    /// [`pod_tracking::release_published_stopped_evacuation`].
    pub(in crate::controllers::substrate::kubernetes) async fn release_stopped_evacuation_placement(
        &self,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start stopped evacuation release transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }
        let released = pod_tracking::release_published_stopped_evacuation(
            &mut *tx,
            sandbox_id,
            source_node_id,
        )
        .await
        .db_context("failed to release stopped evacuation placement")?;
        tx.commit()
            .await
            .db_context("failed to commit stopped evacuation release")?;
        Ok(released)
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
        let active_exists = pplx_space_sandbox_store::k8s_executions::exists(&mut *tx, sandbox_id)
            .await
            .map_err(sqlx::Error::from)
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
    ///
    /// A filled UID is a pod the row didn't track before, so the sandbox's
    /// record is set to `pod_client`, the pod's `space.pplx.net/client-id`
    /// label, in the same transaction (see `runtime_attribution`).
    pub(in crate::controllers::substrate::kubernetes) async fn backfill_missing_pod_uid(
        &self,
        sandbox_id: SandboxId,
        identity: &ExecutionIdentity,
        pod_uid: &str,
        pod_client: Option<&pplx_space_accounts::ClientId>,
    ) -> Result<bool, SubstrateError> {
        let (generation, activation_attempt_id, _, route_activation_id) =
            Self::identity_binds(identity);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod uid backfill transaction")?;
        // `sandboxes` before the execution row, the order multi-row
        // mutations take, since a fill also writes the record. With
        // attribution off the fill writes only the execution row and takes
        // no `sandboxes` lock.
        if self.attribution.is_enabled()
            && !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await?
        {
            return Ok(false);
        }
        let filled = k8s_execution_pod_tracking::backfill_missing_pod_uid(
            &mut *tx,
            sandbox_id,
            generation,
            activation_attempt_id,
            pod_uid,
            route_activation_id,
        )
        .await
        .db_context("failed to backfill pod uid")?;
        if filled
            && self.attribution.is_enabled()
            && let Some(generation) = generation
            && let Some(tracked) = self.lock_tracked_pod_in(&mut tx, sandbox_id, generation).await?
            && let Some(sandbox) = tracked.sandbox.as_ref()
            // The row tracked no pod before this fill.
            && let Some(record) = super::attribution_update(
                &sandbox.lifecycle,
                None,
                Some(pod_uid),
                pod_client,
                sandbox.record.as_ref(),
            )
        {
            Self::attribute_runtime_in(&mut tx, sandbox_id, record.as_ref()).await?;
        }
        tx.commit()
            .await
            .db_context("failed to commit pod uid backfill")?;
        Ok(filled)
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

        // Clearing the pod UID stamps `pod_materialized_at` first if the row
        // held one, so the record that a pod ran outlives the UID.
        let cleared = k8s_execution_pod_tracking::clear_absent_recovery_pod(
            &mut *tx,
            sandbox_id,
            generation,
            ActiveExecutionState::RecoveryFailed.as_str(),
            expected_pod_uid,
            expected_pod_ip,
            expected_k8s_node_name,
        )
        .await
        .db_context("failed to clear missing recovery pod metadata")?;
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
        activation_attempt_id: pplx_space_sandbox_model::SandboxActivationAttemptId,
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

    /// Whether the execution that names `pod_name` at `generation` is already
    /// retired (`active = FALSE`). A retired execution generation is final:
    /// every activation assignment requires `active`, generations are
    /// per-sandbox monotonic, and sandbox-manager refuses to start a retired
    /// generation's runtime. So a pod whose execution row reads retired
    /// belongs to no current or future runtime and is safe to delete under a
    /// UID precondition. Returns `false` when no such row exists, so a pod the
    /// store has no record of is never treated as retired.
    pub(in crate::controllers::substrate::kubernetes) async fn execution_generation_retired(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
    ) -> Result<bool, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::generation_retired(
            &self.pool, sandbox_id, generation, pod_name,
        )
        .await
        .db_context("failed to read execution retirement")
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
        activation_attempt_id: Option<pplx_space_sandbox_model::SandboxActivationAttemptId>,
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
        let slot_deleted =
            WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut **tx, sandbox_id)
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
        let unplaced = match placement_clock {
            PlacementClock::Bump => {
                pod_tracking::unplace_after_pod_teardown(&mut **tx, sandbox_id).await
            }
            PlacementClock::Preserve => {
                pod_tracking::unplace_after_pod_teardown_preserving_placement_clock(
                    &mut **tx, sandbox_id,
                )
                .await
            }
        }
        .db_context("failed to mark sandbox unplaced")?;
        // The unplace refuses a `stopped` row, whose disk is on the node it
        // names; see `pod_tracking::unplace_after_pod_teardown`. Every caller
        // holds the row lock, so the row exists and that refusal is the only
        // way here. The drain's stopped evacuation releases the node itself,
        // with `release_stopped_evacuation_placement`.
        if !unplaced {
            tracing::debug!(
                sandbox_id = %sandbox_id,
                "kept the placement of a stopped sandbox after its pod teardown"
            );
        }
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
        pod_tracking::recover_markerless_restoring_after_execution_cleanup(&mut **tx, sandbox_id)
            .await
            .db_context("failed to recover markerless restoring sandbox after execution cleanup")
    }

    pub(super) async fn recover_started_after_suspend_cleanup_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        pod_tracking::recover_started_after_execution_retired(&mut **tx, sandbox_id)
            .await
            .db_context("failed to recover started sandbox with current suspend snapshot")
    }

    pub(super) async fn recover_started_after_failed_recovery_in(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> Result<bool, SubstrateError> {
        pod_tracking::recover_started_after_failed_recovery(&mut **tx, sandbox_id, generation)
            .await
            .db_context("failed to recover started sandbox with current suspend snapshot")
    }
}

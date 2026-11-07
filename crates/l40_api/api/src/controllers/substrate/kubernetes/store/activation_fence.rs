//! Activation attempt fence: assigning, completing, releasing, and abandoning
//! activation attempts, route identity publication, and fence reconciliation.

use super::*;
use pplx_space_sandbox_store::restore_source;

impl K8sStore {
    pub(in crate::controllers::substrate::kubernetes) async fn begin_activation_execution_row(
        &self,
        sandbox_id: SandboxId,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
    ) -> Result<SandboxExecutionRow, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start activation execution transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await? {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }
        let claim = self
            .ensure_active_execution_claim_in(
                &mut tx,
                sandbox_id,
                Some((activation_attempt_id, target_state)),
                None,
                None,
            )
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit activation execution transaction")?;
        record_route_identity_decision(&claim, false);
        Ok(claim.row)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn assign_activation_attempt(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
    ) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, SubstrateError> {
        self.assign_activation_attempt_guarded(
            sandbox_id,
            generation,
            activation_attempt_id,
            target_state,
            AssignmentGuard::Any,
        )
        .await
    }

    /// Assign `activation_attempt_id` to the active execution under the
    /// sandbox's row lock, once `guard` holds under that lock.
    async fn assign_activation_attempt_guarded(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
        guard: AssignmentGuard,
    ) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start activation-attempt transaction")?;
        let not_found = || SubstrateError::NotFound {
            message: format!("sandbox not found: {sandbox_id}"),
        };
        let left_by = match guard {
            AssignmentGuard::Any => {
                if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
                    return Err(not_found());
                }
                None
            }
            AssignmentGuard::StoppedLeftBy(left_by) => {
                match Self::lock_sandbox_execution_lifecycle_in(&mut tx, sandbox_id).await? {
                    // A drain's stopped evacuation took the execution over
                    // when it opened, and its teardown retires it.
                    Some(SandboxLifecycle::Stopped)
                        if Self::stopped_evacuation_marked_in(&mut tx, sandbox_id).await? =>
                    {
                        return Err(evacuation_refusal(sandbox_id));
                    }
                    Some(SandboxLifecycle::Stopped) => Some(left_by),
                    // Another activation took the execution: retry once it's
                    // done, as for a live claim.
                    Some(lifecycle) => {
                        return Err(SubstrateError::Unavailable {
                            message: format!(
                                "sandbox {sandbox_id} is {lifecycle}, so its runtime upgrade's \
                                 execution belongs to another activation; retry"
                            ),
                        });
                    }
                    None => return Err(not_found()),
                }
            }
        };
        let row = match (
            self.active_execution_row(&mut tx, sandbox_id).await?,
            left_by,
        ) {
            (Some(row), None) => row,
            // The generation is checked here too: a replacement can carry the
            // earlier attempt to a new generation, and the assignment below,
            // keyed by generation, would then find no row.
            (Some(row), Some(left_by))
                if row.activation_attempt_id == Some(left_by) && row.generation == generation =>
            {
                row
            }
            (_, Some(_)) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "another activation took stopped sandbox {sandbox_id}'s runtime upgrade \
                         execution; retry"
                    ),
                });
            }
            (None, None) => {
                return Err(SubstrateError::Conflict {
                    message: "execution changed before activation attempt assignment".to_string(),
                });
            }
        };
        let route_publication = classify_activation_route_publication(&row, false)?;
        // Neither caller (a pooled sandbox's activation, a stopped sandbox's
        // runtime upgrade taking its own execution over) clears an upgrade's
        // record: a pooled sandbox's execution never carries one, and the
        // upgrade's own claim keeps it.
        let activation_attempt_id = Self::assign_activation_attempt_in(
            &mut tx,
            sandbox_id,
            generation,
            activation_attempt_id,
            target_state,
            route_publication,
            None,
        )
        .await?;
        tx.commit()
            .await
            .db_context("failed to commit activation-attempt transaction")?;
        match route_publication {
            ActivationRoutePublication::InitializeFirst => {
                crate::metrics::sandbox::api::route_identity_decision(
                    RouteIdentityDecisionReason::FirstIdentityInitialized,
                );
            }
            ActivationRoutePublication::AdoptLegacyResident => {
                crate::metrics::sandbox::api::route_identity_decision(
                    RouteIdentityDecisionReason::ExistingExecutionTransition,
                );
            }
            ActivationRoutePublication::PreserveExisting => {}
        }
        Ok(activation_attempt_id)
    }

    /// Publish the route selected by a legacy resident Wake only after the
    /// manager returned the exact typed acknowledgement. This upgrades rows
    /// created before `route_activation_id` existed without recreating the
    /// unsafe publish-before-adopt ordering.
    pub(in crate::controllers::substrate::kubernetes) async fn publish_legacy_route_identity_after_ack(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
        expected_pod_uid: &str,
    ) -> Result<(), SubstrateError> {
        let published = sqlx::query_scalar::<_, Uuid>(sql!(
            "UPDATE sandbox_k8s_executions
                SET route_activation_id = $3,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND activation_attempt_id = $3
                AND route_activation_id IS NULL
                AND pod_uid = $4
              RETURNING route_activation_id"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .bind(expected_pod_uid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("failed to publish acknowledged legacy route identity: {error}"),
        })?;
        if published == Some(activation_attempt_id.as_uuid()) {
            return Ok(());
        }
        let current = sqlx::query_scalar::<_, Option<Uuid>>(sql!(
            "SELECT route_activation_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND pod_uid = $3"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(expected_pod_uid)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("failed to verify acknowledged legacy route identity: {error}"),
        })?;
        if current.flatten() == Some(activation_attempt_id.as_uuid()) {
            Ok(())
        } else {
            Err(SubstrateError::Conflict {
                message: "execution changed before acknowledged legacy route publication"
                    .to_string(),
            })
        }
    }

    /// Use one database clock to distinguish the execution retired by STOP
    /// from a replacement that booted before its START published completion.
    pub(in crate::controllers::substrate::kubernetes) async fn execution_predates_current_stop(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                SELECT 1 FROM sandbox_k8s_executions execution
                JOIN sandboxes sandbox ON sandbox.id = execution.sandbox_id
                WHERE execution.sandbox_id = $1 AND execution.generation = $2
                  AND execution.active AND sandbox.lifecycle = 'stopped'
                  AND execution.created_at <= sandbox.lifecycle_changed_at
            )"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_one(&self.pool)
        .await
        .db_context("failed to identify execution retired by stop")
    }

    pub(in crate::controllers::substrate::kubernetes) async fn complete_activation_attempt(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<bool, SubstrateError> {
        let changed = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
               FROM sandboxes sandbox
              WHERE sandbox.id = execution.sandbox_id
                AND execution.sandbox_id = $1
                AND execution.generation = $2
                AND execution.active
                AND execution.activation_attempt_id = $3
                AND execution.route_activation_id IS NOT NULL
                AND LOWER(TRIM(sandbox.lifecycle)) = execution.activation_attempt_target_state"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&self.pool)
        .await
        .db_context("failed to complete activation attempt")?;
        Ok(changed.rows_affected() == 1)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn release_activation_attempt_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) {
        if let Err(error) = self
            .complete_activation_attempt(sandbox_id, generation, activation_attempt_id)
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %activation_attempt_id,
                %error,
                "deferred activation-attempt release to completion receipt or lease expiry"
            );
        }
    }

    /// Release ownership when this caller proves either that no Wake was
    /// dispatched, that its activation reached command readiness, or, for a
    /// stopped re-placement, that the node confirmed the stopped row it
    /// materialized (nothing is activated, so no receipt will). A completed
    /// activation may already have been stopped again; its exact lease can be
    /// released without changing the newer lifecycle or published route identity.
    /// Potentially dispatched, unfinished work must retain its lease.
    pub(in crate::controllers::substrate::kubernetes) async fn release_finished_activation_attempt_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
    ) {
        if let Err(error) = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
              WHERE sandbox_id = $1 AND generation = $2 AND active
                AND activation_attempt_id = $3"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&self.pool)
        .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %activation_attempt_id,
                %error,
                "failed to release finished activation; lease expiry will allow recovery"
            );
        }
    }

    /// Give up an activation attempt whose Wake was never dispatched, after
    /// the activation failed.
    ///
    /// Clears the attempt like [`Self::release_finished_activation_attempt_best_effort`],
    /// so the next activation claims the execution at once rather than
    /// joining work nobody is doing, but keeps `activation_attempt_expires_at`
    /// as the time ownership ended: now, or the lease's own expiry if that
    /// already passed. The row may still be in the lifecycle its owner was
    /// moving it out of, say a start that adopted a crashed create's
    /// `creating` row and failed. Nothing else moves such a row on, and the
    /// transient-lifecycle sweeper reads that clock to tell an owner that
    /// gave up from one that may still be running; with the expiry cleared
    /// too, it would leave such a row alone for good while its VM is alive.
    /// The kept expiry stays until the next attempt overwrites it; nothing
    /// but the sweeper reads it without an attempt.
    pub(in crate::controllers::substrate::kubernetes) async fn give_up_activation_attempt_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
    ) {
        self.end_activation_attempt_best_effort(
            sql!(
                "UPDATE sandbox_k8s_executions
                    SET activation_attempt_id = NULL,
                        activation_attempt_expires_at = LEAST(activation_attempt_expires_at, NOW()),
                        activation_attempt_target_state = NULL
                  WHERE sandbox_id = $1 AND generation = $2 AND active
                    AND activation_attempt_id = $3"
            ),
            "give up",
            sandbox_id,
            generation,
            activation_attempt_id,
        )
        .await;
    }

    /// Run `statement`, which ends the lease of `activation_attempt_id` on
    /// the active execution of `generation` (bound as `$1` sandbox, `$2`
    /// generation, `$3` attempt), and only warn when it fails: the lease's
    /// expiry recovers the execution either way.
    async fn end_activation_attempt_best_effort(
        &self,
        statement: &'static str,
        action: &'static str,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
    ) {
        if let Err(error) = sqlx::query(statement)
            .bind(sandbox_id.as_uuid())
            .bind(generation)
            .bind(activation_attempt_id)
            .execute(&self.pool)
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %activation_attempt_id,
                action,
                %error,
                "failed to end unfinished activation; lease expiry will allow recovery"
            );
        }
    }

    /// Take over the execution an earlier runtime upgrade of the stopped
    /// sandbox left, with `activation_attempt_id` targeting `stopped`. The
    /// lifecycle and the earlier attempt, `left_by`, are checked under the
    /// sandbox's row lock with the assignment, so a start that moved the
    /// sandbox or claimed the execution meanwhile keeps it.
    pub(in crate::controllers::substrate::kubernetes) async fn take_over_stopped_runtime_upgrade(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        left_by: SandboxActivationAttemptId,
        activation_attempt_id: SandboxActivationAttemptId,
    ) -> Result<SandboxActivationAttemptId, SubstrateError> {
        self.assign_activation_attempt_guarded(
            sandbox_id,
            generation,
            activation_attempt_id,
            SandboxTransitionTargetState::Stopped,
            AssignmentGuard::StoppedLeftBy(left_by),
        )
        .await
    }

    /// End an unfinished stopped runtime upgrade's lease now. An attempt that
    /// recorded its upgrade keeps its id and target: unlike
    /// [`Self::give_up_activation_attempt_best_effort`], the claim still says
    /// what it was for, which is how a retry finds the execution
    /// ([`Self::stopped_runtime_upgrade_claim`]). One that recorded nothing
    /// placed no pod with the upgrade, so it gives the claim up instead, as
    /// that method does: a claim with nothing recorded isn't the upgrade's to
    /// take over. Either way any activation may take the execution at once.
    pub(in crate::controllers::substrate::kubernetes) async fn expire_activation_attempt_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
    ) {
        self.end_activation_attempt_best_effort(
            sql!(
                "UPDATE sandbox_k8s_executions
                    SET activation_attempt_expires_at = LEAST(activation_attempt_expires_at, NOW()),
                        activation_attempt_id = CASE
                            WHEN runtime_upgrade_operation_id IS NULL THEN NULL
                            ELSE activation_attempt_id
                        END,
                        activation_attempt_target_state = CASE
                            WHEN runtime_upgrade_operation_id IS NULL THEN NULL
                            ELSE activation_attempt_target_state
                        END,
                        updated_at = NOW()
                  WHERE sandbox_id = $1 AND generation = $2 AND active
                    AND activation_attempt_id = $3"
            ),
            "expire",
            sandbox_id,
            generation,
            activation_attempt_id,
        )
        .await;
    }

    /// The active execution claimed by an attempt targeting `stopped`: a
    /// runtime upgrade of the stopped sandbox `sandbox_id`, or a drain's
    /// re-placement, which claims it on a `restoring` row and may leave the
    /// claim behind once the row is `stopped` again. Only a claim with a
    /// recorded upgrade is the upgrade's: every other activation's claim
    /// clears the record (`assign_activation_attempt_in`).
    pub(in crate::controllers::substrate::kubernetes) async fn stopped_runtime_upgrade_claim(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<StoppedRuntimeUpgradeClaim>, SubstrateError> {
        type ClaimRow = (
            i64,
            SandboxActivationAttemptId,
            String,
            Option<String>,
            bool,
            Option<Uuid>,
            Option<String>,
            Option<String>,
        );
        let row: Option<ClaimRow> = sqlx::query_as(sql!(
            "SELECT execution.generation, execution.activation_attempt_id, execution.pod_name,
                    execution.k8s_node_name,
                    COALESCE(execution.activation_attempt_expires_at > NOW(), TRUE),
                    execution.runtime_upgrade_operation_id,
                    execution.runtime_upgrade_target_environment_ref,
                    execution.runtime_upgrade_authorization_id
               FROM sandbox_k8s_executions execution
              WHERE execution.sandbox_id = $1
                AND execution.active
                AND execution.activation_attempt_id IS NOT NULL
                AND execution.activation_attempt_target_state = $2"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(SandboxTransitionTargetState::Stopped.as_str())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to read a stopped sandbox's runtime upgrade claim")?;
        row.map(
            |(
                generation,
                activation_attempt_id,
                pod_name,
                node_name,
                live,
                operation_id,
                target,
                authorization_id,
            )| {
                let upgrade = match (operation_id, target, authorization_id) {
                    (Some(operation_id), Some(target), Some(authorization_id)) => {
                        Some(RecordedRuntimeUpgrade {
                            operation_id,
                            target: target.parse().map_err(|error| SubstrateError::Failed {
                                message: format!(
                                    "execution records an invalid runtime upgrade target {target}: \
                                     {error}"
                                ),
                            })?,
                            authorization_id,
                        })
                    }
                    _ => None,
                };
                Ok(StoppedRuntimeUpgradeClaim {
                    generation,
                    activation_attempt_id,
                    pod_name,
                    node_name,
                    live,
                    upgrade,
                })
            },
        )
        .transpose()
    }

    /// Record on the execution `activation_attempt_id` just claimed, in the
    /// claim's own transaction, the upgrade its pod is about to carry, so a
    /// retry that finds the execution places exactly that upgrade again. In
    /// the claim's transaction, no claim targeting `stopped` commits without
    /// its record: one that did, left by an attempt that ended in between,
    /// would read as a drain's claim to every retry
    /// ([`Self::stopped_runtime_upgrade_claim`]).
    ///
    /// Refused unless the sandbox is still stopped and the attempt still owns
    /// the execution: a drain's re-placement moves the row to `restoring`
    /// before it claims, so it either sees this record and refuses to join
    /// the attempt, or this record fails. That needs the sandbox row locked
    /// (taken again here, a no-op when the claim already holds it): under READ
    /// COMMITTED a plain read of the lifecycle sees the statement's snapshot,
    /// so a drain committing `restoring` after it could claim the execution,
    /// join this attempt and miss the record. Holding the lock until the claim
    /// commits makes the drain's lifecycle write wait for it. A refusal is a
    /// retryable error, which rolls the claim back with the record.
    ///
    /// Under the same lock it refuses retryably while a drain's stopped
    /// evacuation has claimed the sandbox (its marker is set): the
    /// evacuation's teardown releases the node only while the node still pins
    /// the artifact the evacuation minted, and an upgrade boot drops that
    /// pin. The evacuation claims under this lock too
    /// (`open_stopped_evacuation`), and waits while an upgrade's lease runs,
    /// so one of the two waits for the other. The marker outlasts an
    /// `:undrain` until the sandbox's lifecycle next changes, and the
    /// upgrade waits that long too: a later forced drain resumes the claim
    /// on the artifact it minted.
    ///
    /// Under the same lock it checks that the disk is still where the
    /// operation checked it before claiming: `record.disk_node` is still the
    /// node [`Self::restore_source_plan`] names and the registered node the
    /// placement pins the disk to (`restore_source::disk_pin_node_name`), and
    /// no stopped evacuation has left the sandbox unplaced. A drain that released or re-placed the
    /// sandbox in between leaves the row `stopped`, so the lifecycle alone
    /// doesn't see it, and a pod pinned to the old node would boot a released
    /// or stale copy. That refusal is retryable too: a retry checks the disk
    /// node again.
    ///
    /// The record never replaces another operation's: an execution whose
    /// claim expired may still carry the record of an upgrade that owes its
    /// disk backup, and overwriting it would lose that debt. The claim is
    /// refused retryably instead, and the retry takes that upgrade over.
    pub(super) async fn record_stopped_runtime_upgrade_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: SandboxActivationAttemptId,
        record: &StoppedUpgradeRecord,
    ) -> Result<(), SubstrateError> {
        let upgrade = &record.upgrade;
        let changed_hands = || SubstrateError::Unavailable {
            message: format!(
                "stopped sandbox {sandbox_id}'s execution changed hands before its runtime \
                 upgrade was recorded; retry"
            ),
        };
        if Self::lock_sandbox_execution_lifecycle_in(&mut *conn, sandbox_id).await?
            != Some(SandboxLifecycle::Stopped)
        {
            return Err(changed_hands());
        }
        if Self::stopped_evacuation_marked_in(&mut *conn, sandbox_id).await? {
            return Err(evacuation_refusal(sandbox_id));
        }
        let source = Self::restore_source_plan_in(&mut *conn, sandbox_id).await?;
        let pin = restore_source::disk_pin_node_name(&mut *conn, sandbox_id)
            .await
            .db_context("failed to read a stopped sandbox's disk pin")?;
        if source.stopped_evacuation_unplaced
            || source.node_name.as_deref() != Some(record.disk_node.as_str())
            || pin.as_deref() != Some(record.disk_node.as_str())
        {
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id}'s disk moved off node {} before its runtime \
                     upgrade was claimed; retry",
                    record.disk_node
                ),
            });
        }
        let recorded = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET runtime_upgrade_operation_id = $4,
                    runtime_upgrade_target_environment_ref = $5,
                    runtime_upgrade_authorization_id = $6,
                    updated_at = NOW()
              WHERE sandbox_id = $1 AND generation = $2 AND active
                AND activation_attempt_id = $3
                -- Another operation's record may still owe its disk backup:
                -- never overwrite it. Its own retry takes it over instead.
                AND (runtime_upgrade_operation_id IS NULL
                     OR runtime_upgrade_operation_id = $4)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .bind(upgrade.operation_id)
        .bind(upgrade.target.to_string())
        .bind(&upgrade.authorization_id)
        .execute(&mut *conn)
        .await
        .db_context("failed to record a stopped sandbox's runtime upgrade")?
        .rows_affected();
        if recorded != 1 {
            return Err(changed_hands());
        }
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn abandon_activation_attempt_superseded_by_suspend(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) -> Result<bool, SubstrateError> {
        let changed = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions execution
                SET activation_attempt_id = NULL,
                    activation_attempt_expires_at = NULL,
                    activation_attempt_target_state = NULL
               FROM sandboxes sandbox
              WHERE sandbox.id = execution.sandbox_id
                AND execution.sandbox_id = $1
                AND execution.generation = $2
                AND execution.active
                AND execution.activation_attempt_id = $3
                AND LOWER(TRIM(sandbox.lifecycle)) IN ('suspended', 'restoring')"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(activation_attempt_id)
        .execute(&self.pool)
        .await
        .db_context("failed to abandon suspend-superseded activation attempt")?;
        Ok(changed.rows_affected() == 1)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn abandon_activation_attempt_superseded_by_suspend_best_effort(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
    ) {
        if let Err(error) = self
            .abandon_activation_attempt_superseded_by_suspend(
                sandbox_id,
                generation,
                activation_attempt_id,
            )
            .await
        {
            tracing::warn!(
                sandbox_id = %sandbox_id,
                generation,
                %activation_attempt_id,
                %error,
                "failed to abandon suspend-superseded activation attempt"
            );
        }
    }

    /// Assign `requested_attempt_id` (or the next one after the current
    /// owner) to the active execution of `generation`, under its row lock.
    /// The caller holds the sandbox row's lock.
    ///
    /// An activation other than a stopped sandbox's runtime upgrade clears the
    /// upgrade's record from the execution. An upgrade that recorded itself
    /// and wasn't retired may have changed the disk, whether or not the node
    /// has published its result yet, and may still owe the disk backup that
    /// retiring the execution would have enqueued; the record is its only
    /// trace. So the claim is refused unless the caller enqueued that backup
    /// for the recorded operation (`owed_backup_enqueued`), which it does
    /// outside this transaction (`enqueue_backup_owed_by_stopped_upgrade`,
    /// which answers the refusals no retry changes), retryably: the retry
    /// enqueues it first. Deciding it here,
    /// under the locks, means a lease that expires after the caller's check
    /// can't let a claim clear the record without the backup.
    pub(super) async fn assign_activation_attempt_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        requested_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
        route_publication: ActivationRoutePublication,
        owed_backup_enqueued: Option<Uuid>,
    ) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, SubstrateError> {
        type CurrentAttempt = (
            Option<Uuid>,
            Option<String>,
            f64,
            Option<Uuid>,
            Option<Uuid>,
            Option<String>,
        );
        let current_attempt = sqlx::query_as::<_, CurrentAttempt>(sql!(
            "SELECT activation_attempt_id,
                    activation_attempt_target_state,
                    GREATEST(
                        EXTRACT(EPOCH FROM (activation_attempt_expires_at - NOW())),
                        0.0
                    )::float8,
                    route_activation_id,
                    runtime_upgrade_operation_id,
                    runtime_upgrade_target_environment_ref
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2 AND active
              FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to inspect activation attempt owner")?;
        let Some((
            current_attempt_id,
            current_target,
            lease_remaining_secs,
            route_owner,
            recorded_upgrade,
            recorded_target,
        )) = current_attempt
        else {
            return Err(SubstrateError::Conflict {
                message: "execution changed before activation attempt assignment".to_string(),
            });
        };
        if let Some(owner_attempt_id) = current_attempt_id
            && lease_remaining_secs > 0.0
        {
            return Err(SubstrateError::ActivationInFlight {
                message: format!(
                    "another activation attempt owns this execution for {}",
                    current_target.as_deref().unwrap_or("an unknown target")
                ),
                lease_remaining: Duration::from_secs_f64(lease_remaining_secs),
                same_target: current_target.as_deref() == Some(target_state.as_str()),
                owner_attempt_id:
                    pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
                        owner_attempt_id,
                    ),
            });
        }
        if route_publication == ActivationRoutePublication::PreserveExisting
            && route_owner.is_none()
        {
            crate::metrics::sandbox::api::route_identity_decision(
                RouteIdentityDecisionReason::RotationRequiredUnclassified,
            );
            return Err(SubstrateError::Conflict {
                message: "resident execution has no published route identity; refusing activation assignment"
                    .to_string(),
            });
        }
        // The recorded runtime upgrade belongs to the stopped sandbox's
        // upgrade that claimed the execution (`record_stopped_runtime_upgrade_in`).
        // Any other activation that takes the execution owns it from now on,
        // and nothing it does carries that upgrade, so the record goes. Only
        // the upgrade claims a `stopped` target on a `stopped` row: a drain's
        // re-placement claims it on a `restoring` one.
        let clears_upgrade_record = match recorded_upgrade {
            None => None,
            Some(operation_id) => {
                let upgrade_claims = target_state == SandboxTransitionTargetState::Stopped
                    && sqlx::query_scalar::<_, bool>(sql!(
                        "SELECT EXISTS (
                             SELECT 1 FROM sandboxes WHERE id = $1 AND lifecycle = $2
                         )"
                    ))
                    .bind(sandbox_id.as_uuid())
                    .bind(SandboxLifecycle::Stopped.as_str())
                    .fetch_one(&mut *conn)
                    .await
                    .db_context("failed to read the sandbox's lifecycle")?;
                (!upgrade_claims).then_some(operation_id)
            }
        };
        if let Some(operation_id) = clears_upgrade_record
            && owed_backup_enqueued != Some(operation_id)
        {
            let target = recorded_target.as_deref().unwrap_or("its target");
            return Err(SubstrateError::Unavailable {
                message: format!(
                    "stopped sandbox {sandbox_id}'s runtime upgrade {operation_id} to {target} \
                     may have changed its disk, and the disk backup it owes isn't enqueued; \
                     retry, which enqueues it first"
                ),
            });
        }

        // A resident route may precede an expired operation owner. Respect
        // both IDs so a delayed request cannot move the current lease backward.
        let allocation_floor = route_owner.into_iter().chain(current_attempt_id).max();
        let activation_attempt_id = match allocation_floor {
            Some(route_owner) => {
                let route_owner =
                    pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(
                        route_owner,
                    );
                if requested_attempt_id > route_owner {
                    requested_attempt_id
                } else {
                    pplx_space_common_core::types::SandboxActivationAttemptId::new_after(
                        route_owner,
                    )
                    .ok_or_else(|| SubstrateError::Failed {
                        message: "activation attempt identity space exhausted".to_string(),
                    })?
                }
            }
            None => requested_attempt_id,
        };
        let assigned = match route_publication {
            ActivationRoutePublication::PreserveExisting => {
                sqlx::query_scalar::<_, Uuid>(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND route_activation_id IS NOT NULL
                      RETURNING activation_attempt_id"
                ))
                .bind(sandbox_id.as_uuid())
                .bind(generation)
                .bind(activation_attempt_id)
                .bind(target_state.as_str())
                .bind(super::super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
                .fetch_optional(&mut *conn)
                .await
            }
            ActivationRoutePublication::InitializeFirst => {
                sqlx::query_scalar::<_, Uuid>(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            route_activation_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND state IN ('creating', 'recovering')
                        AND NULLIF(BTRIM(pod_uid), '') IS NULL
                        AND route_activation_id IS NULL
                      RETURNING activation_attempt_id"
                ))
                .bind(sandbox_id.as_uuid())
                .bind(generation)
                .bind(activation_attempt_id)
                .bind(target_state.as_str())
                .bind(super::super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
                .fetch_optional(&mut *conn)
                .await
            }
            ActivationRoutePublication::AdoptLegacyResident => {
                sqlx::query_scalar::<_, Uuid>(sql!(
                    "UPDATE sandbox_k8s_executions
                        SET activation_attempt_id = $3,
                            activation_attempt_expires_at =
                                NOW() + make_interval(secs => $5),
                            activation_attempt_target_state = $4,
                            updated_at = NOW()
                      WHERE sandbox_id = $1
                        AND generation = $2
                        AND active
                        AND route_activation_id IS NULL
                        AND NULLIF(BTRIM(pod_uid), '') IS NOT NULL
                      RETURNING activation_attempt_id"
                ))
                .bind(sandbox_id.as_uuid())
                .bind(generation)
                .bind(activation_attempt_id)
                .bind(target_state.as_str())
                .bind(super::super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
                .fetch_optional(&mut *conn)
                .await
            }
        }
        // Classified like every other statement in this file; `db_context`
        // renders the same `"{what}: {err}"` message the hand-rolled mapper did.
        .db_context("failed to assign activation attempt")?
        .ok_or_else(|| SubstrateError::Conflict {
            message: match route_publication {
                ActivationRoutePublication::PreserveExisting => {
                    "execution changed or has no published route before activation assignment"
                }
                ActivationRoutePublication::InitializeFirst => {
                    "execution changed before first route initialization"
                }
                ActivationRoutePublication::AdoptLegacyResident => {
                    "legacy resident execution changed before route adoption"
                }
            }
            .to_string(),
        })?;
        if clears_upgrade_record.is_some() {
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                    SET runtime_upgrade_operation_id = NULL,
                        runtime_upgrade_target_environment_ref = NULL,
                        runtime_upgrade_authorization_id = NULL
                  WHERE sandbox_id = $1
                    AND generation = $2
                    AND active"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation)
            .execute(&mut *conn)
            .await
            .db_context("failed to clear another activation's runtime upgrade record")?;
        }
        Ok(pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid(assigned))
    }

    /// CAS-clear the `command_ready_pending` marker once an access has confirmed
    /// the guest command API is up. Returns whether this call **claimed** the
    /// row — i.e. cleared a marker that was still set on a `started` sandbox.
    ///
    /// The `lifecycle = 'started'` guard makes this lose to the command-ready
    /// reaper: if the reaper flipped the row to `failed` (and its trigger
    /// cleared the marker) between the access's readiness probe and this clear,
    /// the CAS claims nothing and the caller learns the pod is being torn down
    /// rather than handing back a doomed client. Conversely, if the access
    /// clears the marker first, the reaper's own `started + marker` CAS claims
    /// nothing and backs off. A `false` return is either that lost race or a
    /// benign already-cleared-by-another-access — the caller re-reads lifecycle
    /// to tell them apart.
    pub(in crate::controllers::substrate::kubernetes) async fn clear_command_ready_pending(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        pplx_space_sandbox_store::wake::clear_command_ready_pending(&self.pool, sandbox_id)
            .await
            .db_context("failed to clear command_ready_pending")
    }

    pub(in crate::controllers::substrate::kubernetes) async fn reconcile_activation_fence(
        &self,
        sandbox_id: SandboxId,
        requested_attempt_id: SandboxActivationAttemptId,
        joined_attempt_id: Option<SandboxActivationAttemptId>,
        target_state: SandboxTransitionTargetState,
    ) -> Result<ReconciledActivationFence, SubstrateError> {
        let execution = self
            .active_execution_status(sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Unavailable {
                message: "sandbox has no active Kubernetes execution".to_string(),
            })?;
        let ownership = match (joined_attempt_id, execution.activation_attempt_id) {
            (Some(expected), Some(current)) if expected == current => {
                ActivationFenceOwnership::Joined(current)
            }
            (Some(_), Some(_)) => {
                return Err(SubstrateError::Unavailable {
                    message: "activation owner changed while joining; retry".to_string(),
                });
            }
            (None, Some(current)) if current == requested_attempt_id => {
                ActivationFenceOwnership::Owned(current)
            }
            (None, Some(_)) => {
                return Err(SubstrateError::Unavailable {
                    message: "activation owner changed before runtime work began; retry"
                        .to_string(),
                });
            }
            (_, None)
                if super::super::normalize_lifecycle(&execution.lifecycle)
                    == target_state.as_str()
                    && execution.state == ActiveExecutionState::Running
                    && execution.pod_ip.is_some() =>
            {
                ActivationFenceOwnership::Completed
            }
            (_, None) => {
                return Err(SubstrateError::Unavailable {
                    message: format!(
                        "activation attempt disappeared before reaching {target_state}; retry"
                    ),
                });
            }
        };
        if execution.route_activation_id.is_some()
            && execution.activation_attempt_id != execution.route_activation_id
        {
            let reason = if super::super::normalize_lifecycle(&execution.lifecycle)
                == target_state.as_str()
                && execution.state == ActiveExecutionState::Running
                && execution.pod_ip.is_some()
            {
                RouteIdentityDecisionReason::ResidentTargetIdentityReused
            } else {
                RouteIdentityDecisionReason::ExistingExecutionTransition
            };
            crate::metrics::sandbox::api::route_identity_decision(reason);
        }
        Ok(ReconciledActivationFence {
            execution,
            ownership,
        })
    }
}

/// The retryable refusal of a stopped sandbox's runtime upgrade while a
/// drain's stopped evacuation has claimed the sandbox.
fn evacuation_refusal(sandbox_id: SandboxId) -> SubstrateError {
    SubstrateError::Unavailable {
        message: format!(
            "a drain is evacuating stopped sandbox {sandbox_id}; retry its runtime upgrade once \
             it's placed again"
        ),
    }
}

/// See [`K8sStore::stopped_runtime_upgrade_claim`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::controllers::substrate::kubernetes) struct StoppedRuntimeUpgradeClaim {
    pub(in crate::controllers::substrate::kubernetes) generation: i64,
    pub(in crate::controllers::substrate::kubernetes) activation_attempt_id:
        SandboxActivationAttemptId,
    /// The execution's pod name, which its generation fixes.
    pub(in crate::controllers::substrate::kubernetes) pod_name: String,
    /// The node the execution names, the one holding the disk its pod is
    /// pinned to; `None` before any pod was placed.
    pub(in crate::controllers::substrate::kubernetes) node_name: Option<String>,
    /// Whether its lease still runs: an upgrade may still own the execution.
    pub(in crate::controllers::substrate::kubernetes) live: bool,
    /// The upgrade the claiming attempt recorded before its pod; `None` when
    /// it never got that far, so no pod carried it.
    pub(in crate::controllers::substrate::kubernetes) upgrade: Option<RecordedRuntimeUpgrade>,
}

/// What a stopped sandbox's runtime upgrade records on the execution it
/// claims, and the node holding the disk its pod is pinned to, which the
/// claim checks again under the sandbox row's lock
/// ([`K8sStore::record_stopped_runtime_upgrade_in`]).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::controllers::substrate::kubernetes) struct StoppedUpgradeRecord {
    pub(in crate::controllers::substrate::kubernetes) upgrade: RecordedRuntimeUpgrade,
    pub(in crate::controllers::substrate::kubernetes) disk_node: String,
}

/// What a stopped sandbox's runtime upgrade sends sandbox-manager, as far as
/// a retry must repeat it: sandbox-manager refuses an operation that names
/// another target or authorization than the one it recorded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::controllers::substrate::kubernetes) struct RecordedRuntimeUpgrade {
    pub(in crate::controllers::substrate::kubernetes) operation_id: Uuid,
    pub(in crate::controllers::substrate::kubernetes) target:
        pplx_space_common_core::types::EnvironmentReference,
    pub(in crate::controllers::substrate::kubernetes) authorization_id: String,
}

/// What [`K8sStore::assign_activation_attempt_guarded`] checks under the
/// sandbox's row lock before it assigns.
#[derive(Clone, Copy)]
enum AssignmentGuard {
    /// The sandbox exists.
    Any,
    /// The sandbox is still stopped, no drain's stopped evacuation has
    /// claimed it, and the active execution's attempt is still the one an
    /// earlier runtime upgrade left, so a start that moved the sandbox or
    /// claimed the execution meanwhile keeps it, as does an evacuation that
    /// took the execution over. Each refusal is a retryable `Unavailable`.
    StoppedLeftBy(SandboxActivationAttemptId),
}

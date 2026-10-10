//! Activation attempt fence: assigning, completing, releasing, and abandoning
//! activation attempts, route identity publication, and fence reconciliation.

use super::*;

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
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start activation-attempt transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }
        let row = self
            .active_execution_row(&mut tx, sandbox_id)
            .await?
            .ok_or_else(|| SubstrateError::Conflict {
                message: "execution changed before activation attempt assignment".to_string(),
            })?;
        let route_publication = classify_activation_route_publication(&row, false)?;
        let activation_attempt_id = Self::assign_activation_attempt_in(
            &mut tx,
            sandbox_id,
            generation,
            activation_attempt_id,
            target_state,
            route_publication,
        )
        .await?;
        tx.commit()
            .await
            .db_context("failed to commit activation-attempt transaction")?;
        match route_publication {
            ActivationRoutePublication::InitializeFirst => {
                pplx_space_metrics::sandbox::api::route_identity_decision(
                    RouteIdentityDecisionReason::FirstIdentityInitialized,
                );
            }
            ActivationRoutePublication::AdoptLegacyResident => {
                pplx_space_metrics::sandbox::api::route_identity_decision(
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
    /// dispatched or that its activation reached command readiness. A completed
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

    pub(super) async fn assign_activation_attempt_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        requested_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId,
        target_state: SandboxTransitionTargetState,
        route_publication: ActivationRoutePublication,
    ) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, SubstrateError> {
        let current_attempt =
            sqlx::query_as::<_, (Option<Uuid>, Option<String>, f64, Option<Uuid>)>(sql!(
                "SELECT activation_attempt_id,
                    activation_attempt_target_state,
                    GREATEST(
                        EXTRACT(EPOCH FROM (activation_attempt_expires_at - NOW())),
                        0.0
                    )::float8,
                    route_activation_id
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1 AND generation = $2 AND active
              FOR UPDATE"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation)
            .fetch_optional(&mut *conn)
            .await
            .db_context("failed to inspect activation attempt owner")?;
        let Some((current_attempt_id, current_target, lease_remaining_secs, route_owner)) =
            current_attempt
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
            pplx_space_metrics::sandbox::api::route_identity_decision(
                RouteIdentityDecisionReason::RotationRequiredUnclassified,
            );
            return Err(SubstrateError::Conflict {
                message: "resident execution has no published route identity; refusing activation assignment"
                    .to_string(),
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
        let result = sqlx::query(sql!(
            "UPDATE sandboxes
             SET command_ready_pending = FALSE
             WHERE id = $1 AND lifecycle = 'started' AND command_ready_pending"
        ))
        .bind(sandbox_id)
        .execute(&self.pool)
        .await
        .db_context("failed to clear command_ready_pending")?;
        Ok(result.rows_affected() > 0)
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
            pplx_space_metrics::sandbox::api::route_identity_decision(reason);
        }
        Ok(ReconciledActivationFence {
            execution,
            ownership,
        })
    }
}

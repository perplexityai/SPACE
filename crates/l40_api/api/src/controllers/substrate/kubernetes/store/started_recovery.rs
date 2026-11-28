//! Started-sandbox recovery: the recovery lease, its restore context, replacing
//! the recovered execution, and observing or completing the recovery.

use super::*;
use pplx_space_sandbox_store::started_recovery_source::{
    StartedRecoveryRestoreCandidate, started_recovery_restore_candidate,
};

impl K8sStore {
    async fn started_recovery_lease_remaining(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Duration, SubstrateError> {
        let remaining_millis =
            pplx_space_sandbox_store::k8s_executions::started_recovery_lease_remaining_millis(
                &self.pool,
                sandbox_id,
                generation,
                recovery_lease_ttl_secs(),
            )
            .await
            .db_context("failed to read started recovery lease")?;
        let Some(remaining_millis) = remaining_millis else {
            return Ok(Duration::ZERO);
        };
        let remaining_millis =
            u64::try_from(remaining_millis).map_err(|err| SubstrateError::Failed {
                message: format!("invalid started recovery lease duration: {err}"),
            })?;
        Ok(Duration::from_millis(remaining_millis))
    }

    async fn fresh_started_recovery(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<(i64, Duration)>, SubstrateError> {
        let row = pplx_space_sandbox_store::k8s_executions::fresh_started_recovery(
            &self.pool,
            sandbox_id,
            ActiveExecutionState::Recovering.as_str(),
            recovery_lease_ttl_secs(),
        )
        .await
        .db_context("failed to inspect started recovery lease")?;

        row.map(|recovery| {
            u64::try_from(recovery.lease_remaining_millis)
                .map(|remaining| (recovery.generation, Duration::from_millis(remaining)))
                .map_err(|err| SubstrateError::Failed {
                    message: format!("invalid started recovery lease duration: {err}"),
                })
        })
        .transpose()
    }

    pub(in crate::controllers::substrate::kubernetes) async fn claim_started_recovery(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<StartedRecoveryClaim, SubstrateError> {
        if let Some((generation, lease_remaining)) = self.fresh_started_recovery(sandbox_id).await?
        {
            return Ok(StartedRecoveryClaim::InFlight {
                generation,
                lease_remaining,
            });
        }

        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes recovery transaction")?;
        let Some((lifecycle, drain)) = Self::lock_sandbox_for_claim_in(&mut tx, sandbox_id).await?
        else {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        };
        match lifecycle {
            SandboxLifecycle::Started => {
                super::execution_rows::refuse_started_recovery_under_v2_drain(
                    sandbox_id, lifecycle, drain,
                )?;
            }
            SandboxLifecycle::Suspending => {
                return Err(SubstrateError::suspend_in_flight(sandbox_id));
            }
            lifecycle => {
                return Err(SubstrateError::Conflict {
                    message: format!(
                        "sandbox cannot materialize a {} execution while lifecycle is {lifecycle}",
                        SandboxLifecycle::Started,
                    ),
                });
            }
        }

        let claim = self
            .ensure_active_execution_claim_in(&mut tx, sandbox_id, None)
            .await?;
        let generation = pplx_space_sandbox_store::k8s_executions::started_recovery::claim(
            &mut *tx,
            sandbox_uuid,
            claim.row.generation,
            claim.created,
            recovery_lease_ttl_secs(),
        )
        .await
        .db_context("failed to claim started sandbox recovery")?;

        tx.commit()
            .await
            .db_context("failed to commit Kubernetes recovery transaction")?;

        match generation {
            Some(generation) if claim.created => {
                Ok(StartedRecoveryClaim::CreatedExecution { generation })
            }
            Some(generation) => Ok(StartedRecoveryClaim::ExistingExecution { generation }),
            None => Ok(StartedRecoveryClaim::InFlight {
                generation: claim.row.generation,
                lease_remaining: self
                    .started_recovery_lease_remaining(sandbox_id, claim.row.generation)
                    .await?,
            }),
        }
    }

    pub(in crate::controllers::substrate::kubernetes) async fn started_recovery_restore_context(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> Result<StartedRecoveryRestoreContext, SubstrateError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .db_context("failed to acquire started recovery restore connection")?;
        self.started_recovery_restore_context_in(&mut conn, sandbox_id, generation)
            .await
    }

    async fn started_recovery_restore_context_in(
        &self,
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
    ) -> Result<StartedRecoveryRestoreContext, SubstrateError> {
        let candidate = started_recovery_restore_candidate(&mut *conn, sandbox_id, generation)
            .await
            .db_context("failed to select started recovery restore context")?;
        let Some(StartedRecoveryRestoreCandidate {
            state,
            source_node_name,
            storage_uri,
        }) = candidate
        else {
            return Ok(StartedRecoveryRestoreContext::Unavailable);
        };
        match state
            .parse::<SnapshotState>()
            .map_err(|err| SubstrateError::Failed {
                message: format!("invalid started recovery snapshot state: {err}"),
            })? {
            SnapshotState::Creating => Ok(source_node_name.map_or(
                StartedRecoveryRestoreContext::Pending,
                |source_node_name| {
                    StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                        source_node_name,
                    })
                },
            )),
            SnapshotState::Ready => {
                let Some(storage_uri) = storage_uri else {
                    return Ok(StartedRecoveryRestoreContext::Unavailable);
                };
                let storage_uri =
                    ExactStorageUri::parse(&storage_uri).map_err(|err| SubstrateError::Failed {
                        message: format!("invalid started recovery restore storage URI: {err}"),
                    })?;
                Ok(StartedRecoveryRestoreContext::Ready(
                    StartedRecoveryRestoreSource::Remote { storage_uri },
                ))
            }
            SnapshotState::Failed => Ok(StartedRecoveryRestoreContext::Unavailable),
            // A `deleting` row is claimed for prune and de-advertised; restore
            // must not route to it even though its bytes may still exist. A
            // marker-matched row in this state also means the retention
            // guard's current-artifact exclusion was breached — surface that
            // loudly rather than silently reporting no restore source.
            SnapshotState::Deleting => {
                tracing::warn!(
                    sandbox_id = %sandbox_id,
                    "started recovery restore context: marker-matched snapshot \
                     is claimed 'deleting'; treating the restore source as gone"
                );
                Ok(StartedRecoveryRestoreContext::Unavailable)
            }
        }
    }

    pub(in crate::controllers::substrate::kubernetes) async fn replace_started_recovery_execution(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        allowed_local_source: Option<&str>,
    ) -> Result<StartedRecoveryReplacement, SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start started recovery handoff transaction")?;

        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await? {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        }

        let previous_attempt =
            pplx_space_sandbox_store::k8s_executions::started_recovery::lock_recovering_execution(
                &mut *tx,
                sandbox_uuid,
                generation.get(),
            )
            .await
            .db_context("failed to lock started recovery execution")?;
        let Some(previous_attempt) = previous_attempt else {
            tx.commit()
                .await
                .db_context("failed to commit lost started recovery handoff")?;
            return Ok(StartedRecoveryReplacement::OwnershipLost);
        };
        let activation_attempt_id = previous_attempt.activation_attempt_id;
        let activation_target = previous_attempt
            .activation_attempt_target_state
            .map(|target| target.parse::<SandboxTransitionTargetState>())
            .transpose()
            .map_err(|err| SubstrateError::Failed {
                message: format!("invalid started recovery activation target: {err}"),
            })?;

        let source = match self
            .started_recovery_restore_context_in(&mut tx, sandbox_id, generation)
            .await?
        {
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                source_node_name,
            }) if allowed_local_source == Some(source_node_name.as_str()) => {
                StartedRecoveryRestoreSource::Local { source_node_name }
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Local {
                ..
            })
            | StartedRecoveryRestoreContext::Pending => {
                tx.commit()
                    .await
                    .db_context("failed to commit pending started recovery handoff")?;
                return Ok(StartedRecoveryReplacement::Pending);
            }
            StartedRecoveryRestoreContext::Ready(StartedRecoveryRestoreSource::Remote {
                storage_uri,
            }) => StartedRecoveryRestoreSource::Remote { storage_uri },
            StartedRecoveryRestoreContext::Unavailable => {
                tx.commit()
                    .await
                    .db_context("failed to commit unavailable started recovery handoff")?;
                return Ok(StartedRecoveryReplacement::Unavailable);
            }
        };

        if !self
            .finish_active_execution_generation_in(
                &mut tx,
                sandbox_uuid,
                generation.get(),
                "replaced",
            )
            .await?
        {
            return Err(SubstrateError::Failed {
                message: "started recovery execution changed during atomic handoff".to_string(),
            });
        }

        let mut claim = self
            .insert_active_execution_claim_in(&mut tx, sandbox_id)
            .await?;
        if !claim.created {
            return Err(SubstrateError::Failed {
                message: "failed to allocate started recovery successor execution".to_string(),
            });
        }
        let updated =
            pplx_space_sandbox_store::k8s_executions::started_recovery::hand_off_to_successor(
                &mut *tx,
                sandbox_uuid,
                claim.row.generation,
                activation_attempt_id,
                activation_target.map(SandboxTransitionTargetState::as_str),
                super::super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS,
            )
            .await
            .db_context("failed to preserve started recovery ownership")?;
        if updated != 1 {
            return Err(SubstrateError::Failed {
                message: "failed to preserve started recovery ownership".to_string(),
            });
        }
        claim.row.state = ActiveExecutionState::Recovering;
        claim.row.activation_attempt_id = activation_attempt_id
            .map(pplx_space_sandbox_model::SandboxActivationAttemptId::from_uuid);
        claim.row.route_activation_id = claim.row.activation_attempt_id;

        tx.commit()
            .await
            .db_context("failed to commit started recovery handoff")?;
        if activation_attempt_id.is_some() {
            crate::metrics::sandbox::api::route_identity_decision(
                RouteIdentityDecisionReason::ExecutionReplaced,
            );
        }
        Ok(StartedRecoveryReplacement::Replaced {
            claim: Box::new(claim),
            source,
        })
    }

    pub(in crate::controllers::substrate::kubernetes) async fn complete_started_recovery(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
        completion: StartedRecoveryCompletion,
    ) -> Result<(), SubstrateError> {
        if completion == StartedRecoveryCompletion::Failed {
            let generation =
                ExecutionGeneration::new(generation).map_err(|err| SubstrateError::Failed {
                    message: format!(
                        "invalid started recovery execution generation {generation}: {err}"
                    ),
                })?;
            return self
                .complete_failed_started_recovery(sandbox_id, generation, activation_attempt_id)
                .await;
        }

        pplx_space_sandbox_store::k8s_executions::started_recovery::complete(
            &self.pool,
            sandbox_id,
            generation,
            completion.as_str(),
            ActiveExecutionState::Recovering.as_str(),
        )
        .await
        .db_context_with(|| {
            format!("failed to complete started sandbox recovery as {completion}")
        })?;
        Ok(())
    }

    async fn complete_failed_started_recovery(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        activation_attempt_id: Option<SandboxActivationAttemptId>,
    ) -> Result<(), SubstrateError> {
        let sandbox_uuid = sandbox_id;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start failed recovery transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_uuid).await? {
            return Ok(());
        }

        let failed_generation = pplx_space_sandbox_store::k8s_executions::started_recovery::fail(
            &mut *tx,
            sandbox_uuid,
            generation.get(),
            StartedRecoveryCompletion::Failed.as_str(),
            ActiveExecutionState::Recovering.as_str(),
            activation_attempt_id,
        )
        .await
        .db_context("failed to complete started sandbox recovery as failed")?;
        let generation = failed_generation
            .map(ExecutionGeneration::new)
            .transpose()
            .map_err(|err| SubstrateError::Failed {
                message: format!("invalid failed started recovery generation: {err}"),
            })?;
        let recovered = match generation {
            Some(generation) => {
                self.recover_started_after_failed_recovery_in(&mut tx, sandbox_uuid, generation)
                    .await?
            }
            None => false,
        };
        if recovered
            && !self
                .finish_active_execution_generation_in(
                    &mut tx,
                    sandbox_uuid,
                    generation
                        .expect("recovered execution has a generation")
                        .get(),
                    ActiveExecutionState::RecoveryFailed.as_str(),
                )
                .await?
        {
            return Err(SubstrateError::Failed {
                message: "failed to retire converged started recovery execution".to_string(),
            });
        }

        tx.commit()
            .await
            .db_context("failed to commit failed recovery transaction")?;
        if recovered {
            let generation = generation.expect("recovered execution has a generation");
            tracing::info!(
                sandbox_id = %sandbox_id,
                generation = %generation,
                "recovered started sandbox to suspended after terminal recovery failure"
            );
            emit_started_suspend_recovery_event(
                sandbox_uuid,
                StartedSuspendRecoveryOperation::StartedRecoveryFailure,
            );
        }
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn observe_started_recovery(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<StartedRecoveryObservation, SubstrateError> {
        // The sandbox's lifecycle decides what a running execution means: a
        // suspend and a restore can land while a follower waits, and the
        // generation it then finds running is that restore's. Until the row
        // is `started` that restore can still be rolled back, so it is no
        // recovery to hand out (see `join_restore_before_dial`).
        let row = pplx_space_sandbox_store::k8s_executions::recovering_execution(
            &self.pool,
            sandbox_id,
            generation,
            [
                ActiveExecutionState::Creating.as_str(),
                ActiveExecutionState::Recovering.as_str(),
                ActiveExecutionState::Running.as_str(),
            ],
        )
        .await
        .db_context("failed to observe started sandbox recovery")?;

        let Some(pplx_space_sandbox_store::k8s_executions::RecoveringExecution {
            generation,
            state,
            pod_name,
            pod_ip,
            lifecycle,
        }) = row
        else {
            return Ok(StartedRecoveryObservation::Failed);
        };
        let state =
            state
                .parse::<ActiveExecutionState>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse started recovery execution state: {err}"),
                })?;

        let lifecycle =
            lifecycle
                .parse::<SandboxLifecycle>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse started recovery sandbox lifecycle: {err}"),
                })?;
        Ok(match (state, pod_ip) {
            (ActiveExecutionState::Running, Some(pod_ip))
                if lifecycle == SandboxLifecycle::Started =>
            {
                StartedRecoveryObservation::Succeeded {
                    generation,
                    pod_name,
                    pod_ip,
                }
            }
            (ActiveExecutionState::Running, _) if lifecycle == SandboxLifecycle::Restoring => {
                StartedRecoveryObservation::Pending
            }
            (ActiveExecutionState::Creating | ActiveExecutionState::Recovering, _) => {
                StartedRecoveryObservation::Pending
            }
            _ => StartedRecoveryObservation::Failed,
        })
    }
}

//! Started-sandbox recovery: the recovery lease, its restore context, replacing
//! the recovered execution, and observing or completing the recovery.

use super::*;

impl K8sStore {
    async fn started_recovery_lease_remaining(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
    ) -> Result<Duration, SubstrateError> {
        let remaining_millis = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT GREATEST(
                        CEIL(EXTRACT(EPOCH FROM (
                            updated_at + make_interval(secs => $3) - NOW()
                        )) * 1000)::BIGINT,
                        0::BIGINT
                    )
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation >= $2 AND active
             ORDER BY generation DESC
             LIMIT 1"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(recovery_lease_ttl_secs())
        .fetch_optional(&self.pool)
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
        let row = sqlx::query_as::<_, (i64, i64)>(sql!(
            "SELECT generation,
                    GREATEST(
                        CEIL(EXTRACT(EPOCH FROM (
                            updated_at + make_interval(secs => $3) - NOW()
                        )) * 1000)::BIGINT,
                        0::BIGINT
                    ) AS lease_remaining_millis
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND active AND state = $2
               AND updated_at > NOW() - make_interval(secs => $3)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(ActiveExecutionState::Recovering.as_str())
        .bind(recovery_lease_ttl_secs())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to inspect started recovery lease")?;

        row.map(|(generation, remaining_millis)| {
            u64::try_from(remaining_millis)
                .map(|remaining| (generation, Duration::from_millis(remaining)))
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
        let Some(lifecycle) =
            Self::lock_sandbox_execution_lifecycle_in(&mut tx, sandbox_id).await?
        else {
            return Err(SubstrateError::NotFound {
                message: format!("sandbox not found: {sandbox_id}"),
            });
        };
        match lifecycle {
            SandboxLifecycle::Started => {}
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
        let generation = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = 'recovering', updated_at = NOW()
             WHERE sandbox_id = $1 AND generation = $2 AND active
               AND (
                 ($3 AND state = 'creating')
                 OR (
                   NOT $3
                   AND (
                     state IN ('created', 'running', 'recovery_failed')
                     OR (
                       state IN ('creating', 'recovering')
                       AND updated_at <= NOW() - make_interval(secs => $4)
                     )
                   )
                 )
               )
             RETURNING generation"
        ))
        .bind(sandbox_uuid)
        .bind(claim.row.generation)
        .bind(claim.created)
        .bind(recovery_lease_ttl_secs())
        .fetch_optional(&mut *tx)
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
        let no_newer = no_newer_full_snapshot("s", "sn");
        let query = format!(
            "SELECT sn.state,
                    nk.k8s_node_name,
                    (
                        SELECT ssl.storage_connection_string
                          FROM snapshot_storage_locations ssl
                         WHERE ssl.snapshot_id = sn.id
                           AND ssl.orphaned_at IS NULL
                         ORDER BY ssl.created_at DESC
                         LIMIT 1
                    ) AS storage_uri
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.active
                AND e.state = 'recovering'
               JOIN snapshots sn
                 ON sn.sandbox_id = s.id
                AND sn.local_artifact_id = s.current_local_artifact_id
                AND sn.artifact_kind = s.current_local_artifact_kind
                AND sn.type = 'full'
                AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
               LEFT JOIN nodes n
                 ON n.id = sn.source_node_id
                AND n.role = 'worker'
                AND n.state = 'ready'
                AND n.drain_phase IS NULL
               LEFT JOIN node_k8s nk ON nk.node_id = n.id
              WHERE s.id = $1
                AND s.lifecycle = 'started'
                AND s.current_local_artifact_origin = 'suspend'
                AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
                AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
                AND {no_newer}
              ORDER BY sn.timestamp DESC, sn.id DESC
              LIMIT 1"
        );
        let candidate = sqlx::query_as::<_, (String, Option<String>, Option<String>)>(
            sqlx::AssertSqlSafe(sql!(query.as_str())),
        )
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to select started recovery restore context")?;
        let Some((state, source_node_name, storage_uri)) = candidate else {
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

        let previous_attempt = sqlx::query_as::<_, (Option<Uuid>, Option<String>)>(sql!(
            "SELECT e.activation_attempt_id, e.activation_attempt_target_state
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.active
                AND e.state = 'recovering'
              WHERE s.id = $1
                AND s.lifecycle = 'started'
              FOR UPDATE OF e"
        ))
        .bind(sandbox_uuid)
        .bind(generation.get())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock started recovery execution")?;
        let Some((activation_attempt_id, activation_target)) = previous_attempt else {
            tx.commit()
                .await
                .db_context("failed to commit lost started recovery handoff")?;
            return Ok(StartedRecoveryReplacement::OwnershipLost);
        };
        let activation_target = activation_target
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
        let updated = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET state = 'recovering',
                    activation_attempt_id = $3,
                    route_activation_id = $3,
                    activation_attempt_expires_at = CASE
                        WHEN $3::UUID IS NULL THEN NULL
                        ELSE NOW() + make_interval(secs => $5)
                    END,
                    activation_attempt_target_state = $4,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND state = 'creating'"
        ))
        .bind(sandbox_uuid)
        .bind(claim.row.generation)
        .bind(activation_attempt_id)
        .bind(activation_target.map(SandboxTransitionTargetState::as_str))
        .bind(super::super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
        .execute(&mut *tx)
        .await
        .db_context("failed to preserve started recovery ownership")?
        .rows_affected();
        if updated != 1 {
            return Err(SubstrateError::Failed {
                message: "failed to preserve started recovery ownership".to_string(),
            });
        }
        claim.row.state = ActiveExecutionState::Recovering;
        claim.row.activation_attempt_id = activation_attempt_id
            .map(pplx_space_common_core::types::SandboxActivationAttemptId::from_uuid);
        claim.row.route_activation_id = claim.row.activation_attempt_id;

        tx.commit()
            .await
            .db_context("failed to commit started recovery handoff")?;
        if activation_attempt_id.is_some() {
            pplx_space_metrics::sandbox::api::route_identity_decision(
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

        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = $3, updated_at = NOW()
             WHERE sandbox_id = $1 AND generation >= $2 AND active AND state = $4"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(completion.as_str())
        .bind(ActiveExecutionState::Recovering.as_str())
        .execute(&self.pool)
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

        let failed_generation = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = $3,
                 activation_attempt_id = NULL,
                 activation_attempt_expires_at = NULL,
                 activation_attempt_target_state = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1
               AND active
               AND state = $4
               AND (
                 (
                   $5::UUID IS NULL
                   AND activation_attempt_id IS NULL
                   AND generation = $2
                 )
                 OR (
                   $5::UUID IS NOT NULL
                   AND activation_attempt_id = $5
                   AND generation >= $2
                 )
               )
             RETURNING generation"
        ))
        .bind(sandbox_uuid)
        .bind(generation.get())
        .bind(StartedRecoveryCompletion::Failed.as_str())
        .bind(ActiveExecutionState::Recovering.as_str())
        .bind(activation_attempt_id)
        .fetch_optional(&mut *tx)
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
        let row = sqlx::query_as::<_, (i64, String, String, Option<String>)>(sql!(
            "SELECT generation, state, pod_name, host(pod_ip)
             FROM sandbox_k8s_executions
             WHERE sandbox_id = $1 AND generation >= $2 AND active
               AND state IN ($3, $4, $5)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(ActiveExecutionState::Creating.as_str())
        .bind(ActiveExecutionState::Recovering.as_str())
        .bind(ActiveExecutionState::Running.as_str())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to observe started sandbox recovery")?;

        let Some((generation, state, pod_name, pod_ip)) = row else {
            return Ok(StartedRecoveryObservation::Failed);
        };
        let state =
            state
                .parse::<ActiveExecutionState>()
                .map_err(|err| SubstrateError::Failed {
                    message: format!("failed to parse started recovery execution state: {err}"),
                })?;

        Ok(match (state, pod_ip) {
            (ActiveExecutionState::Running, Some(pod_ip)) => {
                StartedRecoveryObservation::Succeeded {
                    generation,
                    pod_name,
                    pod_ip,
                }
            }
            (ActiveExecutionState::Creating | ActiveExecutionState::Recovering, _) => {
                StartedRecoveryObservation::Pending
            }
            _ => StartedRecoveryObservation::Failed,
        })
    }
}

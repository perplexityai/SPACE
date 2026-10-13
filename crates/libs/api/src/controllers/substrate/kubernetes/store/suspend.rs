//! Suspend compare-and-swap: claiming a suspend, persisting or accepting its
//! committed artifact, and finalizing the suspended execution.

use super::*;

impl K8sStore {
    pub(in crate::controllers::substrate::kubernetes) async fn finish_suspended_execution(
        &self,
        sandbox_id: SandboxId,
        expected_generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
        published_artifact: super::super::super::PublishedLocalArtifact,
    ) -> Result<(), SubstrateError> {
        let artifact_id = published_artifact.id.as_uuid();
        let artifact_kind = published_artifact.kind.as_str();
        let artifact_origin = ArtifactOrigin::Suspend.as_str();
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start suspended pod transaction")?;

        let lifecycle = sqlx::query_scalar::<_, String>(sql!(
            "SELECT lifecycle
              FROM sandboxes
              WHERE id = $1
                AND lifecycle IN ('suspending', 'suspended')
                AND current_local_artifact_id = $2
                AND current_local_artifact_kind = $3
                AND current_local_artifact_origin = $4
              FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(artifact_id)
        .bind(artifact_kind)
        .bind(artifact_origin)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock suspend artifact for pod retirement")?;
        let Some(lifecycle) = lifecycle else {
            return Err(SubstrateError::Unavailable {
                message: "sandbox changed before suspended pod retirement".to_string(),
            });
        };

        let execution_active = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT active
               FROM sandbox_k8s_executions
              WHERE sandbox_id = $1
                AND generation = $2
                AND pod_uid = $3
                AND state = $4
              FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(expected_generation.get())
        .bind(expected_pod_uid.as_str())
        .bind(ActiveExecutionState::Suspended.as_str())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock suspended Kubernetes execution")?;
        let Some(execution_active) = execution_active else {
            return Err(SubstrateError::Unavailable {
                message: "sandbox execution changed before suspended pod retirement".to_string(),
            });
        };
        let already_finalized = lifecycle == SandboxLifecycle::Suspended.as_str()
            && !execution_active
            && !sqlx::query_scalar::<_, bool>(sql!(
                "SELECT EXISTS (
                     SELECT 1
                       FROM sandbox_k8s_executions
                      WHERE sandbox_id = $1
                        AND generation > $2
                 )"
            ))
            .bind(sandbox_id)
            .bind(expected_generation.get())
            .fetch_one(&mut *tx)
            .await
            .db_context("failed to check for successor Kubernetes execution")?;
        let pending_finalization =
            lifecycle == SandboxLifecycle::Suspending.as_str() && execution_active;
        if !pending_finalization && !already_finalized {
            return Err(SubstrateError::Unavailable {
                message: "sandbox or execution changed before suspended pod retirement".to_string(),
            });
        }

        if pending_finalization {
            Self::finalize_suspended_sandbox_in(
                &mut tx,
                sandbox_id,
                artifact_id,
                artifact_kind,
                artifact_origin,
            )
            .await?;
        }

        // The claimed pod is gone once the suspend finalizes; drop the slot
        // receipt with it. The stale-claimed sweep decides ownership from
        // slot state rather than the retired compatibility projection.
        let slot_deleted =
            WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut tx, sandbox_id)
                .await
                .db_context("failed to delete claimed pool slot")?;
        if slot_deleted {
            tracing::info!(
                sandbox_id = %sandbox_id,
                path = "suspend_finalize",
                "deleted claimed pool slot inline with pod teardown"
            );
        }

        tx.commit()
            .await
            .db_context("failed to commit suspended pod transaction")?;
        Ok(())
    }

    async fn finalize_suspended_sandbox_in(
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
        artifact_id: Uuid,
        artifact_kind: &str,
        artifact_origin: &str,
    ) -> Result<(), SubstrateError> {
        let marker_assignments = write_current_local_artifact_marker_assignments("$2", "$3", "$4");
        let finalization = suspended_k8s_finalization_sql(
            "s",
            "candidate",
            &marker_assignments,
            SuspendedTargetNodeAssignment::Preserve,
        );
        let query = format!(
            "WITH candidate AS MATERIALIZED (
                 SELECT id
                   FROM sandboxes
                  WHERE id = $1
             ),
             {retire_active_execution_cte},
             {invalidate_open_port_exposures_cte}
             UPDATE sandboxes s
                SET {sandbox_assignments}
               FROM candidate
              WHERE s.id = candidate.id",
            retire_active_execution_cte = finalization.retire_active_execution_cte(),
            invalidate_open_port_exposures_cte = finalization.invalidate_open_port_exposures_cte(),
            sandbox_assignments = finalization.sandbox_assignments(),
        );

        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(artifact_id)
            .bind(artifact_kind)
            .bind(artifact_origin)
            .execute(&mut **tx)
            .await
            .db_context("failed to mark suspended sandbox unplaced")?;
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn persist_or_accept_suspend_commit(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
        published_artifact: super::super::super::PublishedLocalArtifact,
    ) -> Result<(), SubstrateError> {
        let artifact_id = published_artifact.id.as_uuid();
        let artifact_kind = published_artifact.kind.as_str();
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes suspend commit transaction")?;
        let sandbox_exists = sqlx::query_scalar::<_, Uuid>(sql!(
            "SELECT id
               FROM sandboxes
              WHERE id = $1
              FOR UPDATE"
        ))
        .bind(sandbox_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to lock sandbox for Kubernetes suspend commit")?
        .is_some();
        if !sandbox_exists {
            return Err(SubstrateError::Unavailable {
                message: "sandbox disappeared before its suspend commit was persisted".to_string(),
            });
        }
        // A fresh suspend publishes a newly minted local artifact id, so it
        // cannot collide with an older claimed snapshot. This fence protects
        // completion retries that replay an artifact after its snapshot row
        // has already moved to `deleting`.
        reject_deleting_current_local_artifact(&mut tx, sandbox_id, published_artifact).await?;
        let execution_updated = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET state = $4,
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND generation = $2
                AND active
                AND pod_uid = $3
                AND state IN ($4, $5)"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .bind(ActiveExecutionState::Suspended.as_str())
        .bind(ActiveExecutionState::Running.as_str())
        .execute(&mut *tx)
        .await
        .db_context("failed to mark Kubernetes execution suspend committed")?;
        if execution_updated.rows_affected() == 0 {
            let forgotten_execution_committed = sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions e
                    SET state = $4,
                        updated_at = NOW()
                   FROM sandboxes s
                  WHERE e.sandbox_id = $1
                    AND e.generation = $2
                    AND e.pod_uid = $3
                    AND NOT e.active
                    AND e.state = 'forgotten'
                    AND s.id = e.sandbox_id
                    AND s.lifecycle = 'suspending'
                    AND NOT EXISTS (
                        SELECT 1
                          FROM sandbox_k8s_executions successor
                         WHERE successor.sandbox_id = e.sandbox_id
                           AND successor.generation > e.generation
                    )"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation.get())
            .bind(expected_pod_uid.as_str())
            .bind(ActiveExecutionState::Suspended.as_str())
            .execute(&mut *tx)
            .await
            .db_context("failed to commit suspend after terminal Kubernetes pod cleanup")?;
            if forgotten_execution_committed.rows_affected() == 1 {
                Self::finalize_suspended_sandbox_in(
                    &mut tx,
                    sandbox_id,
                    artifact_id,
                    artifact_kind,
                    ArtifactOrigin::Suspend.as_str(),
                )
                .await?;
                tx.commit().await.db_context(
                    "failed to commit finalized Kubernetes suspend after pod cleanup",
                )?;
                return Ok(());
            }

            let exact_projection = sqlx::query_scalar::<_, bool>(sql!(
                "SELECT EXISTS (
                     SELECT 1
                       FROM sandboxes s
                       JOIN sandbox_k8s_executions e
                         ON e.sandbox_id = s.id
                        AND e.generation = $2
                        AND e.pod_uid = $3
                      WHERE s.id = $1
                        AND s.lifecycle = 'suspended'
                        AND s.current_local_artifact_id = $4
                        AND s.current_local_artifact_kind = $5
                        AND s.current_local_artifact_origin = $6
                        AND e.state = $7
                        AND NOT e.active
                        AND NOT EXISTS (
                            SELECT 1
                              FROM sandbox_k8s_executions successor
                             WHERE successor.sandbox_id = s.id
                               AND successor.generation > e.generation
                        )
                 )"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation.get())
            .bind(expected_pod_uid.as_str())
            .bind(artifact_id)
            .bind(artifact_kind)
            .bind(ArtifactOrigin::Suspend.as_str())
            .bind(ActiveExecutionState::Suspended.as_str())
            .fetch_one(&mut *tx)
            .await
            .db_context("failed to accept projected Kubernetes suspend commit")?;
            if exact_projection {
                tx.commit()
                    .await
                    .db_context("failed to commit accepted Kubernetes suspend projection")?;
                return Ok(());
            }
            return Err(SubstrateError::Unavailable {
                message:
                    "sandbox execution changed before its suspend commit was persisted or projected"
                        .to_string(),
            });
        }
        let updated = sqlx::query(sql!(
            "UPDATE sandboxes
                SET lifecycle = 'suspending',
                    current_local_artifact_id = $2,
                    current_local_artifact_kind = $3,
                    current_local_artifact_origin = 'suspend'
              WHERE id = $1
                AND lifecycle = 'suspending'"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(artifact_id)
        .bind(artifact_kind)
        .execute(&mut *tx)
        .await
        .db_context("failed to persist Kubernetes suspend commit")?;
        if updated.rows_affected() != 1 {
            return Err(SubstrateError::Unavailable {
                message: "sandbox execution changed before its suspend commit was persisted"
                    .to_string(),
            });
        }
        tx.commit()
            .await
            .db_context("failed to commit Kubernetes suspend transaction")?;
        Ok(())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn claim_suspend_execution(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
    ) -> Result<SuspendExecutionClaim, SubstrateError> {
        for previous in [SandboxLifecycle::Started, SandboxLifecycle::Paused] {
            let claimed = sqlx::query(sql!(
                "UPDATE sandboxes s
                    SET lifecycle = 'suspending'
                   FROM sandbox_k8s_executions e
                  WHERE s.id = $1
                    AND s.lifecycle = $4
                    AND e.sandbox_id = s.id
                    AND e.generation = $2
                    AND e.active
                    AND e.pod_uid = $3
                    AND e.state = 'running'"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(generation.get())
            .bind(expected_pod_uid.as_str())
            .bind(previous.as_str())
            .execute(&self.pool)
            .await
            .db_context("failed to claim Kubernetes suspend execution")?;
            if claimed.rows_affected() == 1 {
                return Ok(SuspendExecutionClaim::Claimed);
            }
        }

        let existing: bool = sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
                  WHERE s.id = $1
                    AND s.lifecycle = 'suspending'
                    AND e.generation = $2
                    AND e.active
                    AND e.pod_uid = $3
                    AND e.state IN ('running', 'suspended')
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .fetch_one(&self.pool)
        .await
        .db_context("failed to verify Kubernetes suspend execution claim")?;
        if existing {
            return Ok(SuspendExecutionClaim::Existing);
        }

        match self
            .committed_suspend_artifact(sandbox_id, generation, expected_pod_uid)
            .await
        {
            Ok(Some(artifact)) => Ok(SuspendExecutionClaim::Completed(artifact)),
            Ok(None) | Err(SubstrateError::Unavailable { .. }) => {
                Err(SubstrateError::Unavailable {
                    message: "sandbox execution changed before suspend ownership was claimed"
                        .to_string(),
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Match the node's exact suspend observation to an unfinished backup.
    /// The committed sandbox artifact marker is absent or from the prior cycle
    /// until finalization. This separate evidence authorizes only waiting.
    pub(in crate::controllers::substrate::kubernetes) async fn local_suspend_backup_pending(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1
                   FROM sandboxes s
                   JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
                   JOIN node_facet_observed_status observed
                     ON observed.node_id = s.active_node_id
                    AND observed.facet = $4
                    AND observed.element = s.id::TEXT
                   JOIN snapshots sn
                     ON sn.sandbox_id = s.id
                    AND sn.source_node_id = observed.node_id
                    AND sn.local_artifact_id::TEXT =
                        observed.observed_payload -> $5::TEXT ->> 'local_artifact_id'
                  WHERE s.id = $1
                    AND s.lifecycle = 'started'
                    AND e.generation = $2
                    AND e.pod_uid = $3
                    AND e.active
                    AND e.state = 'recovering'
                    AND observed.observed_payload -> $5::TEXT ->> 'execution_generation' = e.generation::TEXT
                    AND observed.observed_payload -> $5::TEXT ->> 'pod_uid' = e.pod_uid
                    AND sn.type = 'full'
                    AND sn.artifact_kind = 'full'
                    AND sn.artifact_origin = 'suspend'
                    AND sn.state = 'creating'
                    AND (s.last_command_at IS NULL OR sn.timestamp >= s.last_command_at)
                    AND (s.last_access_at IS NULL OR sn.timestamp >= s.last_access_at)
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .bind(FACET_SANDBOX_DESIRED_STATE)
        .bind(SANDBOX_LOCAL_SUSPEND_KEY)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| SubstrateError::Failed {
            message: format!("failed to observe local suspend backup: {error}"),
        })
    }

    pub(in crate::controllers::substrate::kubernetes) async fn committed_suspend_artifact(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
    ) -> Result<Option<super::super::super::PublishedLocalArtifact>, SubstrateError> {
        let marker: Option<CommittedSuspendArtifactRow> = sqlx::query_as(sql!(
            "SELECT e.state AS execution_state,
                    s.current_local_artifact_id AS artifact_id,
                    s.current_local_artifact_kind AS artifact_kind,
                    s.current_local_artifact_origin AS artifact_origin
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.pod_uid = $3
              WHERE s.id = $1
                AND (
                    (s.lifecycle = 'suspending' AND e.active)
                    OR (
                        s.lifecycle = 'suspended'
                        AND e.state = 'suspended'
                        AND NOT e.active
                        AND NOT EXISTS (
                            SELECT 1
                              FROM sandbox_k8s_executions successor
                             WHERE successor.sandbox_id = s.id
                               AND successor.generation > e.generation
                        )
                    )
                )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to load committed Kubernetes suspend artifact")?;
        let Some(marker) = marker else {
            return Err(SubstrateError::Unavailable {
                message: "committed Kubernetes suspend execution changed before pod retirement"
                    .to_string(),
            });
        };
        if marker.execution_state != ActiveExecutionState::Suspended {
            return Ok(None);
        }
        match (
            marker.artifact_id,
            marker.artifact_kind,
            marker.artifact_origin,
        ) {
            (None, None, None) => Ok(None),
            (Some(artifact_id), Some(artifact_kind), Some(artifact_origin)) => {
                if artifact_origin != ArtifactOrigin::Suspend {
                    return Err(SubstrateError::Failed {
                        message: format!(
                            "committed Kubernetes suspend has {artifact_origin} artifact origin"
                        ),
                    });
                }
                Ok(Some(super::super::super::PublishedLocalArtifact {
                    id: artifact_id,
                    kind: artifact_kind,
                }))
            }
            _ => Err(SubstrateError::Failed {
                message: "committed Kubernetes suspend has incomplete artifact identity"
                    .to_string(),
            }),
        }
    }

    /// The durable suspend artifact of this execution's in-flight suspend, as
    /// recorded by the control plane, for when the node can no longer publish
    /// it: a node that tears the sandbox down after its suspend backup has
    /// finalized answers the suspend poll with a generation-fenced delete.
    ///
    /// Accepts only the newest full suspend snapshot of a `suspending` row,
    /// taken by the active execution's node after the suspend began, ready and
    /// with a live storage location, so a stale or foreign artifact cannot
    /// finalize this suspend.
    pub(in crate::controllers::substrate::kubernetes) async fn durable_suspend_snapshot_artifact(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
    ) -> Result<Option<super::super::super::PublishedLocalArtifact>, SubstrateError> {
        let row: Option<(Uuid, String)> = sqlx::query_as(sql!(
            "SELECT sn.local_artifact_id, sn.artifact_kind
               FROM sandboxes s
               JOIN sandbox_k8s_executions e
                 ON e.sandbox_id = s.id
                AND e.generation = $2
                AND e.pod_uid = $3
                AND e.active
               JOIN snapshots sn
                 ON sn.sandbox_id = s.id
              WHERE s.id = $1
                AND s.lifecycle = 'suspending'
                AND sn.type = 'full'
                AND sn.state = 'ready'
                AND sn.artifact_origin = 'suspend'
                AND sn.artifact_kind IS NOT NULL
                AND sn.local_artifact_id IS NOT NULL
                AND sn.source_node_id = s.active_node_id
                AND sn.timestamp >= s.lifecycle_changed_at
                AND EXISTS (
                    SELECT 1
                      FROM snapshot_storage_locations l
                     WHERE l.snapshot_id = sn.id
                       AND l.orphaned_at IS NULL
                )
                AND NOT EXISTS (
                    SELECT 1
                      FROM snapshots newer
                     WHERE newer.sandbox_id = s.id
                       AND newer.id <> sn.id
                       AND newer.timestamp >= sn.timestamp
                       AND (newer.state = 'creating' OR newer.type = 'full')
                )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation.get())
        .bind(expected_pod_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .db_context("failed to load durable Kubernetes suspend snapshot")?;
        row.map(|(artifact_id, artifact_kind)| {
            let kind =
                ArtifactKind::try_from(artifact_kind).map_err(|error| SubstrateError::Failed {
                    message: format!(
                        "durable suspend snapshot has an invalid artifact kind: {error}"
                    ),
                })?;
            Ok(super::super::super::PublishedLocalArtifact {
                id: LocalArtifactId::from_uuid(artifact_id),
                kind,
            })
        })
        .transpose()
    }

    /// A pod that dies while its sandbox is `suspending` belongs to the
    /// suspend flow: finalization retires the execution as `suspended`, and
    /// the transient-lifecycle sweeper ages out suspends that never finish.
    /// Forgetting the execution here instead clears `active_node_id`, which
    /// makes the publish ingest drop the node's final `suspended` record —
    /// the projector requires an actively-placed publisher — and strands
    /// the row in `suspending`. Read inside the caller's transaction, after
    /// the per-sandbox lock, so the answer cannot race a lifecycle change.
    pub(super) async fn suspend_owns_pod_teardown_in(
        tx: &mut Transaction<'_, Postgres>,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        let suspending: Option<bool> = sqlx::query_scalar(sql!(
            "SELECT LOWER(TRIM(lifecycle)) = 'suspending' FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut **tx)
        .await
        .db_context("failed to read sandbox lifecycle for pod tracking cleanup")?;
        Ok(suspending.unwrap_or(false))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn finalize_missing_suspending_pod_with_current_snapshot(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: &str,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start missing suspend pod transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }

        let eligibility = missing_suspending_pod_revert_eligibility("s");
        let clear_marker = eligibility.clear_marker;
        let eligible = eligibility.eligible;
        let query = format!(
            "SELECT {clear_marker}
               FROM sandboxes s
              WHERE s.id = $1
                AND LOWER(TRIM(s.lifecycle)) = 'suspending'
                AND s.active_node_id IS NOT NULL
                AND EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions e
                     WHERE e.sandbox_id = s.id
                       AND e.generation = $2
                       AND e.pod_uid = $3
                       AND e.active
                       AND e.state IN ('running', 'suspended')
                )
                AND NOT EXISTS (
                    SELECT 1
                      FROM sandbox_k8s_executions successor
                     WHERE successor.sandbox_id = s.id
                       AND successor.generation > $2
                )
                AND {eligible}",
        );
        let clear_marker = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(generation)
            .bind(pod_uid)
            .fetch_optional(&mut *tx)
            .await
            .db_context("failed to select missing suspended pod finalization")?;
        let Some(clear_marker) = clear_marker else {
            return Ok(false);
        };

        self.finish_active_execution_in(&mut tx, sandbox_id, SandboxLifecycle::Suspended.as_str())
            .await?;

        let marker_assignments = clear_current_local_artifact_marker_assignments("s", "$2");
        let sandbox_assignments = suspended_k8s_finalization_sql(
            "s",
            "candidate",
            &marker_assignments,
            SuspendedTargetNodeAssignment::Preserve,
        )
        .sandbox_assignments();
        let query = format!("UPDATE sandboxes s SET {sandbox_assignments} WHERE s.id = $1");
        sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox_id)
            .bind(clear_marker)
            .execute(&mut *tx)
            .await
            .db_context("failed to finalize missing suspended pod")?;
        WarmPoolStore::delete_claimed_slot_for_retired_execution(&mut tx, sandbox_id)
            .await
            .db_context("failed to delete claimed pool slot")?;
        tx.commit()
            .await
            .db_context("failed to commit missing suspend pod transaction")?;
        SandboxEvent::new(
            sandbox_id,
            SandboxEventType::SuspendCompleted,
            SandboxEventSource::Substrate,
        )
        .emit_with(
            SandboxEventDetails::default()
                .reason("missing_pod_current_snapshot")
                .execution_generation(generation.to_string()),
        );
        Ok(true)
    }
}

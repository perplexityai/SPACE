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
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start suspended pod transaction")?;

        let lifecycle = pplx_space_sandbox_store::suspend::lock_committed_artifact(
            &mut *tx,
            sandbox_id,
            published_artifact.id,
            published_artifact.kind,
        )
        .await
        .db_context("failed to lock suspend artifact for pod retirement")?;
        let Some(lifecycle) = lifecycle else {
            return Err(SubstrateError::Unavailable {
                message: "sandbox changed before suspended pod retirement".to_string(),
            });
        };

        let execution_active =
            pplx_space_sandbox_store::k8s_executions::suspend::lock_suspended_execution(
                &mut *tx,
                sandbox_id,
                expected_generation.get(),
                expected_pod_uid.as_str(),
                ActiveExecutionState::Suspended.as_str(),
            )
            .await
            .db_context("failed to lock suspended Kubernetes execution")?;
        let Some(execution_active) = execution_active else {
            return Err(SubstrateError::Unavailable {
                message: "sandbox execution changed before suspended pod retirement".to_string(),
            });
        };
        let already_finalized = lifecycle == SandboxLifecycle::Suspended.as_str()
            && !execution_active
            && !pplx_space_sandbox_store::k8s_executions::successor_exists(
                &mut *tx,
                sandbox_id,
                expected_generation.get(),
            )
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
            pplx_space_sandbox_store::suspend::finalize_with_committed_artifact(
                &mut *tx,
                sandbox_id,
                published_artifact.id,
                published_artifact.kind,
            )
            .await
            .db_context("failed to mark suspended sandbox unplaced")?;
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

    pub(in crate::controllers::substrate::kubernetes) async fn persist_or_accept_suspend_commit(
        &self,
        sandbox_id: SandboxId,
        generation: ExecutionGeneration,
        expected_pod_uid: &PodUid,
        published_artifact: super::super::super::PublishedLocalArtifact,
    ) -> Result<(), SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start Kubernetes suspend commit transaction")?;
        let sandbox_exists = pplx_space_sandbox_store::row_locks::lock_id(&mut *tx, sandbox_id)
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
        let execution_updated = pplx_space_sandbox_store::k8s_executions::suspend::mark_committed(
            &mut *tx,
            sandbox_id,
            generation.get(),
            expected_pod_uid.as_str(),
            ActiveExecutionState::Suspended.as_str(),
            ActiveExecutionState::Running.as_str(),
        )
        .await
        .db_context("failed to mark Kubernetes execution suspend committed")?;
        if execution_updated == 0 {
            let forgotten_execution_committed =
                pplx_space_sandbox_store::k8s_executions::suspend::mark_forgotten_committed(
                    &mut *tx,
                    sandbox_id,
                    generation.get(),
                    expected_pod_uid.as_str(),
                    ActiveExecutionState::Suspended.as_str(),
                )
                .await
                .db_context("failed to commit suspend after terminal Kubernetes pod cleanup")?;
            if forgotten_execution_committed == 1 {
                pplx_space_sandbox_store::suspend::finalize_with_committed_artifact(
                    &mut *tx,
                    sandbox_id,
                    published_artifact.id,
                    published_artifact.kind,
                )
                .await
                .db_context("failed to mark suspended sandbox unplaced")?;
                tx.commit().await.db_context(
                    "failed to commit finalized Kubernetes suspend after pod cleanup",
                )?;
                return Ok(());
            }

            let exact_projection = pplx_space_sandbox_store::suspend_reads::projection_matches(
                &mut *tx,
                sandbox_id,
                pplx_space_sandbox_store::suspend_reads::ProjectedSuspend {
                    generation,
                    pod_uid: expected_pod_uid,
                    artifact_id: published_artifact.id,
                    artifact_kind: published_artifact.kind,
                    artifact_origin: ArtifactOrigin::Suspend,
                    execution_state: ActiveExecutionState::Suspended.as_str(),
                },
            )
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
        let updated = pplx_space_sandbox_store::suspend::record_committed_artifact(
            &mut *tx,
            sandbox_id,
            published_artifact.id,
            published_artifact.kind,
        )
        .await
        .db_context("failed to persist Kubernetes suspend commit")?;
        if !updated {
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
            let claimed = pplx_space_sandbox_store::suspend::claim(
                &self.pool,
                sandbox_id,
                generation,
                expected_pod_uid,
                previous,
            )
            .await
            .db_context("failed to claim Kubernetes suspend execution")?;
            if claimed {
                return Ok(SuspendExecutionClaim::Claimed);
            }
        }

        let existing = pplx_space_sandbox_store::suspend_reads::claim_held(
            &self.pool,
            sandbox_id,
            generation,
            expected_pod_uid,
        )
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
        pplx_space_sandbox_store::suspend_reads::local_backup_pending(
            &self.pool,
            sandbox_id,
            generation,
            expected_pod_uid,
        )
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
        let marker = pplx_space_sandbox_store::suspend_reads::committed_marker(
            &self.pool,
            sandbox_id,
            generation,
            expected_pod_uid,
        )
        .await
        .map_err(sqlx::Error::from)
        .and_then(|marker| {
            marker
                .map(CommittedSuspendArtifactRow::try_from)
                .transpose()
        })
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
        let row = pplx_space_sandbox_store::suspend_reads::durable_suspend_artifact(
            &self.pool,
            sandbox_id,
            generation,
            expected_pod_uid,
        )
        .await
        .db_context("failed to load durable Kubernetes suspend snapshot")?;
        row.map(|row| {
            let kind = ArtifactKind::try_from(row.artifact_kind).map_err(|error| {
                SubstrateError::Failed {
                    message: format!(
                        "durable suspend snapshot has an invalid artifact kind: {error}"
                    ),
                }
            })?;
            Ok(super::super::super::PublishedLocalArtifact {
                id: LocalArtifactId::from_uuid(row.local_artifact_id),
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
        let suspending =
            pplx_space_sandbox_store::suspend_reads::lifecycle_is_suspending(&mut **tx, sandbox_id)
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

        let clear_marker =
            pplx_space_sandbox_store::suspend_reads::missing_suspending_pod_clear_marker(
                &mut *tx, sandbox_id, generation, pod_uid,
            )
            .await
            .db_context("failed to select missing suspended pod finalization")?;
        let Some(clear_marker) = clear_marker else {
            return Ok(false);
        };

        self.finish_active_execution_in(&mut tx, sandbox_id, SandboxLifecycle::Suspended.as_str())
            .await?;

        pplx_space_sandbox_store::suspend::finalize_missing_pod_from_current_snapshot(
            &mut *tx,
            sandbox_id,
            clear_marker,
        )
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

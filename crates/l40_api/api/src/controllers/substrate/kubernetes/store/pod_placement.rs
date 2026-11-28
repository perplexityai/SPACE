//! Pod metadata and node placement persisted onto the active execution row.

use super::*;
use pplx_space_accounts::ClientId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::controllers::substrate::kubernetes) enum FencedPodMetadataOutcome {
    Applied(ActiveExecutionState),
    Retired,
    Superseded,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::controllers::substrate::kubernetes) struct PodMetadataFence<'a> {
    pub expected_pod_uid: Option<&'a str>,
    pub expected_pod_ip: Option<&'a str>,
    pub expected_pod_materialized_at_set: bool,
}

fn tracked_pod_matches_fence(
    tracked: &super::runtime_attribution::TrackedPod,
    fence: PodMetadataFence<'_>,
    new_pod_uid: Option<&str>,
    new_pod_ip: Option<&str>,
) -> bool {
    let expected_pod_uid = normalize_pod_uid(fence.expected_pod_uid);
    let uid_matches = tracked.pod_uid.as_deref() == expected_pod_uid
        && (expected_pod_uid.is_some()
            || tracked.pod_materialized_at_set == fence.expected_pod_materialized_at_set);
    let ip_matches = tracked.pod_ip.as_deref() == fence.expected_pod_ip
        || (new_pod_ip.is_some() && tracked.pod_ip.as_deref() == new_pod_ip);
    (uid_matches && ip_matches)
        || (new_pod_uid.is_some()
            && tracked.pod_uid.as_deref() == new_pod_uid
            && tracked.pod_ip.as_deref() == new_pod_ip)
}

impl K8sStore {
    /// Persist pod metadata onto the active execution row of `generation`.
    ///
    /// Returns the row's **resulting** state — read back under the same row
    /// lock the update ran under — or `None` when the write was a no-op
    /// because no active row of that generation existed (retired or replaced
    /// by another generation, including a retirement that committed while
    /// this call waited on the row lock). Callers that treat "metadata
    /// persisted" as evidence (e.g. the sweeper's execution hydration) must
    /// branch on the returned state instead of assuming the write landed:
    /// a `recovering` row keeps its state by design (the recovery flow owns
    /// the promote to `running`), so its result is `Some(Recovering)`, not a
    /// routable execution.
    ///
    /// `pod_client` is the observed pod's `space.pplx.net/client-id` label.
    /// When the write moves the row to a different pod, or the record lags a
    /// labeled pod's label, the sandbox's record is set to it in the same
    /// transaction (see `runtime_attribution`).
    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let pod_uid = normalize_pod_uid(pod_uid);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod metadata transaction")?;

        let Some(tracked) = self
            .lock_tracked_pod_in(&mut tx, sandbox_id, generation)
            .await?
        else {
            return Ok(None);
        };
        if self.record_after(&tracked, pod_uid, pod_client).is_some() {
            // The record has to change, which needs the `sandboxes` row lock
            // taken before the execution row's: start over in that order,
            // releasing this row lock first.
            tx.rollback()
                .await
                .db_context("failed to release the pod metadata row lock")?;
            return self
                .persist_attributed_pod_metadata(
                    sandbox_id, generation, pod_uid, pod_ip, pod_client,
                )
                .await;
        }
        let resulting_state = self
            .write_tracked_pod_in(
                &mut tx, sandbox_id, generation, &tracked, pod_uid, pod_ip, pod_client,
            )
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit pod metadata transaction")?;
        Ok(Some(resulting_state))
    }

    /// [`Self::persist_pod_metadata`] fenced on the pre-lookup execution-row
    /// snapshot (`expected_pod_uid`, `expected_pod_ip`, and whether the row had
    /// already materialized a pod). Prevents an in-flight fallback or cache read
    /// for pod A from overwriting a same-generation replacement pod B (including
    /// across a same-generation reset that cleared `pod_uid`/`pod_ip` while
    /// preserving `pod_materialized_at`) before this write acquired the row lock.
    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_metadata_if_unchanged(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        fence: PodMetadataFence<'_>,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Result<FencedPodMetadataOutcome, SubstrateError> {
        let pod_uid = normalize_pod_uid(pod_uid);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start fenced pod metadata transaction")?;

        let Some(tracked) = self
            .lock_tracked_pod_in(&mut tx, sandbox_id, generation)
            .await?
        else {
            return Ok(FencedPodMetadataOutcome::Retired);
        };
        if !tracked_pod_matches_fence(&tracked, fence, pod_uid, pod_ip) {
            return Ok(FencedPodMetadataOutcome::Superseded);
        }
        if self.record_after(&tracked, pod_uid, pod_client).is_some() {
            tx.rollback()
                .await
                .db_context("failed to release the fenced pod metadata row lock")?;
            return self
                .persist_attributed_pod_metadata_if_unchanged(
                    sandbox_id, generation, fence, pod_uid, pod_ip, pod_client,
                )
                .await;
        }
        let resulting_state = self
            .write_tracked_pod_in(
                &mut tx, sandbox_id, generation, &tracked, pod_uid, pod_ip, pod_client,
            )
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit fenced pod metadata transaction")?;
        Ok(FencedPodMetadataOutcome::Applied(resulting_state))
    }

    /// Record the address the pod's node reported or published for `pod_uid`,
    /// once confirmed to serve the execution there. Only a `created`,
    /// `running`, or `recovering` row of `generation` that records `pod_uid`
    /// and no address takes it (`created` becomes `running`, while `running`
    /// and `recovering` keep their state), and a `running` or `recovering` row
    /// that already records it is kept without taking a row lock. Every other
    /// row, including one a suspend or retire has moved on, is left as it is.
    /// Returns whether the row records the address.
    pub(in crate::controllers::substrate::kubernetes) async fn record_confirmed_pod_ip(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: &str,
        pod_ip: PodIp,
    ) -> Result<bool, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::pod_placement::record_confirmed_pod_ip(
            &self.pool,
            sandbox_id,
            generation,
            pod_uid,
            &pod_ip.to_string(),
        )
        .await
        .db_context("failed to record the confirmed pod IP")
    }

    /// Repair a missing `pod_uid` on an active `'creating'` execution row of
    /// `generation`, promoting it to `'created'` without overwriting any UID,
    /// IP, or state written by a concurrent materializer or same-generation reset.
    pub(in crate::controllers::substrate::kubernetes) async fn repair_creating_pod_uid(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: &str,
        observed_updated_at: time::OffsetDateTime,
        pod_client: Option<&ClientId>,
    ) -> Result<bool, SubstrateError> {
        let Some(pod_uid) = normalize_pod_uid(Some(pod_uid)) else {
            return Ok(false);
        };
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start creating pod metadata repair transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }
        let repaired =
            pplx_space_sandbox_store::k8s_executions::materialize::repair_creating_pod_uid(
                &mut *tx,
                sandbox_id,
                generation,
                pod_uid,
                observed_updated_at,
            )
            .await
            .db_context("failed to repair creating pod metadata")?;
        if repaired
            && self.attribution.is_enabled()
            && let Some(tracked) = self
                .lock_tracked_pod_in(&mut tx, sandbox_id, generation)
                .await?
            && let Some(sandbox) = tracked.sandbox.as_ref()
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
            .db_context("failed to commit creating pod metadata repair transaction")?;
        Ok(repaired)
    }

    /// [`Self::persist_pod_metadata`]'s path for a write that also changes the
    /// sandbox's record: `sandboxes` is locked first, then the execution row,
    /// and both are re-read before writing.
    async fn persist_attributed_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod attribution transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(None);
        }
        let Some(tracked) = self
            .lock_tracked_pod_in(&mut tx, sandbox_id, generation)
            .await?
        else {
            tx.commit()
                .await
                .db_context("failed to commit pod attribution transaction")?;
            return Ok(None);
        };
        let resulting_state = self
            .write_tracked_pod_in(
                &mut tx, sandbox_id, generation, &tracked, pod_uid, pod_ip, pod_client,
            )
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit pod attribution transaction")?;
        Ok(Some(resulting_state))
    }

    async fn persist_attributed_pod_metadata_if_unchanged(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        fence: PodMetadataFence<'_>,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Result<FencedPodMetadataOutcome, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start fenced pod attribution transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(FencedPodMetadataOutcome::Retired);
        }
        let Some(tracked) = self
            .lock_tracked_pod_in(&mut tx, sandbox_id, generation)
            .await?
        else {
            tx.commit()
                .await
                .db_context("failed to commit fenced pod attribution transaction")?;
            return Ok(FencedPodMetadataOutcome::Retired);
        };
        if !tracked_pod_matches_fence(&tracked, fence, pod_uid, pod_ip) {
            tx.commit()
                .await
                .db_context("failed to commit fenced pod attribution transaction")?;
            return Ok(FencedPodMetadataOutcome::Superseded);
        }
        let resulting_state = self
            .write_tracked_pod_in(
                &mut tx, sandbox_id, generation, &tracked, pod_uid, pod_ip, pod_client,
            )
            .await?;
        tx.commit()
            .await
            .db_context("failed to commit fenced pod attribution transaction")?;
        Ok(FencedPodMetadataOutcome::Applied(resulting_state))
    }

    /// Write the pod metadata onto the active execution row `tracked` was read
    /// from, set the sandbox's record when the write moves it to a different
    /// pod (see `runtime_attribution`), and return the row's resulting state.
    /// The caller holds the execution-row lock, and the `sandboxes` row lock
    /// whenever the record has to change.
    #[allow(
        clippy::too_many_arguments,
        reason = "the transaction, row key, tracked pod and observed metadata are all distinct"
    )]
    async fn write_tracked_pod_in(
        &self,
        tx: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        tracked: &super::runtime_attribution::TrackedPod,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Result<ActiveExecutionState, SubstrateError> {
        let pod_ip = super::super::support::pod_ip_or_recorded(
            pod_ip,
            pod_uid,
            tracked.pod_uid.as_deref(),
            tracked.pod_ip.as_deref(),
        );
        self.update_active_execution_pod_metadata(tx, sandbox_id, generation, pod_uid, pod_ip)
            .await?;
        if let Some(record) = self.record_after(tracked, pod_uid, pod_client) {
            Self::attribute_runtime_in(tx, sandbox_id, record.as_ref()).await?;
        }
        // Re-read the resulting state inside the transaction (sees our own
        // update; the row is still locked, so it cannot change underneath).
        Self::lock_active_execution_state_in(tx, sandbox_id, generation)
            .await?
            .ok_or_else(|| SubstrateError::Failed {
                message: "locked active Kubernetes execution row disappeared mid-transaction"
                    .to_string(),
            })
    }

    /// Refresh the dial address only while the complete selected route still
    /// owns the active execution. Unlike `persist_pod_metadata`, this must
    /// never adopt a different Pod UID: callers use the result immediately to
    /// send an authenticated node-api request.
    ///
    /// A `created` row becomes `running`, as it does in `persist_pod_metadata`
    /// once its pod has an IP. A start of a stopped sandbox learns its pod's
    /// IP only here, and suspend claims only a `running` execution. Every
    /// other state is kept.
    pub(in crate::controllers::substrate::kubernetes) async fn persist_selected_route_pod_ip(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
        route_activation_id: SandboxActivationAttemptId,
        pod_uid: &PodUid,
        pod_ip: PodIp,
    ) -> Result<bool, SubstrateError> {
        pplx_space_sandbox_store::k8s_executions::pod_placement::refresh_selected_route_pod_ip(
            &self.pool,
            sandbox_id,
            generation,
            pod_name,
            route_activation_id,
            pod_uid.as_str(),
            &pod_ip.to_string(),
        )
        .await
        .map_err(|err| SubstrateError::Failed {
            message: format!("failed to persist selected Kubernetes route pod IP: {err}"),
        })
    }

    pub(in crate::controllers::substrate::kubernetes) async fn claim_active_execution_pod_materialization(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_lifecycle: SandboxLifecycle,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod materialization transaction")?;

        Self::lock_sandbox_for_execution_lifecycle_in(&mut tx, sandbox_id, expected_lifecycle)
            .await?;

        // Clearing the pod UID stamps `pod_materialized_at` first if the row
        // held one, so the record that a pod ran outlives the UID.
        let claimed =
            pplx_space_sandbox_store::k8s_executions::pod_placement::claim_pod_materialization(
                &mut *tx, sandbox_id, generation,
            )
            .await
            .db_context("failed to claim pod materialization")?;

        tx.commit()
            .await
            .db_context("failed to commit pod materialization transaction")?;
        Ok(claimed)
    }

    pub(in crate::controllers::substrate::kubernetes) async fn persist_materialized_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_lifecycle: SandboxLifecycle,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        pod_client: Option<&ClientId>,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let pod_uid = normalize_pod_uid(pod_uid);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod materialization transaction")?;

        let lifecycle = Self::lock_sandbox_execution_lifecycle_in(&mut tx, sandbox_id).await?;
        let tracked = if lifecycle == Some(expected_lifecycle) {
            self.lock_tracked_pod_in(&mut tx, sandbox_id, generation)
                .await?
        } else {
            None
        };
        let Some(tracked) = tracked else {
            tx.commit()
                .await
                .db_context("failed to commit pod materialization transaction")?;
            return Ok(None);
        };

        // The `sandboxes` row is already locked, so the record is written in
        // place.
        let resulting_state = self
            .write_tracked_pod_in(
                &mut tx, sandbox_id, generation, &tracked, pod_uid, pod_ip, pod_client,
            )
            .await?;

        tx.commit()
            .await
            .db_context("failed to commit pod materialization transaction")?;
        Ok(Some(resulting_state))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_node_placement(
        &self,
        placement: PodNodePlacement<'_>,
    ) -> Result<(), SubstrateError> {
        let PodNodePlacement {
            sandbox_id,
            node_name,
            node_uid,
            node_id,
            role,
            address,
            instance_id,
        } = placement;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod node placement transaction")?;

        self.upsert_k8s_node_role(
            &mut tx,
            K8sNodeRoleUpsert {
                node_name,
                node_uid,
                node_id,
                role,
                address,
                state: NodeStateWrite::InsertOnly(NodeLifecycleState::Registered),
                instance_id,
            },
        )
        .await?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await?;
        if sandbox_exists {
            let active_execution = self
                .update_active_execution_node_name(&mut tx, sandbox_id, node_name)
                .await?;

            if active_execution {
                pplx_space_sandbox_store::wake::record_pod_node_placement(
                    &mut *tx, sandbox_id, node_id,
                )
                .await
                // Classified like every sibling in this transaction; the
                // post-side-effect demotion is applied to the whole function's
                // result below.
                .db_context("failed to persist sandbox Kubernetes placement")?;
            }
        }

        tx.commit()
            .await
            .db_context("failed to commit pod node placement transaction")?;
        Ok(())
    }

    /// [`Self::persist_pod_node_placement`], demoting the whole result when the
    /// calling operation's retry would not converge.
    ///
    /// This function only ever runs once the pod exists — recording its node
    /// placement is the point. Where the operation that reached here mints a
    /// fresh `SandboxId` on retry, *nothing* it does may answer retryable,
    /// including the transaction's own `begin` and `commit`. Wrapping the
    /// result once is what makes that true of all five statements inside;
    /// exempting only the `UPDATE` left `begin`, the role upsert, the row lock,
    /// and the `commit` telling the caller to retry a create whose pod is
    /// already live.
    ///
    /// The disposition is the caller's to state because it is a property of the
    /// operation, not of these statements: `start`/`wake`/`access` re-drive a
    /// sandbox the caller already names, so their retry converges on the same
    /// row and a bounded lock wait should stay a 503. Demoting those too would
    /// turn the bound this change exists to make safe back into a 500.
    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_node_placement_after_pod_exists(
        &self,
        placement: PodNodePlacement<'_>,
        retry: PlacementRetryDisposition,
    ) -> Result<(), SubstrateError> {
        let result = self.persist_pod_node_placement(placement).await;
        match retry {
            PlacementRetryDisposition::RetryDuplicatesExecution => {
                result.map_err(SubstrateError::demoted_after_side_effect)
            }
            PlacementRetryDisposition::RetryConverges => result,
        }
    }

    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_node_placement_fenced_after_pod_exists(
        &self,
        placement: PodNodePlacement<'_>,
        generation: i64,
        expected_pod_uid: Option<&str>,
        retry: PlacementRetryDisposition,
    ) -> Result<Option<time::OffsetDateTime>, SubstrateError> {
        let expected_pod_uid = normalize_pod_uid(expected_pod_uid);
        let result = self
            .persist_pod_node_placement_fenced(placement, generation, expected_pod_uid)
            .await;
        match retry {
            PlacementRetryDisposition::RetryDuplicatesExecution => {
                result.map_err(SubstrateError::demoted_after_side_effect)
            }
            PlacementRetryDisposition::RetryConverges => result,
        }
    }

    async fn persist_pod_node_placement_fenced(
        &self,
        placement: PodNodePlacement<'_>,
        generation: i64,
        expected_pod_uid: Option<&str>,
    ) -> Result<Option<time::OffsetDateTime>, SubstrateError> {
        let PodNodePlacement {
            sandbox_id,
            node_name,
            node_uid,
            node_id,
            role,
            address,
            instance_id,
        } = placement;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start fenced pod node placement transaction")?;

        self.upsert_k8s_node_role(
            &mut tx,
            K8sNodeRoleUpsert {
                node_name,
                node_uid,
                node_id,
                role,
                address,
                state: NodeStateWrite::InsertOnly(NodeLifecycleState::Registered),
                instance_id,
            },
        )
        .await?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await?;
        let execution_updated_at = if sandbox_exists {
            let updated_at = self
                .update_active_execution_node_name_fenced(
                    &mut tx,
                    sandbox_id,
                    generation,
                    expected_pod_uid,
                    node_name,
                )
                .await?;
            if updated_at.is_some() {
                pplx_space_sandbox_store::wake::record_pod_node_placement(
                    &mut *tx, sandbox_id, node_id,
                )
                .await
                .db_context("failed to persist sandbox Kubernetes placement")?;
            }
            updated_at
        } else {
            None
        };

        tx.commit()
            .await
            .db_context("failed to commit fenced pod node placement transaction")?;
        Ok(execution_updated_at)
    }
}

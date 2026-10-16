//! Pod metadata and node placement persisted onto the active execution row.

use super::*;

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
    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let pod_uid = normalize_pod_uid(pod_uid);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod metadata transaction")?;

        if Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        self.update_active_execution_pod_metadata(&mut tx, sandbox_id, generation, pod_uid, pod_ip)
            .await?;
        // Re-read the resulting state inside the transaction (sees our own
        // update; the row is still locked, so it cannot change underneath).
        let resulting_state = Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
            .await?
            .ok_or_else(|| SubstrateError::Failed {
                message: "locked active Kubernetes execution row disappeared mid-transaction"
                    .to_string(),
            })?;

        tx.commit()
            .await
            .db_context("failed to commit pod metadata transaction")?;
        Ok(Some(resulting_state))
    }

    /// Refresh the dial address only while the complete selected route still
    /// owns the active execution. Unlike `persist_pod_metadata`, this must
    /// never adopt a different Pod UID: callers use the result immediately to
    /// send an authenticated node-api request.
    pub(in crate::controllers::substrate::kubernetes) async fn persist_selected_route_pod_ip(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_name: &str,
        route_activation_id: SandboxActivationAttemptId,
        pod_uid: &PodUid,
        pod_ip: PodIp,
    ) -> Result<bool, SubstrateError> {
        let result = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET pod_ip = $1::INET,
                 updated_at = CASE
                     WHEN pod_ip IS DISTINCT FROM $1::INET THEN NOW()
                     ELSE updated_at
                 END
             WHERE sandbox_id = $2
               AND generation = $3
               AND active
               AND pod_name = $4
               AND COALESCE(route_activation_id, activation_attempt_id) = $5
               AND NULLIF(BTRIM(pod_uid), '') = $6
             RETURNING 1"
        ))
        .bind(pod_ip.to_string())
        .bind(sandbox_id)
        .bind(generation)
        .bind(pod_name)
        .bind(route_activation_id.as_uuid())
        .bind(pod_uid.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| SubstrateError::Failed {
            message: format!("failed to persist selected Kubernetes route pod IP: {err}"),
        })?;
        Ok(result.is_some())
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

        let claimed = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
             SET state = CASE WHEN state = 'recovering' THEN state ELSE 'creating' END,
                 pod_uid = NULL,
                 pod_ip = NULL,
                 updated_at = NOW()
             WHERE sandbox_id = $1 AND generation = $2 AND active
             RETURNING 1"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .fetch_optional(&mut *tx)
        .await
        .db_context("failed to claim pod materialization")?
        .is_some();

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
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let pod_uid = normalize_pod_uid(pod_uid);
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod materialization transaction")?;

        let lifecycle = Self::lock_sandbox_execution_lifecycle_in(&mut tx, sandbox_id).await?;
        if lifecycle != Some(expected_lifecycle)
            || Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
                .await?
                .is_none()
        {
            tx.commit()
                .await
                .db_context("failed to commit pod materialization transaction")?;
            return Ok(None);
        }

        self.update_active_execution_pod_metadata(&mut tx, sandbox_id, generation, pod_uid, pod_ip)
            .await?;
        let resulting_state = Self::lock_active_execution_state_in(&mut tx, sandbox_id, generation)
            .await?
            .ok_or_else(|| SubstrateError::Failed {
                message: "locked active Kubernetes execution row disappeared mid-transaction"
                    .to_string(),
            })?;

        tx.commit()
            .await
            .db_context("failed to commit pod materialization transaction")?;
        Ok(Some(resulting_state))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_node_placement(
        &self,
        sandbox_id: SandboxId,
        node_name: &str,
        node_uid: &str,
        node_id: NodeId,
        role: NodeRole,
        address: Option<&str>,
    ) -> Result<(), SubstrateError> {
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
            },
        )
        .await?;

        // Termination fence. This writer was the one already *ordered* against
        // the terminate path — the `nodes` upsert above locks the same row — and
        // the one that never looked at the answer, so a phase write that won the
        // lock left this transaction free to attach a `started` sandbox to a
        // `drained` or `terminating` node. Ordering without a check buys nothing.
        //
        // It sits after the upsert on purpose: the upsert already holds this
        // row's lock, so the fence is a read of the committed phase rather than
        // a second lock acquisition, and it still precedes the `sandboxes` lock
        // below — the node-then-sandbox order every fenced writer shares.
        //
        // The whole transaction rolls back. The retryable error is
        // load-bearing: the pod may already be running, so a fresh restore must
        // stay `restoring` until its retry or the existing cleanup machinery
        // confirms that pod is gone. A terminal conflict would revert the row to
        // `suspended`, which no pod sweeper owns, and hide the live execution.
        // Through the shared helper, not `fence_node_for_placement` directly, for
        // its already-attached carve-out. This writer re-runs over already-placed
        // rows on every pod watch event, and refusing one of those would make the
        // caller tear down a live sandbox that some earlier placement legitimately
        // put there.
        //
        // The node name is passed, not just the sandbox id, and that is what
        // keeps this carve-out sound. A `started` sandbox keeps `active_node_id`
        // across an execution replacement, so a node-id match alone would read a
        // *fresh* recovery execution landing on the old node as a re-statement of
        // a placement that belonged to its predecessor — and because the `UPDATE`
        // below no-ops when all three columns already match, nothing would
        // refresh `placement_changed_at` to re-arm the census discount that let
        // the drain complete in the first place. Requiring the sandbox's active
        // `sandbox_k8s_executions` row to already name this node distinguishes
        // the two: a re-fired watch event for the running execution matches, a
        // replacement does not.
        //
        // Note what the carve-out still does *not* promise: the `UPDATE` below
        // no-ops only when `active_node_id`, `target_node_id` and `placement` all
        // already match, while the fence's subject is `active_node_id` alone. So a
        // row whose `active_node_id` matches — with the execution agreeing — takes
        // the carve-out even though this statement may still set `target_node_id`
        // and `placement`. The effect is to pin an already-resident sandbox
        // slightly harder to a node it is already on, not to place a new one.
        let fence = check_placement_fence_for_execution(
            &mut tx,
            node_id,
            sandbox_id,
            Writer::PodPlacement,
            Some(node_name),
        )
        .await
        .db_context("failed to check the pod-placement fence")?;
        if fence.refuses() {
            return Err(SubstrateError::restore_in_flight(fence_refusal_message(
                node_id, fence,
            )));
        }

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await?;
        if sandbox_exists {
            let execution_node = self
                .update_active_execution_node_name(&mut tx, sandbox_id, node_name)
                .await?;

            if execution_node.active_execution {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                     SET active_node_id = $1,
                         target_node_id = $1,
                         placement = 'on_node',
                         placement_changed_at = NOW()
                     WHERE id = $2
                       AND (
                         $3::BOOL
                         OR active_node_id IS DISTINCT FROM $1
                         OR target_node_id IS DISTINCT FROM $1
                         OR placement IS DISTINCT FROM 'on_node'
                       )"
                ))
                .bind(node_id)
                .bind(sandbox_id)
                // A replacement can land on the same node all three placement
                // columns already name. Refreshing the clock in that case keeps
                // the node census from treating the new execution as an old,
                // absent one while drain completion waits on this transaction.
                .bind(execution_node.node_name_changed)
                .execute(&mut *tx)
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
        let PodNodePlacement {
            sandbox_id,
            node_name,
            node_uid,
            node_id,
            role,
            address,
        } = placement;
        let result = self
            .persist_pod_node_placement(sandbox_id, node_name, node_uid, node_id, role, address)
            .await;
        match retry {
            PlacementRetryDisposition::RetryDuplicatesExecution => {
                result.map_err(SubstrateError::demoted_after_side_effect)
            }
            PlacementRetryDisposition::RetryConverges => result,
        }
    }
}

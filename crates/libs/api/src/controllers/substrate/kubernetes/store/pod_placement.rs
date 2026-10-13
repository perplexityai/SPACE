//! Pod metadata and node placement persisted onto the active execution row.

use super::*;

#[derive(sqlx::FromRow)]
struct PlacementNodeRecord {
    node_uid: String,
    address: Option<String>,
    eligible: bool,
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
    pub(in crate::controllers::substrate::kubernetes) async fn persist_observed_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        materialization_id: Option<pplx_space_common_core::types::PodMaterializationId>,
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
        if !Self::lock_matching_pod_materialization(
            &mut tx,
            sandbox_id,
            generation,
            materialization_id,
            pod_uid,
        )
        .await?
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

    // Fixture convenience only: production observations must supply the pod's
    // token, never read the expected token from the database and echo it back.
    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
    ) -> Result<Option<ActiveExecutionState>, SubstrateError> {
        let id = sqlx::query_scalar(sql!("SELECT materialization_id FROM sandbox_k8s_executions WHERE sandbox_id=$1 AND generation=$2"))
            .bind(sandbox_id).bind(generation).fetch_optional(&self.pool).await
            .db_context("read fixture materialization")?.flatten();
        self.persist_observed_pod_metadata(sandbox_id, generation, pod_uid, pod_ip, id)
            .await
    }

    async fn lock_matching_pod_materialization(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
        generation: i64,
        materialization_id: Option<pplx_space_common_core::types::PodMaterializationId>,
        pod_uid: Option<&str>,
    ) -> Result<bool, SubstrateError> {
        // New materializations adopt exactly one UID with the authorized token.
        // Legacy rows may adopt once while still in their pre-token lifetime;
        // they never gain a historical node UID from this observation.
        Ok(sqlx::query_scalar::<_, bool>(sql!(
            "SELECT TRUE FROM sandbox_k8s_executions
             WHERE sandbox_id=$1 AND generation=$2 AND active
               AND $4::TEXT IS NOT NULL
               AND (NULLIF(BTRIM(pod_uid), '') IS NULL OR pod_uid=$4)
               AND ((materialization_id IS NOT NULL AND materialization_id=$3)
                    OR (materialization_id IS NULL AND $3::UUID IS NULL))
             FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(generation)
        .bind(materialization_id)
        .bind(pod_uid)
        .fetch_optional(conn)
        .await
        .db_context("lock pod materialization identity")?
        .unwrap_or(false))
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

    /// Authorize cleanup of a pre-token create whose response was lost. The
    /// caller must verify the pod's sandbox/generation labels and delete with
    /// its observed UID precondition. This does not establish node provenance.
    pub(in crate::controllers::substrate::kubernetes) async fn legacy_cleanup_matches(
        &self,
        sandbox_id: SandboxId,
        expected: &SandboxExecutionRow,
    ) -> Result<bool, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("begin legacy cleanup check")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }
        let matches = sqlx::query_scalar::<_, bool>(sql!(
            "SELECT TRUE FROM sandbox_k8s_executions
             WHERE sandbox_id=$1 AND generation=$2 AND active
               AND materialization_id IS NULL AND NULLIF(BTRIM(pod_uid), '') IS NULL
               AND pod_name=$3 AND state=$4
               AND activation_attempt_id IS NOT DISTINCT FROM $5
               AND route_activation_id IS NOT DISTINCT FROM $6
             FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(expected.generation)
        .bind(&expected.pod_name)
        .bind(expected.state.as_str())
        .bind(expected.activation_attempt_id)
        .bind(expected.route_activation_id)
        .fetch_optional(&mut *tx)
        .await
        .db_context("check legacy cleanup claim")?
        .unwrap_or(false);
        Ok(matches)
    }

    /// Prove an observed token cannot belong to the current materialization.
    /// The expected claim must still be current. Tokens are never reused or
    /// downgraded within a generation, so releasing this lock cannot make the
    /// foreign token adoptable. The caller must match sandbox/generation/name
    /// and delete with both observed UID and resource-version preconditions.
    pub(in crate::controllers::substrate::kubernetes) async fn obsolete_materialization_matches(
        &self,
        sandbox_id: SandboxId,
        expected: &SandboxExecutionRow,
        observed: Option<pplx_space_common_core::types::PodMaterializationId>,
    ) -> Result<bool, SubstrateError> {
        if expected.materialization_id.is_none() || observed == expected.materialization_id {
            return Ok(false);
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("begin obsolete materialization check")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(false);
        }
        Ok(sqlx::query_scalar::<_, bool>(sql!(
            "SELECT TRUE FROM sandbox_k8s_executions
             WHERE sandbox_id=$1 AND generation=$2 AND active
               AND materialization_id=$3 AND pod_name=$4
               AND activation_attempt_id IS NOT DISTINCT FROM $5
               AND route_activation_id IS NOT DISTINCT FROM $6
             FOR UPDATE"
        ))
        .bind(sandbox_id)
        .bind(expected.generation)
        .bind(expected.materialization_id)
        .bind(&expected.pod_name)
        .bind(expected.activation_attempt_id)
        .bind(expected.route_activation_id)
        .fetch_optional(&mut *tx)
        .await
        .db_context("check obsolete materialization")?
        .unwrap_or(false))
    }

    pub(in crate::controllers::substrate::kubernetes) async fn claim_pod_materialization(
        &self,
        sandbox_id: SandboxId,
        expected: &SandboxExecutionRow,
        expected_lifecycle: SandboxLifecycle,
    ) -> Result<Option<ClaimedMaterialization>, SubstrateError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("start materialization claim")?;
        Self::lock_sandbox_for_execution_lifecycle_in(&mut tx, sandbox_id, expected_lifecycle)
            .await?;
        // Preserve a legacy generation through rollout, including unresolved
        // creates. A pool-bound Pod/Node identity is the one token-less lifetime
        // eligible for a fenced successor after confirmed predecessor removal.
        // Check that evidence atomically before clearing the old binding. Never
        // upgrade an unresolved create, and reuse the token on successor retries.
        let next = if expected.observed_pod_uid.is_none() {
            expected.materialization_id
        } else {
            expected
                .materialization_id
                .map(|_| pplx_space_common_core::types::PodMaterializationId::new())
        };
        let claimed = sqlx::query_as(sql!(
            "UPDATE sandbox_k8s_executions
             SET state=CASE WHEN state='recovering' THEN state ELSE 'creating' END,
                 pod_uid=NULL, pod_ip=NULL, k8s_node_name=NULL, k8s_node_uid=NULL,
                 materialization_id=CASE
                     WHEN $8 AND k8s_node_name IS NOT NULL AND k8s_node_uid IS NOT NULL
                     THEN $9 ELSE $5 END,
                 updated_at=NOW()
             WHERE sandbox_id=$1 AND generation=$2 AND active
               AND materialization_id IS NOT DISTINCT FROM $3
               AND NULLIF(BTRIM(pod_uid), '') IS NOT DISTINCT FROM $4
               AND activation_attempt_id IS NOT DISTINCT FROM $6
               AND route_activation_id IS NOT DISTINCT FROM $7
             RETURNING materialization_id, state"
        ))
        .bind(sandbox_id)
        .bind(expected.generation)
        .bind(expected.materialization_id)
        .bind(&expected.observed_pod_uid)
        .bind(next)
        .bind(expected.activation_attempt_id)
        .bind(expected.route_activation_id)
        .bind(
            matches!(self.materialization_mode, MaterializationMode::Fenced)
                && expected.materialization_id.is_none()
                && expected.observed_pod_uid.is_some(),
        )
        .bind(pplx_space_common_core::types::PodMaterializationId::new())
        .fetch_optional(&mut *tx)
        .await
        .db_context("claim pod materialization")?;
        tx.commit()
            .await
            .db_context("commit materialization claim")?;
        Ok(claimed)
    }

    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) async fn claim_active_execution_pod_materialization(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        lifecycle: SandboxLifecycle,
    ) -> Result<bool, SubstrateError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .db_context("fixture materialization connection")?;
        let Some(row) = self.active_execution_row(&mut conn, sandbox_id).await? else {
            return Ok(false);
        };
        if row.generation != generation {
            return Ok(false);
        }
        drop(conn);
        Ok(self
            .claim_pod_materialization(sandbox_id, &row, lifecycle)
            .await?
            .is_some())
    }

    pub(in crate::controllers::substrate::kubernetes) async fn persist_materialized_pod_metadata(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expected_lifecycle: SandboxLifecycle,
        pod_uid: Option<&str>,
        pod_ip: Option<&str>,
        materialization_id: Option<pplx_space_common_core::types::PodMaterializationId>,
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

        if !Self::lock_matching_pod_materialization(
            &mut tx,
            sandbox_id,
            generation,
            materialization_id,
            pod_uid,
        )
        .await?
        {
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

    #[cfg(test)]
    pub(in crate::controllers::substrate::kubernetes) async fn persist_pod_node_placement(
        &self,
        sandbox_id: SandboxId,
        node_name: &str,
        node_uid: &str,
        node_id: NodeId,
        role: NodeRole,
        address: Option<&str>,
    ) -> Result<(), SubstrateError> {
        let pod_identity: Option<(String, Option<String>, Option<pplx_space_common_core::types::PodMaterializationId>)> = sqlx::query_as(sql!(
            "SELECT pod_name, pod_uid, materialization_id FROM sandbox_k8s_executions WHERE sandbox_id=$1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_optional(&self.pool)
        .await
        .db_context("read fixture execution pod name")?;
        let (pod_name, pod_uid, materialization_id) = pod_identity.unwrap_or_default();
        self.persist_observed_pod_node_placement(PodNodePlacement {
            sandbox_id,
            pod_name: &pod_name,
            pod_uid: pod_uid.as_deref(),
            materialization_id,
            node_name,
            node_uid,
            node_id,
            role,
            address,
        })
        .await
    }

    async fn persist_observed_pod_node_placement(
        &self,
        placement: PodNodePlacement<'_>,
    ) -> Result<(), SubstrateError> {
        if self
            .try_persist_observed_pod_node_placement(placement)
            .await?
        {
            return Ok(());
        }
        self.persist_k8s_node_inventory(
            placement.node_name,
            placement.node_uid,
            placement.node_id,
            placement.role,
            placement.address,
            NodeStateWrite::InsertOnly(NodeLifecycleState::Registered),
        )
        .await?;
        // Inventory can change again between transactions. Revalidate once;
        // ordinary reconciliation owns any further retry.
        if self
            .try_persist_observed_pod_node_placement(placement)
            .await?
        {
            Ok(())
        } else {
            Err(SubstrateError::Unavailable {
                message: "node inventory changed while recording pod placement; retry".into(),
            })
        }
    }

    async fn try_persist_observed_pod_node_placement(
        &self,
        placement: PodNodePlacement<'_>,
    ) -> Result<bool, SubstrateError> {
        let PodNodePlacement {
            sandbox_id,
            pod_name,
            pod_uid,
            materialization_id,
            node_name,
            node_uid,
            node_id,
            role,
            address,
        } = placement;
        let mut tx = self
            .pool
            .begin()
            .await
            .db_context("failed to start pod node placement transaction")?;

        let sandbox_exists =
            Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await?;

        // Fence first identity capture to the observed pod, while holding the
        // sandbox lock that serializes retirement and successor creation.
        if sandbox_exists {
            let active_pod: Option<(
                String,
                Option<String>,
                Option<pplx_space_common_core::types::PodMaterializationId>,
            )> = sqlx::query_as(sql!(
                "SELECT pod_name, NULLIF(BTRIM(pod_uid), ''), materialization_id
                 FROM sandbox_k8s_executions WHERE sandbox_id=$1 AND active FOR UPDATE"
            ))
            .bind(sandbox_id)
            .fetch_optional(&mut *tx)
            .await
            .db_context("lock observed pod execution")?;
            if let Some((name, uid, token)) = active_pod
                && (name != pod_name
                    || uid.as_deref().is_some_and(|uid| Some(uid) != pod_uid)
                    || (token.is_some() && token != materialization_id)
                    || (token.is_none() && materialization_id.is_some()))
            {
                return Err(SubstrateError::Unavailable {
                    message: "observed pod no longer owns the active execution".to_string(),
                });
            }
        }

        // Only shared registry locks may coexist with the sandbox lock. The
        // terminator locks the registry before the sandbox; an upsert here
        // would invert that order and deadlock.
        let registered: Option<PlacementNodeRecord> = sqlx::query_as(sql!(
            "SELECT k.k8s_node_uid AS node_uid, host(n.address) AS address,
                    (n.managed_by='k8s' AND n.role=$3 AND n.state<>'deleted') AS eligible
             FROM nodes n JOIN node_k8s k ON k.node_id=n.id
             WHERE n.id=$1 AND k.k8s_node_name=$2 FOR SHARE OF n, k"
        ))
        .bind(node_id)
        .bind(node_name)
        .bind(role.as_str())
        .fetch_optional(&mut *tx)
        .await
        .db_context("revalidate placement node incarnation")?;
        if registered.as_ref().is_some_and(|node| !node.eligible) {
            return Err(SubstrateError::Unavailable {
                message: "observed placement node is no longer an eligible Kubernetes worker"
                    .into(),
            });
        }
        // Never repair inventory in response to a bound execution disagreeing
        // with this observation. That would launder a replacement's identity.
        let binding: Option<(Option<String>, Option<String>)> = sqlx::query_as(sql!(
            "SELECT k8s_node_name, k8s_node_uid FROM sandbox_k8s_executions
             WHERE sandbox_id=$1 AND active"
        ))
        .bind(sandbox_id)
        .fetch_optional(&mut *tx)
        .await
        .db_context("revalidate execution incarnation")?;
        if let Some((name, uid)) = &binding
            && ((name.as_deref().is_some_and(|name| name != node_name)
                && (uid.is_some() || materialization_id.is_some()))
                || uid.as_deref().is_some_and(|uid| uid != node_uid))
        {
            return Err(SubstrateError::Unavailable {
                message: "observed pod node differs from the execution's original node identity"
                    .into(),
            });
        }
        if registered.as_ref().is_none_or(|node| {
            node.node_uid != node_uid
                || address.is_some_and(|address| Some(address) != node.address.as_deref())
        }) {
            if registered
                .as_ref()
                .is_some_and(|node| node.node_uid != node_uid)
                && binding.as_ref().is_some_and(|(_, uid)| uid.is_some())
            {
                return Err(SubstrateError::Unavailable {
                    message: "node registry incarnation changed after execution binding".into(),
                });
            }
            // Explicitly release sandbox/execution/registry locks before the
            // caller repairs missing inventory in a separate transaction.
            tx.rollback()
                .await
                .db_context("release placement locks before inventory repair")?;
            return Ok(false);
        }
        if super::node_inventory::is_node_k8s_tombstoned(&mut tx, node_id, "placement").await? {
            return Err(SubstrateError::NodeTombstoned {
                message: "observed placement node is tombstoned".into(),
            });
        }

        if sandbox_exists {
            let active_execution = self
                .update_active_execution_node_name(&mut tx, sandbox_id, node_name, node_uid)
                .await?;

            if active_execution {
                sqlx::query(sql!(
                    "UPDATE sandboxes
                     SET active_node_id = $1,
                         target_node_id = $1,
                         placement = 'on_node',
                         placement_changed_at = NOW()
                     WHERE id = $2
                       AND (
                         active_node_id IS DISTINCT FROM $1
                         OR target_node_id IS DISTINCT FROM $1
                         OR placement IS DISTINCT FROM 'on_node'
                       )"
                ))
                .bind(node_id)
                .bind(sandbox_id)
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
        Ok(true)
    }

    /// Persist the observed pod placement, demoting the whole result when the
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
        let result = self.persist_observed_pod_node_placement(placement).await;
        match retry {
            PlacementRetryDisposition::RetryDuplicatesExecution => {
                result.map_err(SubstrateError::demoted_after_side_effect)
            }
            PlacementRetryDisposition::RetryConverges => result,
        }
    }
}

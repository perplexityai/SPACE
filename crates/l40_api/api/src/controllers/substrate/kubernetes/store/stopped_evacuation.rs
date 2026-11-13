//! A drain's stopped evacuation against the execution an unfinished
//! stay-stopped runtime upgrade left: the claim that opens the evacuation and
//! the read each later tick makes before the evacuation mints its backup.

use super::*;
use pplx_space_sandbox_store::drain::{StoppedEvacuationClaim, claim_stopped_evacuation};

/// The active execution a stopped sandbox's runtime upgrade claimed and
/// recorded (`record_stopped_runtime_upgrade_in`) and didn't retire: its pod
/// may still boot the upgrade, and kubelet retries the pod's create after the
/// claim's lease ends, since the node records no transient refusal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeftoverStoppedUpgrade {
    /// The execution's pod name, which its generation fixes.
    pub pod_name: String,
    /// The pod UID the execution recorded; `None` when the attempt ended
    /// between creating its pod and recording it.
    pub pod_uid: Option<String>,
    /// The operation the pod carries.
    pub operation_id: Uuid,
    /// The execution's generation, which fixes its pod's name.
    pub generation: i64,
    /// The attempt whose claim the execution carries; a takeover replaces it.
    pub activation_attempt_id: SandboxActivationAttemptId,
    /// Whether the claim's lease still runs: the upgrade may still own the
    /// execution, and an attempt that does may still place or boot its pod.
    pub lease_live: bool,
    /// The Kubernetes node the upgrade's pod runs its create on: the one the
    /// execution names, or else the node the sandbox's placement pins its
    /// disk to, which the pod is pinned to by `spec.nodeName`. `None` when
    /// neither names a registered node.
    pub node_name: Option<String>,
}

/// What [`K8sStore::open_stopped_evacuation`] did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoppedEvacuationOpening {
    /// The claim ran: see [`StoppedEvacuationClaim`].
    Claimed(StoppedEvacuationClaim),
    /// A stay-stopped runtime upgrade still holds a live lease on the
    /// sandbox's execution, so nothing was claimed. Retry once it ends.
    UpgradeInFlight,
}

impl K8sStore {
    /// Take over the expired claim of a stay-stopped upgrade's execution for
    /// `reserved`, keeping its target and its record, and give it a live
    /// lease: only while the execution of `generation` still carries
    /// `expired` and that claim's lease has ended. A retry of the upgrade, a
    /// drain or another activation then sees a live claim and refuses, so
    /// nothing else takes the execution while the reserving caller deletes
    /// the upgrade's pod and abandons its operation. `false` when the claim
    /// changed hands or its lease still runs.
    pub(in crate::controllers::substrate::kubernetes) async fn reserve_expired_stopped_upgrade(
        &self,
        sandbox_id: SandboxId,
        generation: i64,
        expired: SandboxActivationAttemptId,
        reserved: SandboxActivationAttemptId,
    ) -> Result<bool, SubstrateError> {
        let reserved_rows = sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = $4,
                    activation_attempt_expires_at = NOW() + make_interval(secs => $5),
                    updated_at = NOW()
              WHERE sandbox_id = $1
                AND active
                AND generation = $2
                AND activation_attempt_id = $3
                AND activation_attempt_target_state = $6
                AND activation_attempt_expires_at <= NOW()
                AND runtime_upgrade_operation_id IS NOT NULL"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(generation)
        .bind(expired)
        .bind(reserved)
        .bind(super::super::ACTIVATION_ATTEMPT_LEASE_TTL_SECS)
        .bind(SandboxTransitionTargetState::Stopped.as_str())
        .execute(&self.pool)
        .await
        .db_context("failed to reserve a stopped sandbox's expired runtime upgrade")?
        .rows_affected();
        Ok(reserved_rows == 1)
    }

    /// [`Self::leftover_stopped_upgrade_in`] on a connection of its own.
    pub(in crate::controllers::substrate::kubernetes) async fn leftover_stopped_upgrade(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<Option<LeftoverStoppedUpgrade>, SubstrateError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .db_context("failed to acquire a connection to read a stopped sandbox's upgrade")?;
        Self::leftover_stopped_upgrade_in(&mut conn, sandbox_id).await
    }

    /// See [`LeftoverStoppedUpgrade`]; `None` when the sandbox's active
    /// execution carries no recorded stay-stopped upgrade. A claim the
    /// upgrade's takeover expired keeps its attempt and its target with the
    /// record (`expire_activation_attempt_best_effort`), so an unfinished one
    /// is found whether its lease ran out or was ended early.
    pub(in crate::controllers::substrate::kubernetes) async fn leftover_stopped_upgrade_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<Option<LeftoverStoppedUpgrade>, SubstrateError> {
        // Pod name, pod UID, operation, whether the lease runs, node name,
        // generation, attempt.
        type LeftoverRow = (
            String,
            Option<String>,
            Uuid,
            bool,
            Option<String>,
            i64,
            SandboxActivationAttemptId,
        );
        let row: Option<LeftoverRow> = sqlx::query_as(sql!(
            "SELECT e.pod_name,
                        NULLIF(BTRIM(e.pod_uid), ''),
                        e.runtime_upgrade_operation_id,
                        COALESCE(e.activation_attempt_expires_at > NOW(), TRUE),
                        COALESCE(
                            NULLIF(BTRIM(e.k8s_node_name), ''),
                            (SELECT nk.k8s_node_name
                               FROM sandboxes s
                               JOIN node_k8s nk ON nk.node_id = s.active_node_id
                              WHERE s.id = e.sandbox_id
                                AND (s.target_node_id IS NULL
                                     OR s.target_node_id = s.active_node_id))
                        ),
                        e.generation,
                        e.activation_attempt_id
                   FROM sandbox_k8s_executions e
                  WHERE e.sandbox_id = $1
                    AND e.active
                    AND e.activation_attempt_id IS NOT NULL
                    AND e.activation_attempt_target_state = $2
                    AND e.runtime_upgrade_operation_id IS NOT NULL"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(SandboxTransitionTargetState::Stopped.as_str())
        .fetch_optional(&mut *conn)
        .await
        .db_context("failed to read a stopped sandbox's unfinished runtime upgrade")?;
        Ok(row.map(
            |(
                pod_name,
                pod_uid,
                operation_id,
                lease_live,
                node_name,
                generation,
                activation_attempt_id,
            )| LeftoverStoppedUpgrade {
                pod_name,
                pod_uid,
                operation_id,
                lease_live,
                node_name,
                generation,
                activation_attempt_id,
            },
        ))
    }

    /// Open, or resume, a stopped evacuation of `sandbox_id` off
    /// `source_node_id` ([`claim_stopped_evacuation`]), under the sandbox
    /// row's lock, unless a stay-stopped runtime upgrade still holds a live
    /// lease on its execution.
    ///
    /// The lock is the one the upgrade's claim and its takeover take before
    /// they read the evacuation marker
    /// ([`Self::record_stopped_runtime_upgrade_in`],
    /// `assign_activation_attempt_guarded`), so the two exclude each other:
    /// an upgrade claimed first is seen here and the evacuation waits for its
    /// lease, and once the marker is set every new claim or takeover of the
    /// upgrade refuses retryably. An upgrade whose lease has ended is taken
    /// over by the evacuation: its execution stays, and the evacuation deletes
    /// its pod and waits until it's gone before minting its backup, then
    /// retires the execution at teardown
    /// ([`Self::retire_executions_predating_current_stop`]). An attempt
    /// claims and places its pod inside its boot wait, at most 135 s against
    /// a 300 s lease, so no boot is placed after the lease ends. An attempt
    /// can still be running then, since its backup enqueue (an RPC of up to
    /// 150 s), its pod delete and its database writes follow that wait, but
    /// it's only retiring: it deletes the pod under the UID it read and
    /// retires the execution, generation- and attempt-guarded, and boots
    /// nothing. Only kubelet's retries of the pod's create can still boot the
    /// upgrade, and deleting the pod ends those.
    pub(in crate::controllers::substrate::kubernetes) async fn open_stopped_evacuation(
        pool: &PgPool,
        sandbox_id: SandboxId,
        source_node_id: NodeId,
        allow_new_claims: bool,
        reused_durable_artifact: Option<LocalArtifactId>,
    ) -> Result<StoppedEvacuationOpening, SubstrateError> {
        let mut tx = pool
            .begin()
            .await
            .db_context("failed to start stopped evacuation claim transaction")?;
        if !Self::lock_sandbox_for_execution_mutation_in(&mut tx, sandbox_id).await? {
            return Ok(StoppedEvacuationOpening::Claimed(
                StoppedEvacuationClaim::Refused,
            ));
        }
        if Self::leftover_stopped_upgrade_in(&mut tx, sandbox_id)
            .await?
            .is_some_and(|leftover| leftover.lease_live)
        {
            return Ok(StoppedEvacuationOpening::UpgradeInFlight);
        }
        let claim = claim_stopped_evacuation(
            &mut *tx,
            sandbox_id,
            source_node_id,
            allow_new_claims,
            reused_durable_artifact,
        )
        .await
        .db_context("failed to claim a stopped evacuation")?;
        tx.commit()
            .await
            .db_context("failed to commit stopped evacuation claim")?;
        Ok(StoppedEvacuationOpening::Claimed(claim))
    }

    /// Whether a stopped evacuation has claimed `sandbox_id`: its marker
    /// reads `stopped`. Read under the sandbox row's lock, which the
    /// evacuation's claim takes too ([`Self::open_stopped_evacuation`]).
    pub(super) async fn stopped_evacuation_marked_in(
        conn: &mut PgConnection,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        sqlx::query_scalar(sql!(
            "SELECT EXISTS (
                 SELECT 1 FROM sandboxes
                  WHERE id = $1 AND evacuation_restore_target = $2
             )"
        ))
        .bind(sandbox_id.as_uuid())
        .bind(SandboxLifecycle::Stopped.as_str())
        .fetch_one(&mut *conn)
        .await
        .db_context("failed to read a stopped sandbox's evacuation marker")
    }
}

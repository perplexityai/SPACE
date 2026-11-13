//! What a drain's stopped evacuation asks of the Kubernetes substrate about
//! an unfinished stay-stopped runtime upgrade: open the evacuation against
//! the upgrade's lease, find the execution the upgrade left, and remove the
//! pod whose kubelet retries could still boot the upgrade.
//!
//! The evacuation's teardown releases the node only while the node still
//! pins the artifact the evacuation minted, and an upgrade boot drops that
//! pin. So no upgrade may boot once the evacuation mints its backup: the
//! evacuation opens only when no upgrade holds a live lease, the marker it
//! sets refuses every later claim or takeover of the upgrade, and the pod an
//! upgrade left is deleted, and gone, before the mint, and again before the
//! teardown retires its execution, for an evacuation resumed with its backup
//! already durable.

use super::store::K8sStore;
pub use super::store::{LeftoverStoppedUpgrade, StoppedEvacuationOpening};
use super::{FencedPodDeleteOutcome, KubernetesSubstrate, delete_sandbox_pod_fenced};
use crate::controllers::substrate::DbContext;
use crate::controllers::substrate::SubstrateError;
use pplx_space_common_core::types::{
    LocalArtifactId, NodeId, PodName, SandboxActivationAttemptId, SandboxId,
};
use sqlx::PgPool;

/// Open, or resume, a stopped evacuation unless a stay-stopped runtime
/// upgrade still holds a live lease on the sandbox's execution
/// ([`K8sStore::open_stopped_evacuation`]).
pub async fn open_stopped_evacuation(
    pool: &PgPool,
    sandbox_id: SandboxId,
    source_node_id: NodeId,
    allow_new_claims: bool,
    reused_durable_artifact: Option<LocalArtifactId>,
) -> Result<StoppedEvacuationOpening, SubstrateError> {
    K8sStore::open_stopped_evacuation(
        pool,
        sandbox_id,
        source_node_id,
        allow_new_claims,
        reused_durable_artifact,
    )
    .await
}

/// The execution an unfinished stay-stopped runtime upgrade of `sandbox_id`
/// left, if its active execution is one ([`LeftoverStoppedUpgrade`]).
pub async fn leftover_stopped_upgrade(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> Result<Option<LeftoverStoppedUpgrade>, SubstrateError> {
    let mut conn = pool
        .acquire()
        .await
        .db_context("failed to acquire a connection to read a stopped sandbox's upgrade")?;
    K8sStore::leftover_stopped_upgrade_in(&mut conn, sandbox_id).await
}

impl KubernetesSubstrate {
    /// Settle a stay-stopped upgrade whose claim's lease has ended before a
    /// drain checks the node for a fresh stopped evacuation: answer whether
    /// the drain may go on (`true`) or should defer and look again. A boot
    /// the upgrade verified but didn't stop leaves the sandbox running on the
    /// node, with its pod possibly gone, and the node's own recovery doesn't
    /// stop a verified boot; the evacuation's node check would defer on it
    /// forever, before its leftover-upgrade step ever ran. So the expired
    /// claim is reserved, as the owed backup reserves it, so no retry takes
    /// the execution meanwhile, and its pod is deleted and its operation
    /// abandoned on the node, which settles a verified boot by stopping it
    /// ([`Self::remove_leftover_upgrade_pod`]). The reservation's lease is
    /// then ended again, leaving the execution for the evacuation to take
    /// over. No leftover, or one whose lease still runs, goes on unchanged:
    /// the evacuation's own claim defers a live one.
    pub async fn settle_expired_stopped_upgrade(
        &self,
        sandbox_id: SandboxId,
    ) -> Result<bool, SubstrateError> {
        let Some(leftover) = self.store.leftover_stopped_upgrade(sandbox_id).await? else {
            return Ok(true);
        };
        if leftover.lease_live {
            return Ok(true);
        }
        let reserved = SandboxActivationAttemptId::new();
        if !self
            .store
            .reserve_expired_stopped_upgrade(
                sandbox_id,
                leftover.generation,
                leftover.activation_attempt_id,
                reserved,
            )
            .await?
        {
            return Ok(false);
        }
        let reserved_leftover = LeftoverStoppedUpgrade {
            activation_attempt_id: reserved,
            lease_live: true,
            ..leftover
        };
        let settled = self
            .remove_leftover_upgrade_pod(sandbox_id, &reserved_leftover)
            .await;
        self.store
            .expire_activation_attempt_best_effort(
                sandbox_id,
                reserved_leftover.generation,
                reserved,
            )
            .await;
        settled
    }

    /// Delete the pod `leftover`'s upgrade placed, and answer whether it's
    /// gone: `true` once no pod of that upgrade is at the execution's name.
    /// A pod there that is the upgrade's (the UID the execution recorded, or
    /// one carrying its operation) is deleted under its own UID and answers
    /// `false` until the API server no longer has it, so a caller waits for
    /// kubelet to stop it rather than racing its last retry. A pod there
    /// that is neither runs no step of this upgrade and answers `true`.
    ///
    /// The read goes to the API server when the reflector has no pod there,
    /// so a pod the reflector hasn't seen yet isn't taken as gone.
    ///
    /// A pod that's gone doesn't prove its shim's `UpgradeRuntime` call is:
    /// one force-deleted mid-create can still have the call on its way to the
    /// node. So `true` also needs the node to have abandoned the operation
    /// ([`Self::abandon_leftover_upgrade`]), after which no call for it
    /// fences or boots there.
    pub async fn remove_leftover_upgrade_pod(
        &self,
        sandbox_id: SandboxId,
        leftover: &LeftoverStoppedUpgrade,
    ) -> Result<bool, SubstrateError> {
        let name = PodName::parse(&leftover.pod_name).map_err(|error| SubstrateError::Failed {
            message: format!(
                "the runtime upgrade's execution names an invalid pod {:?} ({error})",
                leftover.pod_name
            ),
        })?;
        let pod = self
            .lookup_or_fetch_sandbox_pod(name.as_str())
            .await
            .map_err(|error| SubstrateError::Unavailable {
                message: format!("failed to read the runtime upgrade's pod {name}: {error}; retry"),
            })?;
        let Some(pod) = pod else {
            return self.abandon_leftover_upgrade(sandbox_id, leftover).await;
        };
        let recorded = leftover
            .pod_uid
            .as_deref()
            .is_some_and(|uid| pod.uid() == Some(uid));
        if !recorded && !pod.carries_runtime_upgrade(leftover.operation_id) {
            return self.abandon_leftover_upgrade(sandbox_id, leftover).await;
        }
        if !pod.is_deleting()
            && let Some(uid) = pod.pod_uid()
        {
            match delete_sandbox_pod_fenced(&self.k8s, &name, &uid, None).await {
                Ok(
                    FencedPodDeleteOutcome::Deleted
                    | FencedPodDeleteOutcome::NotFound
                    | FencedPodDeleteOutcome::PreconditionFailed,
                ) => {}
                Err(error) => {
                    return Err(SubstrateError::Unavailable {
                        message: format!(
                            "failed to delete the runtime upgrade's pod {name}: {error}; retry"
                        ),
                    });
                }
            }
        }
        Ok(false)
    }

    /// Abandon `leftover`'s operation on the node its pod runs its create on,
    /// so a call for it that's still on its way never fences or boots, and
    /// answer `true` once the node has. The node takes the sandbox's upgrade
    /// lock, which an upgrade running there holds for its whole call, so one
    /// running now finishes first. An operation still fencing the sandbox,
    /// a node that can't be reached or doesn't serve the call yet, and an
    /// execution naming no node all answer a retryable error: until the node
    /// abandons the operation, a capture could be outrun. A node that refuses
    /// the request as invalid (the operation is recorded there for another
    /// sandbox, or an id doesn't parse) answers `FailedPrecondition`, since no
    /// retry changes that. This call's ids are typed, so they always parse,
    /// and what ends the refusal is an operator repairing the node's record
    /// of the operation.
    ///
    /// Nothing here remembers the refusal: every call asks the node again,
    /// so after a repair the next activation or drain re-placement that
    /// enqueues the owed backup goes through. A caller that remembers it asks
    /// again only by its own rules. The cluster controller's node drain asks
    /// once per sandbox and operation for the life of its process
    /// (`refused_upgrade_abandon` in
    /// `crates/l40_api/cluster/controller/server/src/node_drain/store.rs`),
    /// so a repair reaches the drain only after the controller's process
    /// restarts (a replica that loses its leadership lease exits, so a
    /// leadership change is one) or a new operation on the sandbox, and the
    /// sandbox holds the node until then. Deleting the sandbox releases the node once the delete
    /// completes: a `deleting` row still counts as resident
    /// (`NON_RESIDENT_LIFECYCLES` in `crates/l30_storage/sandbox/residency/src/lib.rs`).
    async fn abandon_leftover_upgrade(
        &self,
        sandbox_id: SandboxId,
        leftover: &LeftoverStoppedUpgrade,
    ) -> Result<bool, SubstrateError> {
        let unavailable = |reason: String| SubstrateError::Unavailable {
            message: format!(
                "sandbox {sandbox_id}'s runtime upgrade {} isn't abandoned on its node: \
                 {reason}; retry",
                leftover.operation_id
            ),
        };
        let Some(node_name) = leftover.node_name.as_deref() else {
            return Err(unavailable(
                "neither its execution nor the sandbox's placement names the node".to_string(),
            ));
        };
        let (addr, client) = self
            .node_worker_client(node_name)
            .await
            .map_err(unavailable)?;
        let request = pplx_space_sbx_mgr_client::proto::AbandonRuntimeUpgradeRequest {
            sandbox_id: sandbox_id.to_string(),
            operation_id: leftover.operation_id.to_string(),
        };
        match tokio::time::timeout(
            ABANDON_TIMEOUT,
            client.internal_client().abandon_runtime_upgrade(request),
        )
        .await
        {
            Ok(Ok(_)) => Ok(true),
            // Sandbox-manager answers `InvalidArgument` for an operation it
            // records for another sandbox, or a request whose ids don't
            // parse. No retry changes either, so retrying would hold the
            // caller, a start or a drain, forever.
            Ok(Err(status)) if status.code() == tonic::Code::InvalidArgument => {
                Err(SubstrateError::FailedPrecondition {
                    message: format!(
                        "sandbox {sandbox_id}'s runtime upgrade {} can't be abandoned on its \
                         node: node-api at {addr} refused it ({}); asking again gets the same \
                         answer until an operator repairs the node's record of the operation",
                        leftover.operation_id,
                        status.message()
                    ),
                })
            }
            Ok(Err(status)) => Err(unavailable(format!("node-api at {addr} answered {status}"))),
            Err(_elapsed) => Err(unavailable(format!(
                "node-api at {addr} didn't answer within {ABANDON_TIMEOUT:?}"
            ))),
        }
    }
}

/// How long [`KubernetesSubstrate::abandon_leftover_upgrade`] waits for the
/// node: past sandbox-manager's own wait for the sandbox's upgrade lock.
const ABANDON_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

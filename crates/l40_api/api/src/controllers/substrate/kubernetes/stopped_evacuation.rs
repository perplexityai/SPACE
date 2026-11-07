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
use pplx_space_common_core::types::{LocalArtifactId, NodeId, PodName, SandboxId};
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
    pub async fn remove_leftover_upgrade_pod(
        &self,
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
            return Ok(true);
        };
        let recorded = leftover
            .pod_uid
            .as_deref()
            .is_some_and(|uid| pod.uid() == Some(uid));
        if !recorded && !pod.carries_runtime_upgrade(leftover.operation_id) {
            return Ok(true);
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
}

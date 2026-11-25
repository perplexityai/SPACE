//! The capture-time fields a `snapshots` row inherits from its source
//! sandbox, and the node a backup attempt of it is attributed to.

use pplx_space_accounts::{NodeId, OrgId};
use pplx_space_sandbox_model::{ExecutionGeneration, SandboxId};
use pplx_space_sql_locator::sql;

use crate::SandboxStoreError;

/// The capture-time fields a backup attempt's `snapshots` row inherits from
/// its sandbox, and the nodes that may be uploading it.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SnapshotCaptureSource {
    pub org_id: OrgId,
    pub active_node_id: Option<NodeId>,
    /// The live worker the named execution's pod was placed on.
    pub execution_node_id: Option<NodeId>,
    pub target_node_id: Option<NodeId>,
    pub vcpu: Option<i32>,
    pub memory_mib: Option<i32>,
    pub template_id: Option<String>,
    pub content_hash: Option<String>,
}

impl SnapshotCaptureSource {
    /// The node the snapshot is physically uploading **from**:
    ///
    /// 1. `active_node_id`, for a fresh suspend of a placed VM.
    /// 2. The pod node of the named execution, on a row with no active node:
    ///    a teardown of that pod captures the VM where the pod ran, and only
    ///    that node holds it. Recording a pod's node also makes it the row's
    ///    active and target node, so this differs from `target_node_id` when
    ///    a later restore claim placed the row on another node and was
    ///    abandoned before its pod ran (the revert keeps the target), and on
    ///    a row detached from every node (`target_node_id` NULL too). The
    ///    registration's locked checks, the generation fence or the recheck
    ///    after it, refuse this source once a newer execution is active or
    ///    has run a pod.
    /// 3. `target_node_id` (kept across suspend), for a reupload of an
    ///    already-suspended sandbox, which runs where its artifacts are. A
    ///    NULL source would hide the upload from the node-scoped drain guard.
    pub fn source_node_id(&self) -> Option<NodeId> {
        self.active_node_id
            .or(self.execution_node_id)
            .or(self.target_node_id)
    }

    /// Whether [`Self::source_node_id`] is the named execution's pod node
    /// standing in for a different target (or for none), which a newer
    /// execution can revoke after this read. A pod that ran on the target
    /// revokes nothing: the source is the target either way.
    pub fn execution_node_stands_in_for_target(&self) -> bool {
        self.active_node_id.is_none()
            && self
                .execution_node_id
                .is_some_and(|node| Some(node) != self.target_node_id)
    }
}

/// [`SnapshotCaptureSource`] of `sandbox_id`, with the pod node of
/// `execution_generation` when one is named. `None` when the sandbox doesn't
/// exist.
pub async fn snapshot_capture_source(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    execution_generation: Option<ExecutionGeneration>,
) -> Result<Option<SnapshotCaptureSource>, SandboxStoreError> {
    let row = sqlx::query_as::<_, SnapshotCaptureSource>(sql!(
        // `node_k8s.k8s_node_name` isn't unique: worker and builder daemons
        // on one host register under the same name. The pod ran on the
        // worker, so only live worker registrations count, and the node is
        // named only when exactly one carries the name; an ambiguous name
        // falls back to the target node rather than guess. Whether a newer
        // execution has revoked this node is decided under lock by the
        // caller's registration, not here.
        "SELECT s.org_id, s.active_node_id, s.target_node_id, s.vcpu, s.memory_mib,
                s.template_id, s.content_hash,
                (SELECT (array_agg(nk.node_id))[1]
                   FROM sandbox_k8s_executions e
                   JOIN node_k8s nk ON nk.k8s_node_name = e.k8s_node_name
                   JOIN nodes n ON n.id = nk.node_id
                  WHERE e.sandbox_id = s.id
                    AND e.generation = $2
                    AND n.role = 'worker'
                    AND COALESCE(n.state, 'registered') != 'deleted'
                 HAVING COUNT(*) = 1) AS execution_node_id
         FROM sandboxes s
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .bind(execution_generation.map(ExecutionGeneration::get))
    .fetch_optional(executor)
    .await?;
    Ok(row)
}

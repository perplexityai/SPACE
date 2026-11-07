//! The pod contract of a runtime upgrade that boots in its pod's shim: the
//! `runtime-upgrade` checkpoint and the annotations that carry the upgrade.
//! [`super::build_sandbox_pod`] renders them for a
//! [`super::SandboxPodRequest::Restore`] that names a [`PodRuntimeUpgrade`].

/// The [`SANDBOX_CHECKPOINT_ANNOTATION`](super::SANDBOX_CHECKPOINT_ANNOTATION) value of a pod that boots its
/// sandbox under another execution environment; see [`PodRuntimeUpgrade`].
/// Its shim calls sandbox-manager's `UpgradeRuntime` with the cold start it
/// would otherwise send as a `Start`, carrying the pod's network namespace,
/// and never falls back to a `Start`.
///
/// A distinct value so a shim that predates it fails closed: it forwards the
/// value as the `Start` checkpoint, which sandbox-manager refuses for a
/// sandbox in any local state and for one absent from the node.
pub const RUNTIME_UPGRADE_CHECKPOINT: &str = "runtime-upgrade";
/// The upgrade's operation id. A retry, on this pod or a new one, carries the
/// same id, and the node settles a recorded operation rather than booting it
/// again.
pub const SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION: &str =
    "space.pplx.net/runtime-upgrade-operation-id";
pub const SANDBOX_RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION: &str =
    "space.pplx.net/runtime-upgrade-target-environment";
pub const SANDBOX_RUNTIME_UPGRADE_AUTHORIZATION_ID_ANNOTATION: &str =
    "space.pplx.net/runtime-upgrade-authorization-id";
pub const SANDBOX_RUNTIME_UPGRADE_AUTHORIZATION_REASON_ANNOTATION: &str =
    "space.pplx.net/runtime-upgrade-authorization-reason";
/// `true` or `false`: whether the authorization accepts discarding a
/// suspended sandbox's memory.
pub const SANDBOX_RUNTIME_UPGRADE_MEMORY_LOSS_ANNOTATION: &str =
    "space.pplx.net/runtime-upgrade-memory-loss";
/// `started` or `stopped`; see [`PodRuntimeUpgradeTargetState`].
pub const SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION: &str =
    "space.pplx.net/runtime-upgrade-target-state";
/// For a suspended source, the local artifact of the checkpoint the upgrade
/// was decided from.
pub const SANDBOX_RUNTIME_UPGRADE_CHECKPOINT_LOCAL_ARTIFACT_ID_ANNOTATION: &str =
    "space.pplx.net/runtime-upgrade-checkpoint-local-artifact-id";

/// A runtime upgrade as its pod renders it: the fields of sandbox-manager's
/// `UpgradeRuntimeRequest` that the pod's shim can't derive from the rest of
/// the pod. Renders [`RUNTIME_UPGRADE_CHECKPOINT`] and one annotation per
/// field; the restore's storage URI and source node, when present, are where
/// the node stages a suspended sandbox absent from it.
///
/// Only the control plane writes sandbox pods, and these annotations are the
/// upgrade's authorization: a principal that can create a sandbox pod can
/// request an upgrade, including one that discards a suspended sandbox's
/// memory. The node still refuses any move that isn't a certified upgrade
/// edge from the environment the sandbox ran under.
#[derive(Debug, Clone, Copy)]
pub struct PodRuntimeUpgrade<'a> {
    pub operation_id: uuid::Uuid,
    pub target_environment: pplx_space_execution_spec::EnvironmentReference,
    pub authorization_id: &'a str,
    pub authorization_reason: &'a str,
    pub memory_loss: bool,
    pub target_state: PodRuntimeUpgradeTargetState,
    /// Required for a suspended source, which the node refuses to upgrade
    /// from any other checkpoint; absent for a stopped one.
    pub checkpoint_local_artifact_id: Option<pplx_space_common_core::types::LocalArtifactId>,
}

/// The lifecycle a pod's runtime upgrade leaves the sandbox in. `Started`
/// keeps the new execution running in the pod; `Stopped` stops it again, and
/// the pod's task is a placeholder with no microVM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodRuntimeUpgradeTargetState {
    Started,
    Stopped,
}

impl PodRuntimeUpgradeTargetState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for PodRuntimeUpgradeTargetState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

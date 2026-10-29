//! Durable identity of an idle decision, not authority to stop a live VM.

use pplx_space_common_core::types::{
    AutoLifecyclePolicy, ExecutionGeneration, SandboxActivationAttemptId, SandboxRuntimeLabel,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct AutoSuspendHandoff {
    runtime_label: SandboxRuntimeLabel,
    execution_generation: Option<ExecutionGeneration>,
    node_execution_id: Option<SandboxActivationAttemptId>,
    policy: AutoLifecyclePolicy,
    activity_epoch: u64,
    pub(crate) due_at: u64,
    pub(crate) foreground_deadline_at: u64,
    /// First wire Unavailable returned by an actual transfer submission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) refused_at: Option<u64>,
    #[serde(default)]
    pub(crate) failures: u32,
}

impl AutoSuspendHandoff {
    pub(crate) fn new(sandbox: &crate::db::SandboxRow, due_at: u64) -> Self {
        Self {
            runtime_label: sandbox.uuid8.clone(),
            execution_generation: sandbox.space_api_route_identity.execution_generation,
            node_execution_id: sandbox.node_execution_id,
            policy: sandbox.auto_lifecycle.clone(),
            activity_epoch: sandbox.auto_lifecycle_state.handoff_epoch,
            due_at,
            foreground_deadline_at: crate::auto_lifecycle::now_secs().saturating_add(120),
            refused_at: None,
            failures: 0,
        }
    }

    pub(crate) fn owns(&self, sandbox: &crate::db::SandboxRow) -> bool {
        self.runtime_label == sandbox.uuid8
            && self.execution_generation == sandbox.space_api_route_identity.execution_generation
            && self.node_execution_id == sandbox.node_execution_id
            && self.same_idle_decision(sandbox)
            && matches!(
                sandbox.state,
                crate::state::SandboxState::Paused | crate::state::SandboxState::Suspending
            )
            && sandbox.cleanup_completion.is_none()
    }

    pub(crate) fn same_idle_decision(&self, sandbox: &crate::db::SandboxRow) -> bool {
        self.policy == sandbox.auto_lifecycle
            && self.activity_epoch == sandbox.auto_lifecycle_state.handoff_epoch
    }
}

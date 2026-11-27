use pplx_space_public_api::SandboxLifecycleState;
use pplx_space_sandbox_model::SandboxLifecycle;

pub(crate) fn public_lifecycle_state(lifecycle: SandboxLifecycle) -> SandboxLifecycleState {
    SandboxLifecycleState::from(lifecycle)
}

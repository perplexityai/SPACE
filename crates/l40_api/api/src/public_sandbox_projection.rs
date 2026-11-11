use pplx_space_common_core::types::SandboxLifecycle;
use pplx_space_public_api::SandboxLifecycleState;

pub(crate) fn public_lifecycle_state(lifecycle: SandboxLifecycle) -> SandboxLifecycleState {
    SandboxLifecycleState::from(lifecycle)
}

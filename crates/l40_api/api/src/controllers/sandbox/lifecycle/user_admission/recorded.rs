//! The requests [`super::admit_user_lifecycle`] was asked to admit, so a test
//! can see which request a path asked for and for which sandbox.

use std::sync::{Mutex, PoisonError};

use pplx_space_common_core::types::SandboxId;

use super::UserLifecycleRequest;

/// Every admission in this test process, in order. Tests share the process,
/// so each reads only its own sandboxes.
static ADMITTED: Mutex<Vec<(SandboxId, UserLifecycleRequest)>> = Mutex::new(Vec::new());

pub(super) fn record(sandbox_id: SandboxId, request: UserLifecycleRequest) {
    ADMITTED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((sandbox_id, request));
}

/// The requests admitted for `sandbox_id` so far, in order.
pub(crate) fn admitted(sandbox_id: SandboxId) -> Vec<UserLifecycleRequest> {
    ADMITTED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter(|(admitted, _)| *admitted == sandbox_id)
        .map(|(_, request)| *request)
        .collect()
}

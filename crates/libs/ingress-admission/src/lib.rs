//! Request-scoped bounds on ingress preparation, independent of activation admission.
//!
//! It is its own crate so ingress callers can share the per-process waiter
//! limits without depending on the rest of the service host.
use pplx_space_common_core::types::SandboxId;
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};
use tonic::Status;

const PROCESS_LIMIT: usize = 1024;
const SANDBOX_LIMIT: usize = 64;
#[derive(Default)]
struct Waiters {
    total: usize,
    sandboxes: BTreeMap<SandboxId, usize>,
}
fn waiters() -> &'static Mutex<Waiters> {
    static WAITERS: OnceLock<Mutex<Waiters>> = OnceLock::new();
    WAITERS.get_or_init(Mutex::default)
}

pub struct IngressPreparationPermit {
    sandbox: Option<SandboxId>,
}
impl IngressPreparationPermit {
    pub fn acquire(sandbox: Option<SandboxId>) -> Result<Self, Status> {
        let mut state = waiters()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.total >= PROCESS_LIMIT
            || sandbox
                .is_some_and(|id| state.sandboxes.get(&id).copied().unwrap_or(0) >= SANDBOX_LIMIT)
        {
            pplx_space_metrics::data_plane::ingress_waiter_rejected();
            return Err(Status::resource_exhausted(
                "ingress preparation capacity exhausted",
            ));
        }
        state.total += 1;
        if let Some(id) = sandbox {
            *state.sandboxes.entry(id).or_default() += 1;
        }
        Ok(Self { sandbox })
    }
    pub fn bind_sandbox(&mut self, id: SandboxId) -> Result<(), Status> {
        if self.sandbox == Some(id) {
            return Ok(());
        }
        if self.sandbox.is_some() {
            return Err(Status::internal("ingress waiter already bound"));
        }
        let mut state = waiters()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = state.sandboxes.entry(id).or_default();
        if *count >= SANDBOX_LIMIT {
            pplx_space_metrics::data_plane::ingress_waiter_rejected();
            return Err(Status::resource_exhausted(
                "sandbox ingress preparation capacity exhausted",
            ));
        }
        *count += 1;
        self.sandbox = Some(id);
        Ok(())
    }
}
impl Drop for IngressPreparationPermit {
    fn drop(&mut self) {
        let mut state = waiters()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.total -= 1;
        if let Some(id) = self.sandbox
            && let Some(count) = state.sandboxes.get_mut(&id)
        {
            *count -= 1;
            if *count == 0 {
                state.sandboxes.remove(&id);
            }
        }
    }
}

/// Opening budget for ingress preparation.
const PREPARATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Opening budget only; never apply it as an established streaming RPC deadline.
pub fn preparation_timeout() -> std::time::Duration {
    PREPARATION_TIMEOUT
}

#[cfg(test)]
mod tests;

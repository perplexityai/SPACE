//! Execution-local ingress ownership. Drop cannot acquire the async wake lock.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Default)]
pub(super) struct State {
    starts: BTreeMap<Instant, usize>,
    last_release: Option<u64>,
}

#[derive(Clone, Default)]
pub(super) struct Protection(Arc<Mutex<State>>);

impl Protection {
    pub fn acquire(&self) -> Guard {
        let started = Instant::now();
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .starts
            .entry(started)
            .or_default() += 1;
        Guard {
            protection: self.clone(),
            started,
        }
    }

    pub fn observation(&self) -> (usize, Option<u64>, Option<std::time::Duration>) {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.starts.values().sum(),
            state.last_release,
            state
                .starts
                .first_key_value()
                .map(|(start, _)| start.elapsed()),
        )
    }

    pub fn consumed(&self, observed: u64) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.last_release == Some(observed) {
            state.last_release = None;
        }
    }
}

pub(super) struct Guard {
    protection: Protection,
    started: Instant,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = self
            .protection
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = state.starts.get_mut(&self.started) {
            *count -= 1;
            if *count == 0 {
                state.starts.remove(&self.started);
            }
        }
        if state.starts.is_empty() {
            state.last_release = Some(crate::auto_lifecycle::now_secs());
        }
    }
}

pub(super) struct AdmittedIngress {
    pub sandbox: crate::db::SandboxRow,
    pub fence: super::RouteStreamFence,
    pub admission: super::ServicePortIngressAdmissionPermit,
    pub protection: Guard,
}

impl super::SandboxManagerService {
    /// Admission and the scheduler linearize under the same lifecycle lock.
    pub(super) async fn admit_ingress(
        &self,
        sandbox_id: &str,
        timeout: std::time::Duration,
        authorize: impl FnOnce(&crate::db::SandboxRow) -> Result<(), tonic::Status>,
    ) -> Result<AdmittedIngress, tonic::Status> {
        let started = Instant::now();
        let lock = self.auto_lifecycle.wake_lock(sandbox_id);
        let _guard = tokio::time::timeout(timeout, lock.lock())
            .await
            .map_err(|_| {
                tonic::Status::deadline_exceeded("ingress lifecycle admission timed out")
            })?;
        pplx_space_metrics::sandbox::worker::ingress_admission_wait(started);
        let sandbox = self.lookup_guest_bridge_sandbox(sandbox_id)?;
        authorize(&sandbox)?;
        if sandbox.state != crate::state::SandboxState::Started {
            return Err(tonic::Status::failed_precondition(
                "ingress execution is not started",
            ));
        }
        let (fence, admission) = self.service_port_ingress_identity_lease(&sandbox)?;
        let protection = self
            .ingress_protection(&sandbox)?
            .ok_or_else(|| tonic::Status::aborted("ingress execution epoch changed"))?
            .acquire();
        // Only this synchronous write participates in restart drain. A started
        // execution may still serve ingress without touching persisted activity
        // once mutation admission closes; the stream itself owns no writer slot.
        if let Some(_writer) = self.state_writer_lease_if_accepting("ServicePortIngress") {
            let mut runtime = sandbox.auto_lifecycle_state.clone();
            crate::auto_lifecycle::apply_observed_access_touch(
                &mut runtime,
                &sandbox.auto_lifecycle,
                crate::auto_lifecycle::now_secs(),
            );
            crate::db::update_auto_lifecycle_state(&*self.db_conn()?, &sandbox.id, &runtime)
                .map_err(|error| tonic::Status::internal(error.to_string()))?;
        }
        Ok(AdmittedIngress {
            sandbox,
            fence,
            admission,
            protection,
        })
    }
}

#[cfg(test)]
mod tests;

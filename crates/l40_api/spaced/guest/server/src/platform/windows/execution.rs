//! Shared Windows process/file admission and generation-scoped configuration.
use super::{filesystem::Filesystem, process::Processes};
use pplx_space_guest_process::WindowsEnv;
use pplx_space_spaced_guest_protocol::proto::update::DeferReason;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio_util::{
    sync::CancellationToken,
    task::{TaskTracker, task_tracker::TaskTrackerToken},
};
use tonic::Status;
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct Admission(Arc<AdmissionInner>);
struct AdmissionInner {
    owner: Mutex<Option<Uuid>>,
    operations: TaskTracker,
    shutdown: CancellationToken,
    environment: Mutex<HashMap<String, String>>,
}
pub(super) struct MaintenanceHold {
    admission: Admission,
    owner: Uuid,
}
impl Drop for MaintenanceHold {
    fn drop(&mut self) {
        let mut owner = self
            .admission
            .0
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *owner == Some(self.owner) {
            *owner = None;
        }
    }
}
impl Admission {
    pub(super) fn new(shutdown: CancellationToken) -> Self {
        Self(Arc::new(AdmissionInner {
            owner: Mutex::new(None),
            operations: TaskTracker::new(),
            shutdown,
            environment: Mutex::new(HashMap::new()),
        }))
    }
    pub(super) fn admit(&self) -> Result<TaskTrackerToken, Status> {
        let owner = self
            .0
            .owner
            .lock()
            .map_err(|_| Status::internal("admission poisoned"))?;
        if owner.is_some() || self.0.shutdown.is_cancelled() {
            return Err(Status::unavailable("guest maintenance admission held"));
        }
        Ok(self.0.operations.token())
    }
    fn hold(&self, owner: Uuid) -> Result<MaintenanceHold, Status> {
        let mut current = self
            .0
            .owner
            .lock()
            .map_err(|_| Status::internal("admission poisoned"))?;
        if owner.is_nil() || current.is_some() || self.0.shutdown.is_cancelled() {
            return Err(Status::unavailable("guest admission owner changed"));
        }
        *current = Some(owner);
        Ok(MaintenanceHold {
            admission: self.clone(),
            owner,
        })
    }
    pub(super) fn environment(
        &self,
        request: HashMap<String, String>,
    ) -> Result<WindowsEnv, Status> {
        let request =
            WindowsEnv::parse(request).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let mut merged = self
            .0
            .environment
            .lock()
            .map_err(|_| Status::internal("environment poisoned"))?
            .clone();
        // Per-process values override the configured defaults, case-insensitively.
        for (key, value) in request.into_map() {
            merged.retain(|existing, _| existing.to_uppercase() != key.to_uppercase());
            merged.insert(key, value);
        }
        WindowsEnv::parse(merged).map_err(|e| Status::invalid_argument(e.to_string()))
    }
    pub(super) fn configure(&self, env: WindowsEnv) -> Result<(), Status> {
        *self
            .0
            .environment
            .lock()
            .map_err(|_| Status::internal("environment poisoned"))? = env.into_map();
        Ok(())
    }
}

pub(super) struct Execution {
    pub(super) admission: Admission,
    pub(super) filesystem: Option<Filesystem>,
    pub(super) processes: Option<Processes>,
}
impl Execution {
    pub(super) fn hold_for_update(&self, owner: Uuid) -> Result<MaintenanceHold, Status> {
        self.admission.hold(owner)
    }
    pub(super) async fn configure_process_env_for_update(
        &self,
        hold: &MaintenanceHold,
        env: WindowsEnv,
    ) -> Result<(), Status> {
        let owner = self
            .admission
            .0
            .owner
            .lock()
            .map_err(|_| Status::internal("admission poisoned"))?;
        if !Arc::ptr_eq(&hold.admission.0, &self.admission.0)
            || *owner != Some(hold.owner)
            || self.admission.0.shutdown.is_cancelled()
        {
            return Err(Status::failed_precondition("maintenance ownership changed"));
        }
        self.admission.configure(env)
    }
    pub(super) async fn update_defer_reasons(&self) -> Vec<DeferReason> {
        let mut reasons = Vec::new();
        if !self.admission.0.operations.is_empty() {
            reasons.push(DeferReason::Operations);
        }
        if self
            .processes
            .as_ref()
            .is_some_and(Processes::has_update_obligations)
        {
            reasons.push(DeferReason::RetainedProcesses);
        }
        if self
            .filesystem
            .as_ref()
            .is_some_and(Filesystem::has_update_obligations)
        {
            reasons.push(DeferReason::Filesystem);
        }
        reasons
    }
}

pub(super) fn retain_stream<T: Send + 'static>(
    stream: std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, Status>> + Send>>,
    permit: TaskTrackerToken,
) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T, Status>> + Send>> {
    use futures_util::StreamExt;
    Box::pin(futures_util::stream::unfold(
        (stream, permit),
        |(mut stream, permit)| async move { stream.next().await.map(|item| (item, (stream, permit))) },
    ))
}

#[cfg(test)]
mod tests;

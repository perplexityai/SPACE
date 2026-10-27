//! Host-owned device eligibility. Sandbox teardown queues work; it never waits
//! for this worker's reset or a future sanitizer execution.
#[cfg(all(target_os = "linux", feature = "gpu-development"))]
use super::device_admission::device_store_status;
use crate::slot_pool::{NetworkPool, devices::*};
use pplx_space_execution_spec::devices::HostDeviceId;
use std::sync::Arc;
use tonic::Status;

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DevelopmentAuthorization {
    device: HostDeviceId,
    host_boot_id: HostBootId,
}
#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("device development authorization: {0}")]
    Environment(#[from] std::env::VarError),
    #[error("invalid device development authorization: {0}")]
    Json(#[from] serde_json::Error),
    #[error("device development authorization requires a development build")]
    Production,
}
impl DevelopmentAuthorization {
    pub(crate) fn from_env() -> Result<Option<Self>, ConfigurationError> {
        match std::env::var("SPACE_GPU_DEVELOPMENT_AUTHORIZATION") {
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(e) => Err(e.into()),
            Ok(_) if !cfg!(feature = "gpu-development") => Err(ConfigurationError::Production),
            Ok(value) => Ok(Some(serde_json::from_str(&value)?)),
        }
    }
    fn permits(&self, members: &[HostDeviceId], boot: HostBootId) -> bool {
        boot == self.host_boot_id && members == [self.device.clone()]
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct DevicePool {
    authorization: Option<DevelopmentAuthorization>,
    pub(super) gate: Arc<tokio::sync::Mutex<()>>,
    pub(super) changed: Arc<tokio::sync::Notify>,
}
impl DevicePool {
    pub(super) fn enabled(&self) -> bool {
        cfg!(all(target_os = "linux", feature = "gpu-development")) && self.authorization.is_some()
    }
    pub(super) fn new(authorization: Option<DevelopmentAuthorization>) -> Self {
        Self {
            authorization,
            ..Self::default()
        }
    }
    pub(super) fn check(&self, parent: &DeviceParent, boot: HostBootId) -> Result<(), Status> {
        self.check_members(parent.members(), boot)?;
        #[cfg(feature = "gpu-development")]
        {
            let profile = pplx_space_node_runtime_core::catalog::development_t4_profile()
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
            if parent.profile() == profile.profile()
                && parent.qualification_procedure() == profile.qualification_procedure()
                && parent.host_boot_id() == boot
            {
                return Ok(());
            }
        }
        let _ = parent;
        Err(Status::failed_precondition(
            "no enabled reset-only development procedure for parent",
        ))
    }
    pub(super) fn check_members(
        &self,
        members: &[HostDeviceId],
        boot: HostBootId,
    ) -> Result<(), Status> {
        if cfg!(feature = "gpu-development")
            && self
                .authorization
                .as_ref()
                .is_some_and(|a| a.permits(members, boot))
        {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "device is outside this boot's explicit development authorization",
            ))
        }
    }
    pub(super) fn start(&self, pool: Arc<NetworkPool>, tracker: tokio_util::task::TaskTracker) {
        #[cfg(all(target_os = "linux", feature = "gpu-development"))]
        if self.authorization.is_some() {
            let worker = self.clone();
            let tasks = tracker.clone();
            tracker.spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = pool.shutdown_token.cancelled() => break,
                        _ = worker.changed.notified() => {},
                        _ = tick.tick() => {},
                    }
                    let gate = tokio::select! {
                        biased;
                        _ = pool.shutdown_token.cancelled() => break,
                        gate = worker.gate.clone().lock_owned() => gate,
                    };
                    let worker = worker.clone();
                    let pool = pool.clone();
                    let result = tasks
                        .spawn_blocking(move || {
                            let _gate = gate;
                            worker.reconcile(&pool)
                        })
                        .await;
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            tracing::warn!(%error,"device pool reconciliation failed")
                        }
                        Err(error) => tracing::error!(%error,"device pool task failed"),
                    }
                }
            });
        }
        #[cfg(not(all(target_os = "linux", feature = "gpu-development")))]
        let _ = (pool, tracker);
    }

    #[cfg(all(target_os = "linux", feature = "gpu-development"))]
    fn reconcile(&self, pool: &NetworkPool) -> Result<(), Status> {
        let boot = HostBootId::current().map_err(device_store_status)?;
        for parent in pool.device_parents().map_err(device_store_status)? {
            if parent.state() != ParentState::Sanitizing {
                continue;
            }
            let authorization = self.check(&parent, boot);
            let claimed = pool
                .claim_device_reset(parent.id(), parent.revision(), boot)
                .map_err(device_store_status)?;
            let started = jiff::Timestamp::now();
            let reset = authorization.and_then(|()| self.reset(&claimed));
            // reset() has dropped every VFIO descriptor before returning.
            let evidence = reset.map(|()| QualificationEvidence::DevelopmentResetV1 {
                profile: claimed.profile().clone(),
                procedure: claimed.qualification_procedure(),
                host_boot_id: boot,
                members: claimed.members().to_vec(),
                reset_started_at: started,
                completed_at: jiff::Timestamp::now(),
            });
            if let Err(error) = &evidence {
                tracing::warn!(parent=%claimed.id(),%error,"device reset failed; quarantining capacity");
            }
            let message = evidence.as_ref().err().map(ToString::to_string);
            let completed = pool
                .finish_device_reset(
                    &claimed,
                    evidence.map_err(|_| message.as_deref().unwrap_or("device reset failed")),
                )
                .map_err(device_store_status)?;
            tracing::info!(parent=%claimed.id(), revision=claimed.revision(), completed, reset_succeeded=message.is_none(), "device pool reset attempt finished");
        }
        Ok(())
    }
    #[cfg(all(target_os = "linux", feature = "gpu-development"))]
    fn reset(&self, parent: &DeviceParent) -> Result<(), Status> {
        use super::device_admission::{device_inspection_status, observe_device_identity};
        use pplx_space_node_runtime_core::vfio::PinnedVfio;
        let pinned =
            PinnedVfio::inspect_parent(parent.members()).map_err(device_inspection_status)?;
        let exclusion = pinned.exclude_holders().map_err(device_inspection_status)?;
        let observed = pinned
            .devices()
            .iter()
            .map(observe_device_identity)
            .collect::<Result<Vec<_>, _>>()?;
        if observed != parent.enrolled_members() {
            return Err(Status::failed_precondition(
                "enrollment identity changed before reset",
            ));
        }
        let profile = super::device_admin::compiled_profile(parent.profile())?;
        profile
            .check_pci_hardware(
                &observed
                    .iter()
                    .map(EnrollmentIdentity::pci_identifiers)
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        pinned
            .reset_single_device_bus(&exclusion)
            .map_err(device_inspection_status)?;
        drop(exclusion);
        Ok(())
    }
}

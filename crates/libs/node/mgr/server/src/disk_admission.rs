use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pplx_space_common_core::host::disk_admission::{
    CacheDiskAdmissionConfig, CacheDiskAdmissionDecision, CacheDiskAdmissionReason,
    CacheDiskAdmissionState as CoreCacheDiskAdmissionState, CacheDiskMeasurement,
    CacheDiskProbeError, CacheDiskRefreshFailure, read_cache_disk_measurement,
};
use pplx_space_common_svc::disk_admission::CacheDiskAdmissionHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::Status;

use crate::proto::{CacheDiskAdmissionState as ProtoCacheDiskAdmissionState, CacheDiskStatus};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskAllocationOperation {
    PrepareSandbox,
    PrepareExternalRootfs,
    BtrfsReceiveInstance,
}

impl DiskAllocationOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PrepareSandbox => "prepare_sandbox",
            Self::PrepareExternalRootfs => "prepare_external_rootfs",
            Self::BtrfsReceiveInstance => "btrfs_receive_instance",
        }
    }
}

type CacheDiskProbe =
    Arc<dyn Fn() -> Result<CacheDiskMeasurement, CacheDiskProbeError> + Send + Sync>;

/// A completed allocation's reservation, kept charged after its follow-up
/// probe failed. It is released once a refresh newer than that failure
/// succeeds, because only then does the measurement include the allocation.
struct DeferredRelease {
    successful_refreshes_at_failure: u64,
    _reservation: DiskAdmissionReservation,
}

#[derive(Clone)]
pub struct DiskAdmission {
    handle: CacheDiskAdmissionHandle,
    pending_reservation_bytes: Arc<AtomicU64>,
    unknown_reservations: Arc<AtomicU64>,
    // Bounded by allocations completed while probes keep failing; once the
    // last good measurement exceeds its TTL, admission closes.
    deferred_releases: Arc<Mutex<Vec<DeferredRelease>>>,
    probe: CacheDiskProbe,
}

impl std::fmt::Debug for DiskAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskAdmission")
            .field("handle", &self.handle)
            .field("pending_reservation_bytes", &self.pending_reservation_bytes)
            .field("unknown_reservations", &self.unknown_reservations)
            .finish_non_exhaustive()
    }
}

impl DiskAdmission {
    pub async fn new(config: CacheDiskAdmissionConfig) -> Self {
        Self::with_probe(config, Arc::new(read_cache_disk_measurement)).await
    }

    async fn with_probe(config: CacheDiskAdmissionConfig, probe: CacheDiskProbe) -> Self {
        let (handle, initial_refresh) =
            CacheDiskAdmissionHandle::with_initial_refresh(config).await;
        let admission = Self {
            handle,
            pending_reservation_bytes: Arc::new(AtomicU64::new(0)),
            unknown_reservations: Arc::new(AtomicU64::new(0)),
            deferred_releases: Arc::new(Mutex::new(Vec::new())),
            probe,
        };
        if config.enabled {
            if let Err(error) = initial_refresh {
                tracing::warn!(%error, "initial disk admission refresh failed");
            }
            admission.emit_metrics();
        }
        admission
    }

    pub async fn from_env() -> Self {
        Self::new(CacheDiskAdmissionConfig::from_env()).await
    }

    pub fn config(&self) -> CacheDiskAdmissionConfig {
        self.handle.config()
    }

    pub fn pending_reservation_bytes(&self) -> u64 {
        self.pending_reservation_bytes.load(Ordering::Acquire)
    }

    pub fn cache_disk_status(&self) -> CacheDiskStatus {
        cache_disk_status_from_decision(&self.decision())
    }

    fn decision(&self) -> CacheDiskAdmissionDecision {
        // Observe the hold before sampling bytes: recovery publishes the known
        // charge before releasing its hold. The reverse order can miss both.
        let unknown = self.unknown_reservations.load(Ordering::Acquire);
        self.settle_deferred_releases();
        let mut decision = self.handle.evaluate(self.pending_reservation_bytes());
        if unknown != 0 {
            decision.state = CoreCacheDiskAdmissionState::Unavailable;
            decision.reason = CacheDiskAdmissionReason::DiskUnknownStale;
            decision.detail = format!(
                "{unknown} prepared rootfs recovery obligations have unknown disk reservations"
            );
        }
        decision
    }

    /// Free-space probes cannot reconstruct promises made to surviving writers.
    /// Keep admission closed until their markers are repaired or cleanup is confirmed.
    pub(crate) fn hold_unknown_reservation(&self) -> DiskAdmissionReservation {
        self.unknown_reservations.fetch_add(1, Ordering::AcqRel);
        DiskAdmissionReservation {
            counter: Arc::clone(&self.unknown_reservations),
            amount: 1,
            released: false,
        }
    }

    /// Releases deferred reservations once a newer refresh has succeeded. The
    /// cached measurement read afterwards is at least that new, so it already
    /// reflects their allocations.
    fn settle_deferred_releases(&self) {
        let successful_refreshes = self.handle.successful_refreshes();
        self.deferred_releases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|deferred| deferred.successful_refreshes_at_failure >= successful_refreshes);
    }

    fn emit_metrics(&self) {
        let decision = self.decision();
        pplx_space_common_svc::disk_admission::emit_cache_disk_admission_metrics(&decision);
    }

    async fn refresh_once_result(&self, on_failure: CacheDiskRefreshFailure) -> Result<(), String> {
        let probe = Arc::clone(&self.probe);
        let result = pplx_space_common_svc::disk_admission::refresh_cache_disk_admission_once_with(
            &self.handle,
            on_failure,
            move || probe(),
        )
        .await;
        self.emit_metrics();
        result
    }

    pub fn start_refresh_loop(
        self: Arc<Self>,
        shutdown_token: CancellationToken,
        task_tracker: &TaskTracker,
    ) {
        if !self.config().enabled {
            return;
        }
        let handle = self.handle.clone();
        let admission = Arc::clone(&self);
        task_tracker.spawn(handle.start_refresh_loop(
            "space-node-manager",
            shutdown_token,
            move |_| admission.emit_metrics(),
        ));
    }

    pub fn reserve(
        &self,
        operation: DiskAllocationOperation,
        requested_bytes: Option<u64>,
    ) -> Result<DiskAdmissionReservation, Status> {
        let reservation_bytes = self.reservation_bytes(requested_bytes);
        let pending_after = self
            .pending_reservation_bytes
            .fetch_add(reservation_bytes, Ordering::AcqRel)
            .saturating_add(reservation_bytes);
        let decision = self.decision();
        pplx_space_common_svc::disk_admission::emit_cache_disk_admission_metrics(&decision);

        if decision.allows_new_allocation() {
            tracing::debug!(
                operation_class = operation.as_str(),
                reservation_bytes,
                pending_after,
                reason = %decision.reason,
                "disk allocation admitted"
            );
            return Ok(DiskAdmissionReservation {
                counter: Arc::clone(&self.pending_reservation_bytes),
                amount: reservation_bytes,
                released: false,
            });
        }

        self.pending_reservation_bytes
            .fetch_sub(reservation_bytes, Ordering::AcqRel);
        tracing::warn!(
            operation_class = operation.as_str(),
            reservation_bytes,
            pending_after,
            reason = %decision.reason,
            detail = %decision.detail,
            "disk allocation rejected"
        );
        Err(Status::resource_exhausted(format!(
            "node disk admission rejected {}: {}",
            operation.as_str(),
            decision.reason
        )))
    }

    /// Restores accounting for an allocation that was already in progress
    /// when node-manager restarted. It cannot be rejected after work began.
    pub fn restore_reservation(
        &self,
        operation: DiskAllocationOperation,
        requested_bytes: Option<u64>,
    ) -> DiskAdmissionReservation {
        let reservation_bytes = self.reservation_bytes(requested_bytes);
        let pending_after = self
            .pending_reservation_bytes
            .fetch_add(reservation_bytes, Ordering::AcqRel)
            .saturating_add(reservation_bytes);
        self.emit_metrics();
        tracing::info!(
            operation_class = operation.as_str(),
            reservation_bytes,
            pending_after,
            "restored disk allocation reservation"
        );
        DiskAdmissionReservation {
            counter: Arc::clone(&self.pending_reservation_bytes),
            amount: reservation_bytes,
            released: false,
        }
    }

    fn reservation_bytes(&self, requested_bytes: Option<u64>) -> u64 {
        let configured_bytes = self.config().allocation_reservation_bytes;
        requested_bytes
            .unwrap_or(configured_bytes)
            .max(configured_bytes)
    }

    /// Releases a completed allocation's reservation against a measurement
    /// that includes it. If that probe fails, the reservation stays charged
    /// until a later probe succeeds instead of closing admission for everyone.
    pub async fn release_after_success(&self, reservation: DiskAdmissionReservation) {
        if !self.config().enabled {
            reservation.release();
            return;
        }
        let Err(error) = self
            .refresh_once_result(CacheDiskRefreshFailure::KeepLastGood)
            .await
        else {
            reservation.release();
            return;
        };
        tracing::warn!(
            %error,
            "fresh disk measurement failed after allocation; keeping its reservation until a later measurement"
        );
        let successful_refreshes_at_failure = self.handle.successful_refreshes();
        self.deferred_releases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(DeferredRelease {
                successful_refreshes_at_failure,
                _reservation: reservation,
            });
    }

    /// Refreshes after space was consumed without a reservation to keep
    /// charged. A failed probe invalidates the last good measurement.
    pub async fn refresh_after_external_allocation(&self) {
        if let Err(error) = self
            .refresh_once_result(CacheDiskRefreshFailure::Invalidate)
            .await
        {
            tracing::warn!(%error, "fresh disk measurement failed after external allocation");
        }
    }
}

fn cache_disk_status_from_decision(decision: &CacheDiskAdmissionDecision) -> CacheDiskStatus {
    let status = decision
        .measurement
        .as_ref()
        .map(|measurement| measurement.status);
    let btrfs = decision
        .measurement
        .as_ref()
        .and_then(|measurement| measurement.btrfs);
    CacheDiskStatus {
        statvfs_total_bytes: status.map_or(0, |status| status.total_bytes),
        statvfs_free_bytes: status.map_or(0, |status| status.free_bytes),
        statvfs_available_bytes: status.map_or(0, |status| status.available_bytes),
        statvfs_available_fraction: status.map_or(0.0, |status| status.available_fraction()),
        effective_statvfs_available_bytes: decision.effective_available_bytes,
        pending_reservation_bytes: decision.pending_reservation_bytes,
        admission_state: proto_cache_disk_admission_state(decision.state).into(),
        admission_reason: decision.reason.to_string(),
        admission_detail: decision.detail.clone(),
        btrfs_metadata_available_bytes: btrfs.map(|usage| usage.metadata_available_bytes()),
        btrfs_device_unallocated_bytes: btrfs.map(|usage| usage.device_unallocated_bytes),
    }
}

fn proto_cache_disk_admission_state(
    state: CoreCacheDiskAdmissionState,
) -> ProtoCacheDiskAdmissionState {
    match state {
        CoreCacheDiskAdmissionState::Ready => ProtoCacheDiskAdmissionState::Ready,
        CoreCacheDiskAdmissionState::Warning => ProtoCacheDiskAdmissionState::Warning,
        CoreCacheDiskAdmissionState::Full => ProtoCacheDiskAdmissionState::Full,
        CoreCacheDiskAdmissionState::Unavailable => ProtoCacheDiskAdmissionState::Unavailable,
    }
}

pub struct DiskAdmissionReservation {
    // Either reserved bytes or one unknown recovery obligation; both are owned
    // until successful completion/cleanup, independently of disk measurements.
    counter: Arc<AtomicU64>,
    amount: u64,
    released: bool,
}

impl DiskAdmissionReservation {
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        self.counter.fetch_sub(self.amount, Ordering::AcqRel);
        self.released = true;
    }
}

impl Drop for DiskAdmissionReservation {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[cfg(test)]
mod tests;

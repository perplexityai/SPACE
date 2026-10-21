use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_core::host::disk_admission::{
    CacheDiskAdmissionConfig, CacheDiskAdmissionDecision, CacheDiskAdmissionMonitor,
    CacheDiskAdmissionReason, CacheDiskAdmissionState, CacheDiskMeasurement, CacheDiskProbeError,
    CacheDiskRefreshFailure, read_cache_disk_measurement,
};
use tokio_util::sync::CancellationToken;

pub const CACHE_DISK_ADMISSION_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Debug)]
pub struct CacheDiskAdmissionHandle {
    config: CacheDiskAdmissionConfig,
    monitor: Arc<CacheDiskAdmissionMonitor>,
}

impl CacheDiskAdmissionHandle {
    fn new(config: CacheDiskAdmissionConfig) -> Self {
        Self {
            config,
            monitor: Arc::new(CacheDiskAdmissionMonitor::new()),
        }
    }

    pub async fn with_initial_refresh(
        config: CacheDiskAdmissionConfig,
    ) -> (Self, Result<(), String>) {
        let handle = Self::new(config);
        let refresh_result = if config.enabled {
            refresh_cache_disk_admission_once(&handle).await
        } else {
            Ok(())
        };
        (handle, refresh_result)
    }

    pub fn config(&self) -> CacheDiskAdmissionConfig {
        self.config
    }

    /// See [`CacheDiskAdmissionMonitor::successful_refreshes`].
    pub fn successful_refreshes(&self) -> u64 {
        self.monitor.successful_refreshes()
    }

    pub fn evaluate(&self, pending_reservation_bytes: u64) -> CacheDiskAdmissionDecision {
        self.monitor
            .evaluate_cached(self.config, pending_reservation_bytes)
    }

    pub fn start_refresh_loop<F>(
        &self,
        name: &'static str,
        shutdown_token: CancellationToken,
        on_refresh: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(&CacheDiskAdmissionHandle) + Send + Sync + 'static,
    {
        tokio::spawn(run_cache_disk_admission_refresh_loop(
            name,
            self.clone(),
            shutdown_token,
            on_refresh,
        ))
    }
}

pub async fn start_cache_disk_admission_refresh_loop<F>(
    name: &'static str,
    config: CacheDiskAdmissionConfig,
    shutdown_token: CancellationToken,
    on_refresh: F,
) -> (CacheDiskAdmissionHandle, tokio::task::JoinHandle<()>)
where
    F: Fn(&CacheDiskAdmissionHandle) + Send + Sync + 'static,
{
    let (handle, initial_refresh) = CacheDiskAdmissionHandle::with_initial_refresh(config).await;
    if !config.enabled {
        return (handle, tokio::spawn(async {}));
    }
    if let Err(error) = initial_refresh {
        tracing::warn!(%error, "initial cache disk admission refresh failed");
    }
    on_refresh(&handle);
    let refresh_task = handle.start_refresh_loop(name, shutdown_token, on_refresh);
    (handle, refresh_task)
}

/// Periodic refresh: a failed probe keeps the last good measurement until its TTL.
pub async fn refresh_cache_disk_admission_once(
    handle: &CacheDiskAdmissionHandle,
) -> Result<(), String> {
    refresh_cache_disk_admission_once_with(
        handle,
        CacheDiskRefreshFailure::KeepLastGood,
        read_cache_disk_measurement,
    )
    .await
}

pub async fn refresh_cache_disk_admission_once_with<P>(
    handle: &CacheDiskAdmissionHandle,
    on_failure: CacheDiskRefreshFailure,
    probe: P,
) -> Result<(), String>
where
    P: FnOnce() -> Result<CacheDiskMeasurement, CacheDiskProbeError> + Send + 'static,
{
    tokio::task::spawn_blocking({
        let monitor = Arc::clone(&handle.monitor);
        move || monitor.refresh_with(on_failure, probe)
    })
    .await
    .map_err(|error| format!("disk admission refresh task failed: {error}"))?
    .map(|_| ())
    .map_err(|error| error.to_string())
}

async fn run_cache_disk_admission_refresh_loop<F>(
    name: &'static str,
    handle: CacheDiskAdmissionHandle,
    shutdown_token: CancellationToken,
    on_refresh: F,
) where
    F: Fn(&CacheDiskAdmissionHandle) + Send + Sync + 'static,
{
    loop {
        if let Err(error) = refresh_cache_disk_admission_once(&handle).await {
            tracing::warn!(%error, "cache disk admission refresh failed");
        }
        on_refresh(&handle);

        tokio::select! {
            () = shutdown_token.cancelled() => break,
            () = tokio::time::sleep(CACHE_DISK_ADMISSION_REFRESH_INTERVAL) => {}
        }
    }
    tracing::debug!(name, "cache disk admission refresh loop stopped");
}

pub fn emit_cache_disk_admission_metrics(decision: &CacheDiskAdmissionDecision) {
    if let Some(measurement) = decision.measurement {
        pplx_space_metrics::node_disk::sandbox_admission_available_fraction(
            measurement.status.available_fraction(),
        );
        pplx_space_metrics::node_disk::sandbox_admission_available_bytes(
            measurement.status.available_bytes,
        );
        let metadata_pressure = matches!(
            decision.reason,
            CacheDiskAdmissionReason::BtrfsAllocationPressure
        );
        pplx_space_metrics::node_disk::sandbox_admission_btrfs_allocation_pressure(
            "metadata_growth",
            metadata_pressure,
        );
    }
    pplx_space_metrics::node_disk::sandbox_admission_effective_available_bytes(
        decision.effective_available_bytes,
    );
    pplx_space_metrics::node_disk::sandbox_admission_pending_reservation_bytes(
        decision.pending_reservation_bytes,
    );
    if matches!(
        decision.state,
        CacheDiskAdmissionState::Warning | CacheDiskAdmissionState::Full
    ) {
        tracing::warn!(
            state = ?decision.state,
            reason = %decision.reason,
            effective_available_bytes = decision.effective_available_bytes,
            pending_reservation_bytes = decision.pending_reservation_bytes,
            hard_floor_bytes = decision.hard_floor_bytes,
            "node disk admission threshold crossed"
        );
    }
}

//! Node-local `/space/cache` allocation admission.

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::host::btrfs::{BtrfsError, FilesystemUsage, read_filesystem_usage};
use crate::host::disk::{DiskStatus, read_cache_disk_status};

const GIB: u64 = 1024 * 1024 * 1024;

pub const DEFAULT_MIN_AVAILABLE_FRACTION: f64 = 0.05;
pub const DEFAULT_WARNING_AVAILABLE_FRACTION: f64 = 0.10;
pub const DEFAULT_MIN_AVAILABLE_BYTES: u64 = 50 * GIB;
pub const DEFAULT_ALLOCATION_RESERVATION_BYTES: u64 = 8 * GIB;
pub const DEFAULT_BTRFS_METADATA_MIN_AVAILABLE_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_BTRFS_UNALLOCATED_MIN_BYTES: u64 = GIB;
pub const DEFAULT_MEASUREMENT_TTL: Duration = Duration::from_secs(30);

const ENV_MIN_AVAILABLE_BYTES: &str = "SPACE_NODE_DISK_ADMISSION_MIN_AVAILABLE_BYTES";
const TEST_HOOK_ALLOCATION_RESERVATION_BYTES: &str =
    "SPACE_NODE_DISK_ADMISSION_ALLOCATION_RESERVATION_BYTES";

#[derive(Clone, Copy, Debug)]
pub struct CacheDiskAdmissionConfig {
    pub enabled: bool,
    pub min_available_fraction: f64,
    pub warning_available_fraction: f64,
    pub min_available_bytes: u64,
    pub allocation_reservation_bytes: u64,
}

impl Default for CacheDiskAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_available_fraction: DEFAULT_MIN_AVAILABLE_FRACTION,
            warning_available_fraction: DEFAULT_WARNING_AVAILABLE_FRACTION,
            min_available_bytes: DEFAULT_MIN_AVAILABLE_BYTES,
            allocation_reservation_bytes: DEFAULT_ALLOCATION_RESERVATION_BYTES,
        }
    }
}

impl CacheDiskAdmissionConfig {
    /// Fraction thresholds, the enabled switch and the per-allocation
    /// reservation are fixed; only the byte floor is tunable from the
    /// environment (the reservation has a test hook for small kind caches).
    pub fn from_env() -> Self {
        let default = Self::default();
        Self {
            min_available_bytes: env_u64(ENV_MIN_AVAILABLE_BYTES, default.min_available_bytes),
            // Kind and tests shrink the reservation to fit a small cache
            // filesystem; release binaries never read it.
            allocation_reservation_bytes: crate::env::test_hook_var(
                TEST_HOOK_ALLOCATION_RESERVATION_BYTES,
            )
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(default.allocation_reservation_bytes),
            ..default
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "fraction is normalized into 0..=1 before conversion"
    )]
    pub fn fraction_floor_bytes(self, total_bytes: u64) -> u64 {
        (total_bytes as f64 * self.min_available_fraction.clamp(0.0, 1.0)).ceil() as u64
    }

    pub fn hard_floor_bytes(self, total_bytes: u64) -> u64 {
        self.fraction_floor_bytes(total_bytes)
            .max(self.min_available_bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CacheDiskMeasurement {
    pub status: DiskStatus,
    pub btrfs: Option<FilesystemUsage>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheDiskAdmissionState {
    Ready,
    Warning,
    Full,
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheDiskSchedulingEffect {
    Open,
    WarnOnly,
    Full,
    Unavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheDiskAdmissionReason {
    Disabled,
    Open,
    WarningAvailable,
    DiskUnknownStale,
    DiskAvailableBelowThreshold,
    BtrfsAllocationPressure,
}

impl CacheDiskAdmissionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disk_admission_disabled",
            Self::Open => "disk_open",
            Self::WarningAvailable => "disk_available_warning",
            Self::DiskUnknownStale => "disk_unknown_stale",
            Self::DiskAvailableBelowThreshold => "disk_available_below_threshold",
            Self::BtrfsAllocationPressure => "btrfs_allocation_pressure",
        }
    }
}

impl std::fmt::Display for CacheDiskAdmissionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct CacheDiskAdmissionDecision {
    pub state: CacheDiskAdmissionState,
    pub reason: CacheDiskAdmissionReason,
    pub detail: String,
    pub measurement: Option<CacheDiskMeasurement>,
    pub effective_available_bytes: u64,
    pub pending_reservation_bytes: u64,
    pub hard_floor_bytes: u64,
    pub stale_measurement: bool,
}

impl CacheDiskAdmissionDecision {
    pub fn allows_new_allocation(&self) -> bool {
        matches!(
            self.scheduling_effect(),
            CacheDiskSchedulingEffect::Open | CacheDiskSchedulingEffect::WarnOnly
        )
    }

    pub fn scheduling_effect(&self) -> CacheDiskSchedulingEffect {
        match self.state {
            CacheDiskAdmissionState::Ready => CacheDiskSchedulingEffect::Open,
            CacheDiskAdmissionState::Warning => CacheDiskSchedulingEffect::WarnOnly,
            CacheDiskAdmissionState::Full => CacheDiskSchedulingEffect::Full,
            CacheDiskAdmissionState::Unavailable => CacheDiskSchedulingEffect::Unavailable,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CacheDiskProbeError {
    #[error("statvfs /space/cache failed")]
    Statvfs,
    /// statvfs succeeded; `status` is that fresh reading.
    #[error("btrfs filesystem usage /space/cache failed: {detail}")]
    Btrfs { status: DiskStatus, detail: String },
}

/// Why a refresh ran, which decides what a failed probe does to the cached
/// measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheDiskRefreshKind {
    /// A scheduled refresh. A failure keeps the last measurement, or, when
    /// only the btrfs probe failed, publishes the fresh statvfs reading with
    /// the last btrfs usage and its timestamp. [`DEFAULT_MEASUREMENT_TTL`]
    /// bounds how long admission trusts either, so only a run of consecutive
    /// failures closes admission.
    Periodic,
    /// A refresh after an allocation consumed space. A failure never keeps
    /// statvfs free space measured before that allocation, because the caller
    /// is about to release the reservation that covered it.
    AfterAllocation,
}

#[derive(Clone, Copy, Debug)]
struct CachedMeasurement {
    measured_at: Instant,
    measurement: CacheDiskMeasurement,
}

#[derive(Debug, Default)]
pub struct CacheDiskAdmissionMonitor {
    last_good: Mutex<Option<CachedMeasurement>>,
    refresh_lock: Mutex<()>,
}

impl CacheDiskAdmissionMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn evaluate_cached(
        &self,
        config: CacheDiskAdmissionConfig,
        pending_reservation_bytes: u64,
    ) -> CacheDiskAdmissionDecision {
        self.evaluate_cached_at(Instant::now(), config, pending_reservation_bytes)
    }

    fn evaluate_cached_at(
        &self,
        now: Instant,
        config: CacheDiskAdmissionConfig,
        pending_reservation_bytes: u64,
    ) -> CacheDiskAdmissionDecision {
        if !config.enabled {
            return disabled_decision(pending_reservation_bytes);
        }

        let Some(cached) = self.cached_measurement() else {
            return unavailable_decision(
                config,
                pending_reservation_bytes,
                "no cached disk measurement available".to_string(),
            );
        };

        if now.saturating_duration_since(cached.measured_at) > DEFAULT_MEASUREMENT_TTL {
            return unavailable_decision(
                config,
                pending_reservation_bytes,
                "cached disk measurement is stale".to_string(),
            );
        }

        evaluate_cache_disk_measurement(config, cached.measurement, pending_reservation_bytes)
    }

    pub fn refresh(
        &self,
        kind: CacheDiskRefreshKind,
    ) -> Result<CacheDiskMeasurement, CacheDiskProbeError> {
        self.refresh_with(kind, Instant::now, read_cache_disk_measurement)
    }

    fn refresh_with(
        &self,
        kind: CacheDiskRefreshKind,
        now: impl FnOnce() -> Instant,
        probe: impl FnOnce() -> Result<CacheDiskMeasurement, CacheDiskProbeError>,
    ) -> Result<CacheDiskMeasurement, CacheDiskProbeError> {
        // The blocking job owns this lock, including when its async waiter is
        // cancelled. Never publish an older probe after releasing a reservation
        // against a newer observation, or make a slow observation look fresh.
        let _refresh = self.refresh_lock.lock().unwrap_or_else(|e| e.into_inner());
        let measured_at = now();
        let result = probe();
        let mut last_good = self.last_good.lock().unwrap_or_else(|e| e.into_inner());
        *last_good = match &result {
            Ok(measurement) => Some(CachedMeasurement {
                measured_at,
                measurement: *measurement,
            }),
            Err(error) => retained_after_failed_refresh(*last_good, kind, error),
        };
        result
    }

    fn cached_measurement(&self) -> Option<CachedMeasurement> {
        let last_good = self
            .last_good
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *last_good
    }
}

/// The measurement to keep after a failed probe; `None` closes admission.
fn retained_after_failed_refresh(
    previous: Option<CachedMeasurement>,
    kind: CacheDiskRefreshKind,
    error: &CacheDiskProbeError,
) -> Option<CachedMeasurement> {
    let previous_btrfs = previous.and_then(|cached| {
        let btrfs = cached.measurement.btrfs?;
        Some((cached.measured_at, btrfs))
    });
    if let (CacheDiskProbeError::Btrfs { status, .. }, Some((measured_at, btrfs))) =
        (error, previous_btrfs)
    {
        // The fresh statvfs reading already reflects any finished allocation,
        // so it is safe to publish even after one. The btrfs usage beside it
        // may be up to the TTL old and may predate allocations that finished
        // since; keeping its timestamp stops the merge from extending that.
        return Some(CachedMeasurement {
            measured_at,
            measurement: CacheDiskMeasurement {
                status: *status,
                btrfs: Some(btrfs),
            },
        });
    }
    match kind {
        CacheDiskRefreshKind::Periodic => previous,
        CacheDiskRefreshKind::AfterAllocation => None,
    }
}

pub fn read_cache_disk_measurement() -> Result<CacheDiskMeasurement, CacheDiskProbeError> {
    let status = read_cache_disk_status().ok_or(CacheDiskProbeError::Statvfs)?;
    let path = Path::new(crate::host::linux::paths::SPACE_CACHE_DIR);
    let btrfs = match read_filesystem_usage(path) {
        Ok(usage) => Some(usage),
        Err(error) if is_non_btrfs_usage_error(&error) => None,
        Err(error) => {
            return Err(CacheDiskProbeError::Btrfs {
                status,
                detail: error.to_string(),
            });
        }
    };
    Ok(CacheDiskMeasurement { status, btrfs })
}

fn is_non_btrfs_usage_error(error: &BtrfsError) -> bool {
    match error {
        BtrfsError::SpawnBtrfs(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        BtrfsError::CliFailed { stderr, .. } => {
            let stderr = stderr.to_ascii_lowercase();
            stderr.contains("not a btrfs") || stderr.contains("not btrfs")
        }
        _ => false,
    }
}

pub fn evaluate_cache_disk_measurement(
    config: CacheDiskAdmissionConfig,
    measurement: CacheDiskMeasurement,
    pending_reservation_bytes: u64,
) -> CacheDiskAdmissionDecision {
    if !config.enabled {
        return disabled_decision(pending_reservation_bytes);
    }

    let hard_floor_bytes = if measurement.btrfs.is_some() {
        config.hard_floor_bytes(measurement.status.total_bytes)
    } else {
        config.fraction_floor_bytes(measurement.status.total_bytes)
    };
    let effective_available_bytes = measurement
        .status
        .available_bytes
        .saturating_sub(pending_reservation_bytes);

    if btrfs_allocation_pressure(measurement, hard_floor_bytes) {
        return CacheDiskAdmissionDecision {
            state: CacheDiskAdmissionState::Full,
            reason: CacheDiskAdmissionReason::BtrfsAllocationPressure,
            detail: "btrfs metadata is low and cannot safely grow from unallocated space"
                .to_string(),
            measurement: Some(measurement),
            effective_available_bytes,
            pending_reservation_bytes,
            hard_floor_bytes,
            stale_measurement: false,
        };
    }

    if effective_available_bytes < hard_floor_bytes {
        return CacheDiskAdmissionDecision {
            state: CacheDiskAdmissionState::Full,
            reason: CacheDiskAdmissionReason::DiskAvailableBelowThreshold,
            detail: format!(
                "effective available bytes {effective_available_bytes} below floor {hard_floor_bytes}"
            ),
            measurement: Some(measurement),
            effective_available_bytes,
            pending_reservation_bytes,
            hard_floor_bytes,
            stale_measurement: false,
        };
    }

    if measurement.status.available_fraction() < config.warning_available_fraction {
        return CacheDiskAdmissionDecision {
            state: CacheDiskAdmissionState::Warning,
            reason: CacheDiskAdmissionReason::WarningAvailable,
            detail: "disk available fraction below warning threshold".to_string(),
            measurement: Some(measurement),
            effective_available_bytes,
            pending_reservation_bytes,
            hard_floor_bytes,
            stale_measurement: false,
        };
    }

    CacheDiskAdmissionDecision {
        state: CacheDiskAdmissionState::Ready,
        reason: CacheDiskAdmissionReason::Open,
        detail: "disk admission open".to_string(),
        measurement: Some(measurement),
        effective_available_bytes,
        pending_reservation_bytes,
        hard_floor_bytes,
        stale_measurement: false,
    }
}

fn btrfs_allocation_pressure(measurement: CacheDiskMeasurement, hard_floor_bytes: u64) -> bool {
    let Some(btrfs) = measurement.btrfs else {
        return false;
    };
    let metadata_low = btrfs.metadata_total_bytes > 0
        && btrfs.metadata_available_bytes() < DEFAULT_BTRFS_METADATA_MIN_AVAILABLE_BYTES;
    let cannot_grow_metadata =
        btrfs.device_unallocated_bytes < DEFAULT_BTRFS_UNALLOCATED_MIN_BYTES.min(hard_floor_bytes);
    metadata_low && cannot_grow_metadata
}

fn disabled_decision(pending_reservation_bytes: u64) -> CacheDiskAdmissionDecision {
    CacheDiskAdmissionDecision {
        state: CacheDiskAdmissionState::Ready,
        reason: CacheDiskAdmissionReason::Disabled,
        detail: "disk admission disabled".to_string(),
        measurement: None,
        effective_available_bytes: u64::MAX,
        pending_reservation_bytes,
        hard_floor_bytes: 0,
        stale_measurement: false,
    }
}

fn unavailable_decision(
    config: CacheDiskAdmissionConfig,
    pending_reservation_bytes: u64,
    detail: String,
) -> CacheDiskAdmissionDecision {
    CacheDiskAdmissionDecision {
        state: CacheDiskAdmissionState::Unavailable,
        reason: CacheDiskAdmissionReason::DiskUnknownStale,
        detail,
        measurement: None,
        effective_available_bytes: 0,
        pending_reservation_bytes,
        hard_floor_bytes: config.min_available_bytes,
        stale_measurement: true,
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests;

use super::*;

fn config() -> CacheDiskAdmissionConfig {
    CacheDiskAdmissionConfig {
        enabled: true,
        min_available_fraction: 0.05,
        warning_available_fraction: 0.10,
        min_available_bytes: 30,
        allocation_reservation_bytes: 10,
    }
}

fn measurement(available: u64, total: u64) -> CacheDiskMeasurement {
    CacheDiskMeasurement {
        status: DiskStatus {
            total_bytes: total,
            free_bytes: available,
            available_bytes: available,
        },
        btrfs: Some(FilesystemUsage {
            device_unallocated_bytes: 10 * GIB,
            metadata_total_bytes: 2 * GIB,
            metadata_used_bytes: GIB,
            data_total_bytes: 100 * GIB,
            data_used_bytes: 90 * GIB,
        }),
    }
}

#[test]
fn effective_available_below_absolute_floor_is_full() {
    let decision = evaluate_cache_disk_measurement(config(), measurement(29, 1_000), 0);

    assert_eq!(decision.state, CacheDiskAdmissionState::Full);
    assert_eq!(
        decision.reason,
        CacheDiskAdmissionReason::DiskAvailableBelowThreshold
    );
}

#[test]
fn pending_reservations_subtract_from_available_bytes() {
    let decision = evaluate_cache_disk_measurement(config(), measurement(39, 1_000), 10);

    assert_eq!(decision.state, CacheDiskAdmissionState::Full);
    assert_eq!(decision.effective_available_bytes, 29);
}

#[test]
fn scheduling_effect_is_the_single_admission_policy_mapping() {
    let open = evaluate_cache_disk_measurement(config(), measurement(500, 1_000), 0);
    let warning = evaluate_cache_disk_measurement(config(), measurement(75, 1_000), 0);
    let full = evaluate_cache_disk_measurement(config(), measurement(29, 1_000), 0);
    let unavailable = CacheDiskAdmissionDecision {
        state: CacheDiskAdmissionState::Unavailable,
        reason: CacheDiskAdmissionReason::DiskUnknownStale,
        detail: "probe failed".to_string(),
        measurement: None,
        effective_available_bytes: 0,
        pending_reservation_bytes: 0,
        hard_floor_bytes: 0,
        stale_measurement: true,
    };

    assert_eq!(open.scheduling_effect(), CacheDiskSchedulingEffect::Open);
    assert_eq!(
        warning.scheduling_effect(),
        CacheDiskSchedulingEffect::WarnOnly
    );
    assert_eq!(full.scheduling_effect(), CacheDiskSchedulingEffect::Full);
    assert_eq!(
        unavailable.scheduling_effect(),
        CacheDiskSchedulingEffect::Unavailable
    );
    assert!(open.allows_new_allocation());
    assert!(warning.allows_new_allocation());
    assert!(!full.allows_new_allocation());
    assert!(!unavailable.allows_new_allocation());
}

#[test]
fn cached_evaluation_uses_last_measurement_without_refreshing() {
    let monitor = CacheDiskAdmissionMonitor::new();
    monitor
        .refresh_with(CacheDiskRefreshFailure::KeepLastGood, || {
            Ok(measurement(500, 1_000))
        })
        .unwrap();

    let decision = monitor.evaluate_cached(config(), 490);

    assert_eq!(decision.state, CacheDiskAdmissionState::Full);
    assert_eq!(decision.effective_available_bytes, 10);
    monitor
        .refresh_with(CacheDiskRefreshFailure::Invalidate, || {
            Err(CacheDiskProbeError::Statvfs)
        })
        .unwrap_err();
    assert_eq!(
        monitor.evaluate_cached(config(), 0).state,
        CacheDiskAdmissionState::Unavailable
    );
}

#[test]
fn cached_evaluation_without_measurement_is_unavailable() {
    let monitor = CacheDiskAdmissionMonitor::new();

    let decision = monitor.evaluate_cached(config(), 0);

    assert_eq!(decision.state, CacheDiskAdmissionState::Unavailable);
    assert_eq!(decision.reason, CacheDiskAdmissionReason::DiskUnknownStale);
}

#[test]
fn refresh_serializes_probe_and_publication() {
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    let monitor = CacheDiskAdmissionMonitor::new();
    let started = Arc::new(Barrier::new(2));
    let (release, wait) = mpsc::channel();
    let (entered, observed) = mpsc::channel();
    std::thread::scope(|scope| {
        let first_started = Arc::clone(&started);
        let monitor = &monitor;
        scope.spawn(move || {
            monitor
                .refresh_with(CacheDiskRefreshFailure::KeepLastGood, || {
                    first_started.wait();
                    wait.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(measurement(500, 1_000))
                })
                .unwrap();
        });
        started.wait();
        scope.spawn(|| {
            monitor
                .refresh_with(CacheDiskRefreshFailure::KeepLastGood, || {
                    entered.send(()).unwrap();
                    Ok(measurement(100, 1_000))
                })
                .unwrap();
        });
        let premature = observed.recv_timeout(Duration::from_millis(50));
        release.send(()).unwrap();
        assert!(
            premature.is_err(),
            "second probe ran before the first published"
        );
    });
    assert_eq!(
        monitor
            .evaluate_cached(config(), 0)
            .effective_available_bytes,
        100
    );
}

#[test]
fn non_btrfs_usage_errors_are_statvfs_only() {
    let missing_cli = BtrfsError::SpawnBtrfs(std::io::Error::from(std::io::ErrorKind::NotFound));
    assert!(is_non_btrfs_usage_error(&missing_cli));

    let spawn_denied =
        BtrfsError::SpawnBtrfs(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
    assert!(!is_non_btrfs_usage_error(&spawn_denied));

    let not_btrfs = BtrfsError::CliFailed {
        op: "filesystem usage --raw",
        path: Path::new("/space/cache").to_path_buf(),
        stderr: "ERROR: not a btrfs filesystem".to_string(),
    };
    assert!(is_non_btrfs_usage_error(&not_btrfs));

    let parse_failure = BtrfsError::ParseFilesystemUsage {
        path: Path::new("/space/cache").to_path_buf(),
        detail: "missing field".to_string(),
    };
    assert!(!is_non_btrfs_usage_error(&parse_failure));
}

#[test]
fn non_btrfs_measurement_uses_fraction_floor_without_absolute_floor() {
    let mut m = measurement(10, 1_000);
    m.btrfs = None;

    let decision = evaluate_cache_disk_measurement(
        CacheDiskAdmissionConfig {
            min_available_fraction: 0.05,
            min_available_bytes: 100,
            warning_available_fraction: 0.10,
            allocation_reservation_bytes: 10,
            enabled: true,
        },
        m,
        0,
    );

    assert_eq!(decision.state, CacheDiskAdmissionState::Full);
    assert_eq!(decision.hard_floor_bytes, 50);

    m.status.available_bytes = 60;
    m.status.free_bytes = 60;
    let decision = evaluate_cache_disk_measurement(
        CacheDiskAdmissionConfig {
            min_available_fraction: 0.05,
            min_available_bytes: 100,
            warning_available_fraction: 0.10,
            allocation_reservation_bytes: 10,
            enabled: true,
        },
        m,
        0,
    );

    assert_eq!(decision.state, CacheDiskAdmissionState::Warning);
    assert_eq!(decision.hard_floor_bytes, 50);
}

#[test]
fn default_warning_threshold_is_ten_percent_available() {
    let decision = evaluate_cache_disk_measurement(
        CacheDiskAdmissionConfig {
            min_available_bytes: 0,
            ..CacheDiskAdmissionConfig::default()
        },
        measurement(90, 1_000),
        0,
    );

    assert_eq!(decision.state, CacheDiskAdmissionState::Warning);
    assert_eq!(decision.reason, CacheDiskAdmissionReason::WarningAvailable);
}

#[test]
fn default_hard_stop_threshold_is_five_percent_available() {
    let decision = evaluate_cache_disk_measurement(
        CacheDiskAdmissionConfig {
            min_available_bytes: 0,
            ..CacheDiskAdmissionConfig::default()
        },
        measurement(49, 1_000),
        0,
    );

    assert_eq!(decision.state, CacheDiskAdmissionState::Full);
    assert_eq!(
        decision.reason,
        CacheDiskAdmissionReason::DiskAvailableBelowThreshold
    );
}

#[test]
fn btrfs_metadata_pressure_closes_even_when_statvfs_looks_healthy() {
    let mut m = measurement(500, 1_000);
    m.btrfs = Some(FilesystemUsage {
        device_unallocated_bytes: 4,
        metadata_total_bytes: GIB,
        metadata_used_bytes: GIB - 1,
        data_total_bytes: 100 * GIB,
        data_used_bytes: 90 * GIB,
    });

    let decision = evaluate_cache_disk_measurement(config(), m, 0);

    assert_eq!(decision.state, CacheDiskAdmissionState::Full);
    assert_eq!(
        decision.reason,
        CacheDiskAdmissionReason::BtrfsAllocationPressure
    );
}

#[test]
fn disabled_config_keeps_admission_open() {
    let decision = evaluate_cache_disk_measurement(
        CacheDiskAdmissionConfig {
            enabled: false,
            ..config()
        },
        measurement(0, 1_000),
        100,
    );

    assert_eq!(decision.state, CacheDiskAdmissionState::Ready);
    assert_eq!(decision.reason, CacheDiskAdmissionReason::Disabled);
}

#[test]
fn failed_periodic_refresh_keeps_last_good_measurement() {
    let monitor = CacheDiskAdmissionMonitor::new();
    monitor
        .refresh_with(CacheDiskRefreshFailure::KeepLastGood, || {
            Ok(measurement(500, 1_000))
        })
        .unwrap();
    monitor
        .refresh_with(CacheDiskRefreshFailure::KeepLastGood, || {
            Err(CacheDiskProbeError::Btrfs(
                "btrfs filesystem usage --raw timed out".to_string(),
            ))
        })
        .unwrap_err();

    let decision = monitor.evaluate_cached(config(), 0);

    assert_eq!(decision.state, CacheDiskAdmissionState::Ready);
    assert_eq!(decision.effective_available_bytes, 500);
}

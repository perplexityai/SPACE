use super::*;

/// node-manager refreshes the cached measurement on this cadence.
const CACHE_REFRESH_CADENCE: Duration = Duration::from_secs(10);

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
        .refresh_with(CacheDiskRefreshKind::Periodic, Instant::now, || {
            Ok(measurement(500, 1_000))
        })
        .unwrap();

    let decision = monitor.evaluate_cached(config(), 490);

    assert_eq!(decision.state, CacheDiskAdmissionState::Full);
    assert_eq!(decision.effective_available_bytes, 10);
}

#[derive(Clone, Copy, Debug)]
enum ProbeFailure {
    Statvfs,
    /// statvfs read 300 available bytes, then the btrfs probe failed.
    BtrfsAfterFreshStatvfs,
}

#[derive(Clone, Copy, Debug)]
enum PreviousMeasurement {
    None,
    StatvfsOnly,
    WithBtrfs,
}

#[derive(Debug, PartialEq)]
enum Published {
    Nothing,
    Previous,
    FreshStatvfsWithPreviousBtrfs,
}

/// What a failed refresh leaves cached, for every refresh kind, probe failure
/// and earlier measurement.
#[test]
fn failed_refresh_publication_covers_every_kind_failure_and_previous_state() {
    use CacheDiskRefreshKind::{AfterAllocation, Periodic};
    use PreviousMeasurement as Prev;
    use ProbeFailure::{BtrfsAfterFreshStatvfs, Statvfs};

    fn expected(
        kind: CacheDiskRefreshKind,
        failure: ProbeFailure,
        previous: PreviousMeasurement,
    ) -> Published {
        match (kind, failure, previous) {
            (Periodic, Statvfs, Prev::None) => Published::Nothing,
            (Periodic, Statvfs, Prev::StatvfsOnly) => Published::Previous,
            (Periodic, Statvfs, Prev::WithBtrfs) => Published::Previous,
            (Periodic, BtrfsAfterFreshStatvfs, Prev::None) => Published::Nothing,
            (Periodic, BtrfsAfterFreshStatvfs, Prev::StatvfsOnly) => Published::Previous,
            (Periodic, BtrfsAfterFreshStatvfs, Prev::WithBtrfs) => {
                Published::FreshStatvfsWithPreviousBtrfs
            }
            (AfterAllocation, Statvfs, Prev::None) => Published::Nothing,
            (AfterAllocation, Statvfs, Prev::StatvfsOnly) => Published::Nothing,
            (AfterAllocation, Statvfs, Prev::WithBtrfs) => Published::Nothing,
            (AfterAllocation, BtrfsAfterFreshStatvfs, Prev::None) => Published::Nothing,
            (AfterAllocation, BtrfsAfterFreshStatvfs, Prev::StatvfsOnly) => Published::Nothing,
            (AfterAllocation, BtrfsAfterFreshStatvfs, Prev::WithBtrfs) => {
                Published::FreshStatvfsWithPreviousBtrfs
            }
        }
    }

    let t0 = Instant::now();
    let refreshed_at = t0 + CACHE_REFRESH_CADENCE;
    let with_btrfs = measurement(500, 1_000);
    let statvfs_only = CacheDiskMeasurement {
        btrfs: None,
        ..with_btrfs
    };
    let fresh_status = measurement(300, 1_000).status;
    for kind in [Periodic, AfterAllocation] {
        for failure in [Statvfs, BtrfsAfterFreshStatvfs] {
            for previous_state in [Prev::None, Prev::StatvfsOnly, Prev::WithBtrfs] {
                let case = format!("{kind:?} / {failure:?} / previous {previous_state:?}");
                let previous = match previous_state {
                    Prev::None => None,
                    Prev::StatvfsOnly => Some(statvfs_only),
                    Prev::WithBtrfs => Some(with_btrfs),
                };
                let monitor = CacheDiskAdmissionMonitor::new();
                if let Some(previous) = previous {
                    monitor
                        .refresh_with(Periodic, || t0, || Ok(previous))
                        .unwrap();
                }
                let error = match failure {
                    Statvfs => CacheDiskProbeError::Statvfs,
                    BtrfsAfterFreshStatvfs => CacheDiskProbeError::Btrfs {
                        status: fresh_status,
                        detail: "timed out".to_string(),
                    },
                };
                monitor
                    .refresh_with(kind, || refreshed_at, || Err(error))
                    .unwrap_err();

                let cached = monitor
                    .cached_measurement()
                    .map(|cached| (cached.measured_at, cached.measurement));
                let published = match cached {
                    None => Published::Nothing,
                    Some((at, m)) if at == t0 && Some(m) == previous => Published::Previous,
                    Some((at, m))
                        if at == t0
                            && m == CacheDiskMeasurement {
                                status: fresh_status,
                                btrfs: with_btrfs.btrfs,
                            } =>
                    {
                        Published::FreshStatvfsWithPreviousBtrfs
                    }
                    Some(other) => panic!("{case}: unexpected publication {other:?}"),
                };
                assert_eq!(published, expected(kind, failure, previous_state), "{case}");
            }
        }
    }
}

#[test]
fn consecutive_failed_periodic_refreshes_close_admission_at_the_ttl() {
    // The refresh loop sleeps one cadence after each refresh finishes, and a
    // btrfs probe that times out takes its 2 s timeout, so consecutive timed-out
    // refreshes start this far apart. Each one stamps its probe's start.
    const TIMED_OUT_PROBE: Duration = Duration::from_secs(2);
    let probe_period = CACHE_REFRESH_CADENCE + TIMED_OUT_PROBE;
    let t0 = Instant::now();
    type Failure = fn() -> CacheDiskProbeError;
    let failures: [(&str, Failure); 2] = [
        ("statvfs", || CacheDiskProbeError::Statvfs),
        ("btrfs", || CacheDiskProbeError::Btrfs {
            status: measurement(400, 1_000).status,
            detail: "timed out".to_string(),
        }),
    ];
    for (case, error) in failures {
        let monitor = CacheDiskAdmissionMonitor::new();
        monitor
            .refresh_with(
                CacheDiskRefreshKind::Periodic,
                || t0,
                || Ok(measurement(500, 1_000)),
            )
            .unwrap();
        for failure in 1..=2 {
            let started_at = t0 + probe_period * failure;
            monitor
                .refresh_with(
                    CacheDiskRefreshKind::Periodic,
                    || started_at,
                    || Err(error()),
                )
                .unwrap_err();
            let decision = monitor.evaluate_cached_at(started_at + TIMED_OUT_PROBE, config(), 0);
            assert!(
                decision.allows_new_allocation(),
                "{case}: failure {failure} closed admission: {}",
                decision.detail
            );
        }

        let decision = monitor.evaluate_cached_at(t0 + probe_period * 3, config(), 0);
        assert_eq!(
            decision.state,
            CacheDiskAdmissionState::Unavailable,
            "{case}: admission still open when the third refresh starts"
        );
        assert_eq!(decision.reason, CacheDiskAdmissionReason::DiskUnknownStale);
    }
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

    let monitor = CacheDiskAdmissionMonitor::new();
    let started = Arc::new(Barrier::new(2));
    let (release, wait) = mpsc::channel();
    let (entered, observed) = mpsc::channel();
    std::thread::scope(|scope| {
        let first_started = Arc::clone(&started);
        let monitor = &monitor;
        scope.spawn(move || {
            monitor
                .refresh_with(CacheDiskRefreshKind::Periodic, Instant::now, || {
                    first_started.wait();
                    wait.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(measurement(500, 1_000))
                })
                .unwrap();
        });
        started.wait();
        scope.spawn(|| {
            monitor
                .refresh_with(CacheDiskRefreshKind::Periodic, Instant::now, || {
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

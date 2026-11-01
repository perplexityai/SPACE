use super::*;

fn mkdir(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
}

fn write(path: &Path) {
    std::fs::write(path, b"rootfs").unwrap();
}

#[test]
fn classify_disk_metric_targets_uses_disk_layout() {
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_cache = tmp.path().join("sandboxes");
    let templates = tmp.path().join("templates");
    let suspended_id = "018f6d8d-0000-7000-8000-000000000001";
    let active_id = "018f6d8d-0000-7000-8000-000000000002";
    let incomplete_snapshot_id = "018f6d8d-0000-7000-8000-000000000003";
    let btrfs_snapshot_only_id = "018f6d8d-0000-7000-8000-000000000004";
    let non_sandbox_dir = "not-a-sandbox-id";

    mkdir(&sandbox_cache.join(suspended_id).join("snapshot"));
    write(
        &sandbox_cache
            .join(suspended_id)
            .join("snapshot")
            .join("vmstate"),
    );
    write(
        &sandbox_cache
            .join(suspended_id)
            .join("snapshot")
            .join("memory"),
    );
    mkdir(&sandbox_cache.join(suspended_id).join("instance"));
    mkdir(
        &sandbox_cache
            .join(suspended_id)
            .join("snapshots")
            .join("f-manual-20260615T010203Z"),
    );
    mkdir(&sandbox_cache.join(active_id).join("instance"));
    mkdir(
        &sandbox_cache
            .join(active_id)
            .join("snapshots")
            .join("d-1m-20260615T010203Z"),
    );
    mkdir(
        &sandbox_cache
            .join(active_id)
            .join("snapshots")
            .join("scratch"),
    );
    write(
        &sandbox_cache
            .join(active_id)
            .join("snapshots")
            .join("d-5m-20260615T010203Z"),
    );
    mkdir(&sandbox_cache.join(incomplete_snapshot_id).join("snapshot"));
    write(
        &sandbox_cache
            .join(incomplete_snapshot_id)
            .join("snapshot")
            .join("vmstate"),
    );
    mkdir(&sandbox_cache.join(incomplete_snapshot_id).join("instance"));
    mkdir(
        &sandbox_cache
            .join(btrfs_snapshot_only_id)
            .join("snapshots")
            .join("5m-20260615T010203Z"),
    );
    mkdir(&sandbox_cache.join(non_sandbox_dir).join("instance"));
    mkdir(
        &sandbox_cache
            .join(non_sandbox_dir)
            .join("snapshots")
            .join("d-1m-20260615T010203Z"),
    );
    mkdir(
        &templates
            .join("code-runtime")
            .join(format!("blake3:{}", "a".repeat(64))),
    );
    write(
        &templates
            .join("code-runtime")
            .join(format!("blake3:{}", "a".repeat(64)))
            .join("rootfs.ext4"),
    );
    mkdir(
        &templates
            .join(".hidden")
            .join(format!("sha256:{}", "b".repeat(64))),
    );
    write(
        &templates
            .join(".hidden")
            .join(format!("sha256:{}", "b".repeat(64)))
            .join("rootfs.ext4"),
    );

    // A template cached only for another environment counts too.
    let twin_build = templates.join("twin-runtime").join(format!(
        "sha256:{}@sha256:{}",
        "c".repeat(64),
        "d".repeat(64)
    ));
    mkdir(&twin_build);
    write(&twin_build.join("rootfs.ext4"));

    let targets = classify_disk_metric_targets(&sandbox_cache, &templates).unwrap();

    let mut template_targets = targets.templates.clone();
    template_targets.sort();
    assert_eq!(
        template_targets,
        vec![
            path_string(templates.join("code-runtime")),
            path_string(templates.join("twin-runtime")),
        ]
    );
    assert_eq!(
        targets.snapshotted_sandboxes,
        vec![path_string(
            sandbox_cache.join(suspended_id).join("snapshot")
        )]
    );
    assert_eq!(
        targets.active_sandboxes,
        vec![
            path_string(sandbox_cache.join(suspended_id).join("instance")),
            path_string(sandbox_cache.join(active_id).join("instance")),
            path_string(sandbox_cache.join(incomplete_snapshot_id).join("instance")),
        ]
    );
    assert_eq!(
        targets.btrfs_snapshots,
        vec![
            path_string(
                sandbox_cache
                    .join(suspended_id)
                    .join("snapshots")
                    .join("f-manual-20260615T010203Z"),
            ),
            path_string(
                sandbox_cache
                    .join(active_id)
                    .join("snapshots")
                    .join("d-1m-20260615T010203Z"),
            ),
            path_string(
                sandbox_cache
                    .join(active_id)
                    .join("snapshots")
                    .join("scratch"),
            ),
            path_string(
                sandbox_cache
                    .join(btrfs_snapshot_only_id)
                    .join("snapshots")
                    .join("5m-20260615T010203Z"),
            ),
        ]
    );
}

#[test]
fn classify_disk_metric_targets_enumerates_nodes_past_4096_targets() {
    // A busy node keeps two targets per suspended sandbox. Its inventory must
    // stay complete, or every byte gauge on the node stops reporting.
    let tmp = tempfile::tempdir().unwrap();
    let sandbox_cache = tmp.path().join("sandboxes");
    let templates = tmp.path().join("templates");
    mkdir(
        &templates
            .join("code-runtime")
            .join(format!("sha256:{}", "a".repeat(64))),
    );
    write(
        &templates
            .join("code-runtime")
            .join(format!("sha256:{}", "a".repeat(64)))
            .join("rootfs.ext4"),
    );
    let suspended = 2_100;
    for i in 0..suspended {
        let sandbox_root = sandbox_cache.join(format!("018f6d8d-0000-7000-8000-{i:012}"));
        mkdir(&sandbox_root.join("instance"));
        mkdir(&sandbox_root.join("snapshot"));
        write(&sandbox_root.join("snapshot").join("vmstate"));
        write(&sandbox_root.join("snapshot").join("memory"));
    }

    let targets = classify_disk_metric_targets(&sandbox_cache, &templates).unwrap();

    assert_eq!(targets.templates.len(), 1);
    assert_eq!(targets.snapshotted_sandboxes.len(), suspended);
    assert_eq!(targets.active_sandboxes.len(), suspended);
    assert!(targets.all_paths().len() > 4096);
}

#[test]
fn build_snapshot_sums_successful_measurements_only() {
    let targets = DiskMetricTargets {
        templates: vec!["/space/cache/t1".to_string(), "/space/cache/t2".to_string()],
        snapshotted_sandboxes: vec!["/space/cache/s1/snapshot".to_string()],
        active_sandboxes: vec!["/space/cache/s2/instance".to_string()],
        btrfs_snapshots: vec![
            "/space/cache/s1/snapshots/d-1m-20260615T010203Z".to_string(),
            "/space/cache/s2/snapshots/f-manual-20260615T010203Z".to_string(),
        ],
    };
    let mut measurement = DiskMeasurementReport::default();
    measurement.record_usage("/space/cache/t1".to_string(), 10);
    measurement.record_usage("/space/cache/s1/snapshot".to_string(), 20);
    measurement.record_usage("/space/cache/s2/instance".to_string(), 30);
    measurement.record_usage(
        "/space/cache/s1/snapshots/d-1m-20260615T010203Z".to_string(),
        40,
    );

    let snapshot = build_snapshot(&targets, &measurement);

    assert_eq!(snapshot.templates_bytes, 10);
    assert_eq!(snapshot.snapshotted_sandbox_bytes, 20);
    assert_eq!(snapshot.active_sandbox_bytes, 30);
    assert_eq!(snapshot.btrfs_snapshot_bytes, 40);
    assert_eq!(snapshot.template_count, 2);
    assert_eq!(snapshot.snapshotted_sandbox_count, 1);
    assert_eq!(snapshot.active_sandbox_count, 1);
    assert_eq!(snapshot.btrfs_snapshot_count, 2);
    assert_eq!(
        snapshot.measurement_counts,
        MeasurementCounts {
            success_count: 4,
            missing_count: 0,
            error_count: 0,
            ..Default::default()
        }
    );
    assert!(snapshot.should_emit_byte_gauge(DiskUsageCategory::Template));
    assert!(snapshot.should_emit_byte_gauge(DiskUsageCategory::BtrfsSnapshot));
}

#[test]
fn build_snapshot_tracks_failed_and_missing_measurements() {
    let targets = DiskMetricTargets {
        templates: vec!["/space/cache/t1".to_string(), "/space/cache/t2".to_string()],
        snapshotted_sandboxes: vec!["/space/cache/s1/snapshot".to_string()],
        active_sandboxes: vec!["/space/cache/s2/instance".to_string()],
        btrfs_snapshots: vec!["/space/cache/s1/snapshots/snap-1".to_string()],
    };
    let mut measurement = DiskMeasurementReport::default();
    measurement.record_usage("/space/cache/t1".to_string(), 10);
    measurement.record_error("/space/cache/t2".to_string());
    measurement.record_missing("/space/cache/s1/snapshot".to_string());
    measurement.record_usage("/space/cache/s2/instance".to_string(), 30);
    measurement.record_usage("/space/cache/s1/snapshots/snap-1".to_string(), 40);

    let snapshot = build_snapshot(&targets, &measurement);

    assert_eq!(snapshot.templates_bytes, 10);
    assert_eq!(snapshot.snapshotted_sandbox_bytes, 0);
    assert_eq!(snapshot.active_sandbox_bytes, 30);
    assert_eq!(snapshot.btrfs_snapshot_bytes, 40);
    assert_eq!(
        snapshot.measurement_counts,
        MeasurementCounts {
            success_count: 3,
            missing_count: 1,
            error_count: 1,
            ..Default::default()
        }
    );
    assert_eq!(
        snapshot
            .measurement_counts_by_category
            .get(&DiskUsageCategory::Template),
        Some(&MeasurementCounts {
            success_count: 1,
            missing_count: 0,
            error_count: 1,
            ..Default::default()
        })
    );
    assert_eq!(
        snapshot
            .measurement_counts_by_category
            .get(&DiskUsageCategory::SuspendCheckpoint),
        Some(&MeasurementCounts {
            success_count: 0,
            missing_count: 1,
            error_count: 0,
            ..Default::default()
        })
    );
    assert!(!snapshot.should_emit_byte_gauge(DiskUsageCategory::Template));
    assert!(snapshot.should_emit_byte_gauge(DiskUsageCategory::SuspendCheckpoint));
    assert!(snapshot.should_emit_byte_gauge(DiskUsageCategory::ActiveInstance));
    assert!(snapshot.should_emit_byte_gauge(DiskUsageCategory::BtrfsSnapshot));
}

#[test]
fn build_snapshot_counts_deadline_skips_apart_from_probe_errors() {
    let targets = DiskMetricTargets {
        templates: vec!["/space/cache/t1".to_string(), "/space/cache/t2".to_string()],
        snapshotted_sandboxes: vec!["/space/cache/s1/snapshot".to_string()],
        active_sandboxes: vec!["/space/cache/s2/instance".to_string()],
        btrfs_snapshots: vec!["/space/cache/s1/snapshots/snap-1".to_string()],
    };
    let mut measurement = DiskMeasurementReport::default();
    measurement.record_usage("/space/cache/t1".to_string(), 10);
    measurement.record_error("/space/cache/t2".to_string());
    measurement.record(
        "/space/cache/s1/snapshot".to_string(),
        DiskMeasurementOutcome::Unmeasured,
    );
    measurement.record_usage("/space/cache/s2/instance".to_string(), 30);
    measurement.record_usage("/space/cache/s1/snapshots/snap-1".to_string(), 40);

    let snapshot = build_snapshot(&targets, &measurement);

    assert_eq!(
        snapshot.measurement_counts,
        MeasurementCounts {
            success_count: 3,
            missing_count: 0,
            error_count: 1,
            unmeasured_count: 1,
        }
    );
    assert_eq!(
        snapshot
            .measurement_counts_by_category
            .get(&DiskUsageCategory::Template),
        Some(&MeasurementCounts {
            success_count: 1,
            missing_count: 0,
            error_count: 1,
            ..Default::default()
        })
    );
    assert_eq!(
        snapshot
            .measurement_counts_by_category
            .get(&DiskUsageCategory::SuspendCheckpoint),
        Some(&MeasurementCounts {
            success_count: 0,
            missing_count: 0,
            error_count: 0,
            unmeasured_count: 1,
        })
    );
    assert!(!snapshot.should_emit_byte_gauge(DiskUsageCategory::Template));
    assert!(!snapshot.should_emit_byte_gauge(DiskUsageCategory::SuspendCheckpoint));
    assert!(snapshot.should_emit_byte_gauge(DiskUsageCategory::ActiveInstance));
    assert!(snapshot.should_emit_byte_gauge(DiskUsageCategory::BtrfsSnapshot));
}

#[tokio::test(start_paused = true)]
async fn sandbox_disk_usage_cache_uses_completed_census() {
    let measured_id = SandboxId::from_str("018f6d8d-0000-7000-8000-000000000001").unwrap();
    let failed_id = SandboxId::from_str("018f6d8d-0000-7000-8000-000000000002").unwrap();
    let missing_id = SandboxId::from_str("018f6d8d-0000-7000-8000-000000000003").unwrap();
    let absent_id = SandboxId::from_str("018f6d8d-0000-7000-8000-000000000004").unwrap();
    let measured_path = format!("/space/cache/{measured_id}/instance");
    let failed_path = format!("/space/cache/{failed_id}/instance");
    let missing_path = format!("/space/cache/{missing_id}/instance");
    let targets = DiskMetricTargets {
        active_sandboxes: vec![
            measured_path.clone(),
            failed_path.clone(),
            missing_path.clone(),
        ],
        ..DiskMetricTargets::default()
    };
    let mut measurement = DiskMeasurementReport::default();
    measurement.record_usage(measured_path.clone(), 42);
    measurement.record_error(failed_path);
    measurement.record_missing(missing_path.clone());
    let cache = SandboxDiskUsageCache::default();
    assert_eq!(cache.lookup(measured_id), SandboxDiskUsageLookup::Unknown);

    cache.replace(&targets, &measurement, tokio::time::Instant::now());

    assert_eq!(cache.lookup(measured_id), SandboxDiskUsageLookup::Usage(42));
    assert_eq!(cache.lookup(failed_id), SandboxDiskUsageLookup::Unknown);
    assert_eq!(cache.lookup(missing_id), SandboxDiskUsageLookup::Missing);
    assert_eq!(cache.lookup(absent_id), SandboxDiskUsageLookup::Unknown);
    tokio::time::advance(Duration::from_secs(300)).await;
    measurement.record(measured_path, DiskMeasurementOutcome::Unmeasured);
    measurement.record(missing_path, DiskMeasurementOutcome::Unmeasured);
    cache.replace(&targets, &measurement, tokio::time::Instant::now());
    assert_eq!(cache.lookup(measured_id), SandboxDiskUsageLookup::Usage(42));
    tokio::time::advance(DISK_EVIDENCE_MAX_AGE - Duration::from_secs(300) + Duration::from_secs(1))
        .await;
    assert_eq!(cache.lookup(measured_id), SandboxDiskUsageLookup::Unknown);
    assert_eq!(cache.lookup(missing_id), SandboxDiskUsageLookup::Unknown);
}

#[tokio::test(start_paused = true)]
async fn measurement_budget_continues_past_slow_prefix_across_ticks() {
    let mut paths = (0..40).map(|i| format!("path-{i:02}")).collect::<Vec<_>>();
    let mut cursor = None;
    let mut attempted = std::collections::BTreeSet::new();
    for pass in 0..4 {
        let report = measure_paths_with(paths.clone(), &mut cursor, |path, budget| async move {
            tokio::time::sleep(budget.min(Duration::from_secs(30))).await;
            if path.ends_with('0') {
                Err("slow failed probe".into())
            } else {
                Ok(Some(super::super::disk::BtrfsDiskUsage {
                    exclusive_bytes: 1,
                    total_bytes: 1,
                }))
            }
        })
        .await;
        assert!(
            report
                .outcomes
                .values()
                .any(|outcome| *outcome == DiskMeasurementOutcome::Unmeasured)
        );
        attempted.extend(report.outcomes.into_iter().filter_map(|(path, outcome)| {
            (outcome != DiskMeasurementOutcome::Unmeasured).then_some(path)
        }));
        if pass == 0 {
            paths.retain(|path| Some(path) != cursor.as_ref());
            paths.push("path--new".into());
        }
    }
    assert!((0..40).all(|i| attempted.contains(&format!("path-{i:02}"))));
    assert!(attempted.contains("path--new"));
}

#[test]
fn build_disk_usage_diagnostic_selects_bounded_top_consumers() {
    let sandbox_a = "018f6d8d-0000-7000-8000-000000000001";
    let sandbox_b = "018f6d8d-0000-7000-8000-000000000002";
    let sandbox_c = "018f6d8d-0000-7000-8000-000000000003";
    let sandbox_d = "018f6d8d-0000-7000-8000-000000000004";
    let targets = DiskMetricTargets {
        templates: vec![
            "/space/cache/firecracker/templates/template-a".to_string(),
            "/space/cache/firecracker/templates/template-b".to_string(),
            "/space/cache/firecracker/templates/template-missing".to_string(),
        ],
        snapshotted_sandboxes: vec![
            format!("/space/cache/{sandbox_c}/snapshot"),
            format!("/space/cache/{sandbox_a}/snapshot"),
        ],
        active_sandboxes: vec![
            format!("/space/cache/{sandbox_a}/instance"),
            format!("/space/cache/{sandbox_d}/instance"),
        ],
        btrfs_snapshots: vec![
            format!("/space/cache/{sandbox_b}/snapshots/snap-1"),
            format!("/space/cache/{sandbox_a}/snapshots/snap-2"),
        ],
    };
    let usage_bytes = [
        (
            "/space/cache/firecracker/templates/template-a".to_string(),
            70,
        ),
        (
            "/space/cache/firecracker/templates/template-b".to_string(),
            100,
        ),
        (format!("/space/cache/{sandbox_a}/snapshot"), 80),
        (format!("/space/cache/{sandbox_c}/snapshot"), 80),
        (format!("/space/cache/{sandbox_a}/instance"), 95),
        (format!("/space/cache/{sandbox_d}/instance"), 60),
        (format!("/space/cache/{sandbox_b}/snapshots/snap-1"), 90),
        (format!("/space/cache/{sandbox_a}/snapshots/snap-2"), 70),
    ];
    let mut measurement = DiskMeasurementReport::default();
    for (path, exclusive_bytes) in usage_bytes {
        measurement.record_usage(path, exclusive_bytes);
    }

    let diagnostic = build_disk_usage_diagnostic(&targets, &measurement, 7, 1);

    assert_eq!(
        diagnostic.top_consumers,
        vec![
            DiskUsageDiagnosticEntry {
                category: DiskUsageCategory::Template,
                identifier: "template-b".to_string(),
                exclusive_bytes: 100,
            },
            DiskUsageDiagnosticEntry {
                category: DiskUsageCategory::ActiveInstance,
                identifier: sandbox_a.to_string(),
                exclusive_bytes: 95,
            },
            DiskUsageDiagnosticEntry {
                category: DiskUsageCategory::BtrfsSnapshot,
                identifier: format!("{sandbox_b}/snap-1"),
                exclusive_bytes: 90,
            },
            DiskUsageDiagnosticEntry {
                category: DiskUsageCategory::SuspendCheckpoint,
                identifier: sandbox_a.to_string(),
                exclusive_bytes: 80,
            },
            DiskUsageDiagnosticEntry {
                category: DiskUsageCategory::SuspendCheckpoint,
                identifier: sandbox_c.to_string(),
                exclusive_bytes: 80,
            },
            DiskUsageDiagnosticEntry {
                category: DiskUsageCategory::Template,
                identifier: "template-a".to_string(),
                exclusive_bytes: 70,
            },
            DiskUsageDiagnosticEntry {
                category: DiskUsageCategory::BtrfsSnapshot,
                identifier: format!("{sandbox_a}/snap-2"),
                exclusive_bytes: 70,
            },
        ]
    );
    assert_eq!(
        diagnostic
            .top_consumers_by_category
            .get(&DiskUsageCategory::Template),
        Some(&vec![DiskUsageDiagnosticEntry {
            category: DiskUsageCategory::Template,
            identifier: "template-b".to_string(),
            exclusive_bytes: 100,
        }])
    );
    assert_eq!(
        diagnostic
            .top_consumers_by_category
            .get(&DiskUsageCategory::SuspendCheckpoint),
        Some(&vec![DiskUsageDiagnosticEntry {
            category: DiskUsageCategory::SuspendCheckpoint,
            identifier: sandbox_a.to_string(),
            exclusive_bytes: 80,
        }])
    );
    assert_eq!(
        diagnostic
            .top_consumers_by_category
            .get(&DiskUsageCategory::BtrfsSnapshot),
        Some(&vec![DiskUsageDiagnosticEntry {
            category: DiskUsageCategory::BtrfsSnapshot,
            identifier: format!("{sandbox_b}/snap-1"),
            exclusive_bytes: 90,
        }])
    );
    assert_eq!(
        diagnostic
            .top_consumers_by_category
            .get(&DiskUsageCategory::ActiveInstance),
        Some(&vec![DiskUsageDiagnosticEntry {
            category: DiskUsageCategory::ActiveInstance,
            identifier: sandbox_a.to_string(),
            exclusive_bytes: 95,
        }])
    );
}

use super::*;

// The deterministic UUID derivation is wire-format for every
// template-as-parent send stream — pin the bytes so a refactor that
// accidentally bumps the namespace, changes the name format, or
// swaps to v3/v4 will fail the build instead of silently invalidating
// every prior stream.
#[test]
fn deterministic_uuid_for_seed_is_pinned() {
    let got = deterministic_template_received_uuid(
        "seed",
        "sha256:6dfaca783bd73dbb42fe5833b46e575a7c9555e56801e79cda7f01f573f2ebb1",
    );
    assert_eq!(
        got.to_string(),
        "7c824a00-83ad-518d-a954-053fab95bc5c",
        "if this assert fires, every existing template subvolume's \
             received_uuid is invalidated — DO NOT regenerate the pin without \
             a coordinated migration plan"
    );
}

#[test]
fn deterministic_uuid_differs_per_content_hash() {
    let a = deterministic_template_received_uuid("seed", "sha256:aaaa");
    let b = deterministic_template_received_uuid("seed", "sha256:bbbb");
    assert_ne!(a, b, "different content_hash must produce different UUIDs");
}

#[test]
fn deterministic_uuid_differs_per_template_name() {
    let a = deterministic_template_received_uuid("seed", "sha256:abcd");
    let b = deterministic_template_received_uuid("app-runtime", "sha256:abcd");
    assert_ne!(a, b, "different template name must produce different UUIDs");
}

#[test]
fn deterministic_uuid_is_stable_across_calls() {
    let a = deterministic_template_received_uuid("seed", "sha256:abcd");
    let b = deterministic_template_received_uuid("seed", "sha256:abcd");
    assert_eq!(a, b, "same inputs must produce the same UUID");
}

#[test]
fn deterministic_uuid_is_v5() {
    let id = deterministic_template_received_uuid("seed", "sha256:abcd");
    assert_eq!(
        id.get_version_num(),
        5,
        "deterministic UUID must be v5 (SHA-1 of namespace || name)"
    );
}

#[test]
fn parses_filesystem_usage_raw_metadata_and_unallocated() {
    let raw = r#"
Overall:
    Device size:                  107374182400
    Device allocated:             21474836480
    Device unallocated:           85899345920
    Used:                         10737418240
    Free (estimated):             96636764160      (min: 53687091200)
    Free (statfs, df):            96636764160
    Data ratio:                   1.00
    Metadata ratio:               1.00
    Global reserve:               536870912      (used: 0)

Data,single: Size:10737418240, Used:5368709120 (50.00%)
   /dev/nvme0n1    10737418240

Metadata,single: Size:1073741824, Used:268435456 (25.00%)
   /dev/nvme0n1    1073741824

System,single: Size:4194304, Used:16384 (0.39%)
   /dev/nvme0n1    4194304
"#;

    let usage = parse_filesystem_usage(Path::new("/space/cache"), raw).unwrap();

    assert_eq!(usage.device_unallocated_bytes, 85_899_345_920);
    assert_eq!(usage.metadata_total_bytes, 1_073_741_824);
    assert_eq!(usage.metadata_used_bytes, 268_435_456);
    assert_eq!(usage.metadata_available_bytes(), 805_306_368);
    assert_eq!(usage.data_total_bytes, 10_737_418_240);
    assert_eq!(usage.data_used_bytes, 5_368_709_120);
    assert_eq!(usage.data_slack_bytes(), 5_368_709_120);
}

// Synthetic four-device RAID0 output. Per-device breakdowns under each
// block-group section must not be double-counted into the totals.
#[test]
fn parses_filesystem_usage_raw_multi_device_raid0() {
    let raw = r#"
Overall:
    Device size:                     4294967296
    Device allocated:                2147483648
    Device unallocated:              2147483648
    Device missing:                           0
    Device slack:                             0
    Used:                            1342177280
    Free (estimated):                2952790016 (min: 2952790016)
    Free (statfs, df):                2952790016
    Data ratio:                            1.00
    Metadata ratio:                        1.00
    Global reserve:                    33554432 (used: 0)
    Multiple profiles:                       no

Data,RAID0: Size:1610612736, Used:1073741824 (66.67%)
   /dev/nvme0n1 402653184
   /dev/nvme1n1 402653184
   /dev/nvme2n1 402653184
   /dev/nvme3n1 402653184

Metadata,RAID0: Size:268435456, Used:134217728 (50.00%)
   /dev/nvme0n1 67108864
   /dev/nvme1n1 67108864
   /dev/nvme2n1 67108864
   /dev/nvme3n1 67108864

System,RAID0: Size:268435456, Used:134217728 (50.00%)
   /dev/nvme0n1 67108864
   /dev/nvme1n1 67108864
   /dev/nvme2n1 67108864
   /dev/nvme3n1 67108864

Unallocated:
   /dev/nvme0n1 536870912
   /dev/nvme1n1 536870912
   /dev/nvme2n1 536870912
   /dev/nvme3n1 536870912
"#;

    let usage = parse_filesystem_usage(Path::new("/space/cache"), raw).unwrap();

    assert_eq!(usage.device_unallocated_bytes, 2_147_483_648);
    assert_eq!(usage.metadata_total_bytes, 268_435_456);
    assert_eq!(usage.metadata_used_bytes, 134_217_728);
    assert_eq!(usage.data_total_bytes, 1_610_612_736);
    assert_eq!(usage.data_used_bytes, 1_073_741_824);
    assert_eq!(usage.data_slack_bytes(), 536_870_912);
}

#[test]
fn command_output_timeout_returns_without_waiting_for_child_exit() {
    let mut command = Command::new("sleep");
    command.arg("5");
    let started_at = Instant::now();

    let output = command_output_with_timeout_guarded(&mut command, Duration::from_millis(20), None)
        .expect("timeout helper should not surface an io error");

    assert!(output.is_none());
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "timeout helper must not wait for the full child runtime"
    );
}

#[test]
fn command_output_timeout_detaches_reaper_when_grace_expires() {
    let (reaped_tx, reaped_rx) = std::sync::mpsc::channel();
    let mut command = Command::new("sleep");
    command.arg("5");
    let started_at = Instant::now();

    let output = command_output_with_timeout_and_reap_grace(
        &mut command,
        Duration::from_millis(20),
        Duration::ZERO,
        Some(reaped_tx),
        None,
    )
    .expect("timeout helper should not surface an io error");

    assert!(output.is_none());
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "timeout helper must return immediately after handing off the child"
    );
    reaped_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("detached reaper should wait for the timed-out child");
}

#[test]
fn probe_in_flight_guard_rejects_second_acquire_until_released() {
    static ACTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    ACTIVE.store(false, std::sync::atomic::Ordering::Release);

    let guard = ProbeInFlightGuard::try_acquire(&ACTIVE).unwrap();

    assert!(ProbeInFlightGuard::try_acquire(&ACTIVE).is_none());
    drop(guard);
    assert!(ProbeInFlightGuard::try_acquire(&ACTIVE).is_some());
}

// The true-on-a-real-subvolume half of `is_subvolume` needs a btrfs
// mount and is covered by template-manager's
// `delete_template_build_removes_read_only_btrfs_subvolume_build`
// (gated on `SPACE_TEST_BTRFS_DIR`). The false cases are portable.
#[test]
fn is_subvolume_is_false_for_plain_dir_file_and_missing_path() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("plain-dir");
    std::fs::create_dir(&dir).unwrap();
    let file = temp.path().join("file");
    std::fs::write(&file, b"contents").unwrap();

    assert!(
        !is_subvolume(&dir),
        "plain directory must not report as a subvolume"
    );
    assert!(
        !is_subvolume(&file),
        "regular file must not report as a subvolume"
    );
    assert!(
        !is_subvolume(&temp.path().join("missing")),
        "missing path must not report as a subvolume"
    );
}

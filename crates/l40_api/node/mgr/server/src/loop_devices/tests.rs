use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn kernel_indices_ignore_non_loop_and_partition_names() {
    let indices: BTreeSet<u32> = kernel_indices_from_names([
        "loop0", "loop7", "loop8", "nvme0n1", "dm-0", "loop3p1", "loop", "looper2",
    ]);
    assert_eq!(indices, BTreeSet::from([0, 7, 8]));
}

#[test]
fn target_is_derived_from_slots_and_clamped_at_both_ends() {
    // A node configured for very few sandboxes still needs headroom for its own
    // loopback mounts, so the floor applies.
    assert_eq!(
        LoopDeviceTarget::from_max_slots(1).get(),
        LoopDeviceTarget::MIN
    );
    assert_eq!(LoopDeviceTarget::from_max_slots(64).get(), 256);
    // Saturating rather than wrapping, and never above the ceiling.
    assert_eq!(
        LoopDeviceTarget::from_max_slots(u32::MAX).get(),
        LoopDeviceTarget::MAX
    );
}

#[test]
fn a_malformed_override_is_an_error_rather_than_a_silent_fallback() {
    assert_eq!(resolve_target(Some("512"), 8).unwrap().get(), 512);
    // Empty and whitespace-only are treated as unset — a Helm value that
    // renders to "" should not fail startup.
    assert_eq!(
        resolve_target(Some(""), 64).unwrap(),
        LoopDeviceTarget::from_max_slots(64)
    );
    assert_eq!(
        resolve_target(Some("   "), 64).unwrap(),
        LoopDeviceTarget::from_max_slots(64)
    );
    assert_eq!(
        resolve_target(None, 64).unwrap(),
        LoopDeviceTarget::from_max_slots(64)
    );
    assert_eq!(
        resolve_target(Some("lots"), 8),
        Err(LoopDeviceTargetError::NotANumber("lots".to_string()))
    );
    assert_eq!(
        resolve_target(Some("0"), 8),
        Err(LoopDeviceTargetError::OutOfRange(0))
    );
    assert_eq!(
        resolve_target(Some("100000"), 8),
        Err(LoopDeviceTargetError::OutOfRange(100_000))
    );
}

#[test]
fn missing_indices_are_the_gap_below_the_target() {
    let present = BTreeSet::from([0, 1, 2, 5]);
    assert_eq!(missing_indices(&present, 6), vec![3, 4]);
    // Devices above the target are irrelevant, not something to remove.
    assert_eq!(
        missing_indices(&BTreeSet::from([0, 1, 99]), 2),
        Vec::<u32>::new()
    );
    assert_eq!(missing_indices(&BTreeSet::new(), 3), vec![0, 1, 2]);
}

/// A node whose `/dev` is a tmpfs keeps the device nodes it was started with,
/// so a device the kernel already has can be unopenable. That is the shape the
/// failure takes in `losetup`: ENOENT, not a capacity error.
#[test]
fn a_kernel_device_with_no_node_is_reported_as_nodeless() {
    let sys_block = tempfile::tempdir().unwrap();
    let dev_dir = tempfile::tempdir().unwrap();
    for idx in 0..9 {
        std::fs::create_dir(sys_block.path().join(format!("loop{idx}"))).unwrap();
    }
    // Nodes for 0..8 only: the kernel has loop8, /dev does not.
    for idx in 0..8 {
        std::fs::write(dev_dir.path().join(format!("loop{idx}")), b"").unwrap();
    }
    let present = read_kernel_indices(sys_block.path()).unwrap();
    assert_eq!(present.len(), 9);
    let nodeless: Vec<u32> = present
        .iter()
        .copied()
        .filter(|idx| !dev_dir.path().join(format!("loop{idx}")).exists())
        .collect();
    assert_eq!(nodeless, vec![8]);
}

#[test]
fn node_permissions_are_copied_from_an_existing_node() {
    let dev_dir = tempfile::tempdir().unwrap();
    let existing = dev_dir.path().join("loop0");
    std::fs::write(&existing, b"").unwrap();
    std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o640)).unwrap();

    let (mode, _gid) = node_permissions(dev_dir.path(), &BTreeSet::from([0]));
    assert_eq!(mode, 0o640);

    // Nothing to copy from: conservative default rather than 0666.
    let empty = tempfile::tempdir().unwrap();
    let (mode, gid) = node_permissions(empty.path(), &BTreeSet::from([0]));
    assert_eq!((mode, gid), (FALLBACK_NODE_MODE, 0));
}

#[test]
fn ensure_capacity_is_a_no_op_when_the_target_is_already_met() {
    let sys_block = tempfile::tempdir().unwrap();
    let dev_dir = tempfile::tempdir().unwrap();
    for idx in 0..LoopDeviceTarget::MIN {
        std::fs::create_dir(sys_block.path().join(format!("loop{idx}"))).unwrap();
        std::fs::write(dev_dir.path().join(format!("loop{idx}")), b"").unwrap();
    }
    let adder = FakeAdder::refusing_to_materialize();
    let report = ensure_capacity_in(
        sys_block.path(),
        dev_dir.path(),
        LoopDeviceTarget::from_max_slots(1),
        &adder,
    )
    .expect("a met target must not need /dev/loop-control");
    assert_eq!(adder.calls(), 0, "a met target must add nothing");
    assert!(report.changed_nothing());
    assert_eq!(report.kernel_devices_before, LoopDeviceTarget::MIN);
    assert_eq!(report.present(), LoopDeviceTarget::MIN);
}

/// The real thing, against the running kernel: raise the device count past
/// `max_loop`, then attach a file to one of the new devices to prove it is a
/// working loop device and not just an entry in sysfs.
///
/// Ignored by default because it needs root and mutates host state. It leaves
/// the devices it creates in place — `LOOP_CTL_REMOVE` blocks on block-device
/// teardown for ~100ms each, and a spare loop device costs one `gendisk` — so
/// it runs only on a disposable host that opts in:
///
///   SPACE_NODEMGR_TEST_LOOP_DEVICES=1 \
///     cargo test -p pplx-space-node-mgr --lib loop_devices -- --ignored
#[test]
#[ignore = "requires root and an explicit opt-in on a disposable host; adds loop devices to the running kernel"]
fn ensure_capacity_creates_attachable_devices_past_max_loop() {
    // CI includes ignored tests; host mutation still requires explicit opt-in.
    if std::env::var_os("SPACE_NODEMGR_TEST_LOOP_DEVICES").is_none() {
        eprintln!(
            "not opted in: set SPACE_NODEMGR_TEST_LOOP_DEVICES=1 on a disposable host that may keep the loop devices this test adds"
        );
        return;
    }
    let max_loop: u32 = std::fs::read_to_string("/sys/module/loop/parameters/max_loop")
        .expect("loop module parameters")
        .trim()
        .parse()
        .expect("max_loop is a number");
    let target = LoopDeviceTarget::from_max_slots(1);
    assert!(
        max_loop < target.get(),
        "test is meaningless unless the target is past max_loop ({max_loop})"
    );

    // A prior run of this test may have left the node behind; ensure_capacity
    // leaves an existing node alone, so drop it to exercise the creation path.
    let _ = std::fs::remove_file(format!("/dev/loop{}", target.get() - 1));

    let report = ensure_capacity(target).expect("ensure_capacity");
    let present = read_kernel_indices(Path::new("/sys/block")).unwrap();
    assert!(
        present.len() as u32 >= target.get(),
        "kernel should have at least {target} devices, has {}",
        present.len()
    );
    // Reported capacity is the kernel's actual count. Deriving it from
    // `before + added` undercounts whenever another node service on the host is
    // adding devices from the same pool at the same time, which is the normal
    // case: they all start together.
    assert_eq!(
        report.present(),
        present.len() as u32,
        "reported capacity should be re-read, not derived from this call's additions"
    );

    // The highest index we asked for must be a usable loop device.
    let index = target.get() - 1;
    let dev = format!("/dev/loop{index}");
    assert!(
        Path::new(&dev).exists(),
        "{dev} must exist for losetup to open it"
    );

    // A node we created must be indistinguishable from one the kernel created:
    // same mode, same group. mknod applies the process umask to its mode
    // argument, so a node made without correcting for it comes out 0640 under
    // the usual 022 and is not group-writable — losetup run by a non-root
    // member of the device's group then cannot attach to it.
    let ours = std::fs::metadata(&dev).expect("metadata for the node we created");
    let kernels = std::fs::metadata("/dev/loop0").expect("metadata for /dev/loop0");
    assert_eq!(
        (ours.mode() & 0o7777, ours.gid()),
        (kernels.mode() & 0o7777, kernels.gid()),
        "{dev} should match /dev/loop0's mode and group"
    );

    let backing = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(backing.path(), vec![0u8; 1 << 20]).unwrap();
    let backing_path = backing.path().to_string_lossy().into_owned();
    let attach = std::process::Command::new("losetup")
        .args([&dev, &backing_path])
        .output()
        .expect("losetup");
    assert!(
        attach.status.success(),
        "attaching {dev}: {}",
        String::from_utf8_lossy(&attach.stderr)
    );
    let listed = std::process::Command::new("losetup")
        .args(["-j", &backing_path])
        .output()
        .expect("losetup -j");
    let listed = String::from_utf8_lossy(&listed.stdout);
    let detach = std::process::Command::new("losetup")
        .args(["-d", &dev])
        .output()
        .expect("losetup -d");

    assert!(
        listed.contains(&dev),
        "{dev} should back the temp file, got: {listed}"
    );
    assert!(
        detach.status.success(),
        "detaching {dev}: {}",
        String::from_utf8_lossy(&detach.stderr)
    );
    assert!(
        report.kernel_devices_added > 0 || report.device_nodes_created > 0,
        "expected to add capacity past max_loop={max_loop}, report: {report:?}"
    );
}

/// A live slot-budget increase has to move the device target with it. The
/// target must come from the requested budget; deriving it from the current one
/// leaves the node advertising slots it cannot back.
#[test]
fn a_live_slot_increase_sizes_devices_from_the_requested_budget() {
    let current = 64;
    let requested = 1000;

    let target = target_for_slot_increase(None, current, Some(requested))
        .expect("a plain increase resolves")
        .expect("an increase needs a target");
    assert_eq!(target, LoopDeviceTarget::from_max_slots(requested));
    assert!(
        target > LoopDeviceTarget::from_max_slots(current),
        "sizing from the current budget ({current}) would not cover the requested one ({requested})"
    );
}

#[test]
fn updates_that_cannot_need_more_devices_ask_for_no_work() {
    // No slot change in the update at all.
    assert_eq!(target_for_slot_increase(None, 64, None), Ok(None));
    // Same budget restated.
    assert_eq!(target_for_slot_increase(None, 64, Some(64)), Ok(None));
    // A shrink, which the pool rejects anyway, and which never needs devices.
    assert_eq!(target_for_slot_increase(None, 64, Some(32)), Ok(None));
}

#[test]
fn a_pinned_override_survives_a_slot_increase() {
    // The operator named a device count; a bigger slot budget does not silently
    // move it. Ensuring against the pin is a no-op once it is already met.
    assert_eq!(
        target_for_slot_increase(Some("512"), 64, Some(4000)),
        Ok(Some("512".parse().unwrap()))
    );
    // A malformed pin is still reported rather than swapped for a derived value.
    assert_eq!(
        target_for_slot_increase(Some("plenty"), 64, Some(4000)),
        Err(LoopDeviceTargetError::NotANumber("plenty".to_string()))
    );
}

/// Stands in for `/dev/loop-control`.
///
/// `materialize` is the whole point: a real `LOOP_CTL_ADD` allocates the index
/// before it builds the disk and releases it if that fails, so a caller can see
/// `EEXIST` — or even a success — for a device that never appears in
/// `/sys/block`. This fake reproduces that by reporting success without
/// creating the sysfs entry.
struct FakeAdder {
    sys_block: Option<std::path::PathBuf>,
    calls: std::cell::Cell<usize>,
}

impl FakeAdder {
    fn materializing(sys_block: &Path) -> Self {
        Self {
            sys_block: Some(sys_block.to_path_buf()),
            calls: std::cell::Cell::new(0),
        }
    }

    fn refusing_to_materialize() -> Self {
        Self {
            sys_block: None,
            calls: std::cell::Cell::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.get()
    }
}

impl LoopDeviceProvisioner for FakeAdder {
    fn add_device(&self, index: u32) -> Result<bool, LoopCapacityError> {
        self.calls.set(self.calls.get() + 1);
        if let Some(sys_block) = &self.sys_block {
            std::fs::create_dir_all(sys_block.join(format!("loop{index}")))
                .map_err(|source| LoopCapacityError::AddDevice { index, source })?;
        }
        Ok(true)
    }

    /// A plain file stands in for the block node: the reconcile only asks
    /// whether the path exists, and mknod is unavailable to a test process.
    fn create_node(
        &self,
        dev_dir: &Path,
        index: u32,
        _mode: u32,
        _gid: u32,
    ) -> Result<bool, LoopCapacityError> {
        let path = dev_dir.join(format!("loop{index}"));
        if path.exists() {
            return Ok(false);
        }
        std::fs::write(&path, b"")
            .map_err(|source| LoopCapacityError::CreateNode { path, source })?;
        Ok(true)
    }
}

/// The invariant an ensure promises: every index below the target exists and is
/// openable. A caller publishes a slot budget on the strength of that promise.
#[test]
fn ensure_capacity_fails_when_an_added_device_never_materializes() {
    let sys_block = tempfile::tempdir().unwrap();
    let dev_dir = tempfile::tempdir().unwrap();
    let target = LoopDeviceTarget::from_max_slots(1);

    let adder = FakeAdder::refusing_to_materialize();
    let error = ensure_capacity_in(sys_block.path(), dev_dir.path(), target, &adder)
        .expect_err("an unmet target must not report success");

    match error {
        LoopCapacityError::CapacityNotReached {
            target: reported,
            present,
            passes,
            missing_devices,
            ..
        } => {
            assert_eq!(reported, target.get());
            assert_eq!(present, 0);
            assert_eq!(missing_devices as u32, target.get());
            assert_eq!(passes, RECONCILE_PASSES, "every pass should have been used");
        }
        other => panic!("expected CapacityNotReached, got {other:?}"),
    }
    assert_eq!(error.phase(), LoopCapacityPhase::NotReached);
    // Retried rather than giving up on the first pass: the reserving caller's
    // cleanup can release the index, so a later attempt can succeed.
    assert_eq!(
        adder.calls(),
        target.get() as usize * RECONCILE_PASSES as usize
    );
}

#[test]
fn ensure_capacity_reports_success_once_every_index_exists_and_is_openable() {
    let sys_block = tempfile::tempdir().unwrap();
    let dev_dir = tempfile::tempdir().unwrap();
    let target = LoopDeviceTarget::from_max_slots(1);

    let adder = FakeAdder::materializing(sys_block.path());
    let report = ensure_capacity_in(sys_block.path(), dev_dir.path(), target, &adder)
        .expect("devices that materialize satisfy the target");

    assert_eq!(report.present(), target.get());
    assert_eq!(report.kernel_devices_added, target.get());
    // One pass is enough when adds take effect.
    assert_eq!(adder.calls(), target.get() as usize);
    for index in 0..target.get() {
        assert!(
            dev_dir.path().join(format!("loop{index}")).exists(),
            "loop{index} has no device node, so losetup would fail with ENOENT"
        );
    }
}

/// A device the kernel already has but whose node is missing needs no add — the
/// pass must fix the node alone. This is every restart in a container whose
/// `/dev` is a tmpfs.
#[test]
fn a_nodeless_device_is_fixed_without_adding_anything() {
    let sys_block = tempfile::tempdir().unwrap();
    let dev_dir = tempfile::tempdir().unwrap();
    let target = LoopDeviceTarget::from_max_slots(1);
    for index in 0..target.get() {
        std::fs::create_dir(sys_block.path().join(format!("loop{index}"))).unwrap();
    }

    let adder = FakeAdder::refusing_to_materialize();
    let report = ensure_capacity_in(sys_block.path(), dev_dir.path(), target, &adder)
        .expect("nodes alone can be created");

    assert_eq!(adder.calls(), 0, "no kernel device was missing");
    assert_eq!(report.kernel_devices_added, 0);
    assert_eq!(report.device_nodes_created, target.get());
}

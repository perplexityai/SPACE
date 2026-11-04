//! Loop-device capacity for a node.
//!
//! Every sandbox whose rootfs is a device-mapper stack holds loop devices for
//! the lifetime of the VM: one for the dm-snapshot origin, one for the COW
//! backing it, and one more for each encrypted data volume or staged snapshot.
//! Loop devices are a host-wide budget, so exhausting it does not fail one
//! sandbox — it fails every subsequent create on the node, and it fails them in
//! `losetup`, far from anything that looks like a capacity problem.
//!
//! Two separate things bound that budget, and the fix for one does not fix the
//! other:
//!
//! 1. **How many devices the kernel has.** The loop driver pre-creates
//!    `max_loop` of them at module load, default 8. `max_loop` is read-only at
//!    runtime (`/sys/module/loop/parameters/max_loop` is mode 0444), so it can
//!    only be raised by a modprobe drop-in that is in place *before* the module
//!    loads. A node whose loop module autoloaded early — the first loopback
//!    mount pulls it in — keeps the default no matter what the drop-in says.
//!    That is why this is not something a node image alone can guarantee.
//!
//!    `max_loop` is not, however, a hard ceiling on the device count. It gates
//!    only the implicit path where opening `/dev/loopN` by device number
//!    autocreates it (`loop_probe`). `LOOP_CTL_ADD` on `/dev/loop-control` is
//!    not gated, so a node stuck at `max_loop=8` can still be given as many
//!    devices as it needs, at runtime, with no reboot and no module reload.
//!
//! 2. **Whether a device node exists to open.** `losetup` resolves a free index
//!    and then opens `/dev/loopN`; if the node is missing it fails with ENOENT,
//!    which reads as "no such file" rather than "out of capacity". On a host
//!    whose `/dev` is devtmpfs the kernel creates the node with the device, so
//!    step 1 is enough. In a container whose `/dev` is a tmpfs populated once at
//!    start — which is what a node service sees unless it host-mounts `/dev` —
//!    devices created afterwards have no node, and the node must be made
//!    explicitly. This mirrors the `dmsetup mknodes` the device teardown path
//!    already runs for `/dev/mapper` on udev-less hosts.
//!
//! [`ensure_capacity`] does both, at startup, idempotently. It never *removes*
//! a device: `LOOP_CTL_REMOVE` blocks on block-device teardown for on the order
//! of a hundred milliseconds each, and a device that looks free can be attached
//! by the time the removal lands.

use std::collections::BTreeSet;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Character device that accepts `LOOP_CTL_*`.
const LOOP_CONTROL: &str = "/dev/loop-control";
/// Kernel view of which loop devices exist.
const SYS_BLOCK: &str = "/sys/block";
/// Where device nodes live.
const DEV_DIR: &str = "/dev";
/// Reconcile attempts before an unmet target is an error. Two would do for the
/// reserved-index race; three leaves one spare without turning a genuinely
/// refused target into a long stall.
const RECONCILE_PASSES: u32 = 3;
/// `LOOP_CTL_ADD` from `include/linux/loop.h`.
const LOOP_CTL_ADD: libc::c_int = 0x4C80;
/// Block-major of the loop driver, fixed by `Documentation/admin-guide/devices.txt`.
const LOOP_BLOCK_MAJOR: libc::c_uint = 7;
/// Mode for a device node this module creates, used when there is no existing
/// `/dev/loop*` node to copy permissions from.
const FALLBACK_NODE_MODE: u32 = 0o660;

/// Test-hook name overriding the device target, in devices.
pub const LOOP_DEVICE_TARGET_ENV: &str = "SPACE_LOOP_DEVICE_TARGET";

/// Loop devices one sandbox can hold at once: the dm-snapshot origin, its COW,
/// an encrypted data volume, and a staged snapshot volume during
/// suspend/restore. A sandbox that boots straight off its rootfs file holds
/// none, so this over-counts the mixed case on purpose — the cost of a spare
/// device is one `gendisk`, and the cost of running out is every create on the
/// node.
const LOOP_DEVICES_PER_SANDBOX: u32 = 4;

/// How many loop devices a node should have available.
///
/// Bounded at both ends. The floor covers the node's own loopback mounts —
/// `/space/cache` and template builds — on a node configured for very few
/// sandboxes. The ceiling matches what the node image asks the loop driver for,
/// and exists so a mis-derived target cannot ask the kernel for a runaway
/// number of devices.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LoopDeviceTarget(u32);

impl LoopDeviceTarget {
    pub const MIN: u32 = 64;
    pub const MAX: u32 = 10_000;

    /// Derive from the node's slot budget, clamped into range.
    pub fn from_max_slots(max_slots: u32) -> Self {
        Self(
            max_slots
                .saturating_mul(LOOP_DEVICES_PER_SANDBOX)
                .clamp(Self::MIN, Self::MAX),
        )
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for LoopDeviceTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LoopDeviceTargetError {
    #[error("`{0}` is not a device count")]
    NotANumber(String),
    #[error("a device target of {0} is outside {min}..={max}", min = LoopDeviceTarget::MIN, max = LoopDeviceTarget::MAX)]
    OutOfRange(u32),
}

impl FromStr for LoopDeviceTarget {
    type Err = LoopDeviceTargetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parsed: u32 = s
            .trim()
            .parse()
            .map_err(|_| LoopDeviceTargetError::NotANumber(s.to_string()))?;
        if !(Self::MIN..=Self::MAX).contains(&parsed) {
            return Err(LoopDeviceTargetError::OutOfRange(parsed));
        }
        Ok(Self(parsed))
    }
}

impl TryFrom<&str> for LoopDeviceTarget {
    type Error = LoopDeviceTargetError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// The override, if the environment carries one and the `test-hooks` feature
/// is compiled in; release builds never read it, so a node always sizes its
/// devices from the slot budget.
///
/// Every caller resolves a target through this rather than reading the variable
/// itself, so the startup pass and the live slot-increase pass cannot end up
/// disagreeing about what the node was configured for.
pub fn override_from_env() -> Option<String> {
    pplx_space_common_core::env::test_hook_var(LOOP_DEVICE_TARGET_ENV)
}

/// Target to satisfy before a live slot-budget increase becomes visible, or
/// `None` when the update cannot need more devices.
///
/// The device target is derived from the slot budget, so a budget published at
/// runtime moves the target with it. Sizing devices once at startup leaves a
/// node advertising slots it cannot back: the scheduler fills them and the
/// creates fail in `losetup`, on a node that looks correctly configured.
/// Deriving from the *requested* budget rather than the current one is the whole
/// point — sizing from the current value would reproduce the startup-only bug.
///
/// A pinned override still wins, because an operator who names a device count
/// has said what the node should have regardless of its slot budget.
pub fn target_for_slot_increase(
    override_value: Option<&str>,
    current_max_slots: u32,
    requested_max_slots: Option<u32>,
) -> Result<Option<LoopDeviceTarget>, LoopDeviceTargetError> {
    match requested_max_slots {
        Some(requested) if requested > current_max_slots => {
            Ok(Some(resolve_target(override_value, requested)?))
        }
        _ => Ok(None),
    }
}

/// Resolve the target from an operator override and the node's slot budget.
/// The override wins when present and parseable; a malformed one is an error
/// rather than a silent fallback, so a typo in the DaemonSet is visible instead
/// of quietly leaving the node at the derived value.
pub fn resolve_target(
    override_value: Option<&str>,
    max_slots: u32,
) -> Result<LoopDeviceTarget, LoopDeviceTargetError> {
    match override_value.map(str::trim).filter(|v| !v.is_empty()) {
        Some(raw) => raw.parse(),
        None => Ok(LoopDeviceTarget::from_max_slots(max_slots)),
    }
}

/// Where an ensure gave up. A closed set, so it is an enum rather than a
/// string: it is the `phase` tag on the failure counter and the thing an
/// operator pivots on when a node reports it could not reach its target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopCapacityPhase {
    /// `/dev/loop-control` could not be opened, so no device can be added.
    Control,
    /// `/sys/block` could not be listed, so capacity is unknown.
    Enumerate,
    /// `LOOP_CTL_ADD` failed for a specific index.
    AddDevice,
    /// `mknod` failed for a specific index.
    CreateNode,
    /// Every operation reported success, yet the target is still not met.
    NotReached,
}

impl LoopCapacityPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Enumerate => "enumerate",
            Self::AddDevice => "add_device",
            Self::CreateNode => "create_node",
            Self::NotReached => "not_reached",
        }
    }
}

impl std::fmt::Display for LoopCapacityPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoopCapacityError {
    #[error("opening {LOOP_CONTROL} failed: {source}")]
    ControlUnavailable {
        #[source]
        source: std::io::Error,
    },
    #[error("listing {path} failed: {source}")]
    ListDevices {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("LOOP_CTL_ADD for loop{index} failed: {source}")]
    AddDevice {
        index: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("creating device node {path} failed: {source}")]
    CreateNode {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Every individual operation reported success and the target is still not
    /// met. `LOOP_CTL_ADD` allocates the index before it builds the disk and
    /// releases it if that fails, so an `EEXIST` can name a device that never
    /// materializes — the reserving caller can still fail before `add_disk`.
    /// Reporting success there would let a caller publish a slot budget on the
    /// strength of devices the node does not have.
    #[error(
        "only {present} loop devices exist after {passes} reconcile passes; \
         below the target of {target}, {missing_devices} kernel device(s) and \
         {missing_nodes} device node(s) are still absent"
    )]
    CapacityNotReached {
        target: u32,
        present: u32,
        passes: u32,
        missing_devices: usize,
        missing_nodes: usize,
    },
}

impl LoopCapacityError {
    pub fn phase(&self) -> LoopCapacityPhase {
        match self {
            Self::ControlUnavailable { .. } => LoopCapacityPhase::Control,
            Self::ListDevices { .. } => LoopCapacityPhase::Enumerate,
            Self::AddDevice { .. } => LoopCapacityPhase::AddDevice,
            Self::CreateNode { .. } => LoopCapacityPhase::CreateNode,
            Self::CapacityNotReached { .. } => LoopCapacityPhase::NotReached,
        }
    }
}

/// What [`ensure_capacity`] found and changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LoopCapacityReport {
    pub target: u32,
    /// Kernel loop devices present before this ran.
    pub kernel_devices_before: u32,
    /// Kernel loop devices present after, re-read rather than derived from
    /// `before + added`. Every node service on the host starts at once and they
    /// all add devices from the same pool, so a caller's own additions are not
    /// the whole story — one that added 570 devices can find 1200 present.
    pub kernel_devices_after: u32,
    /// Devices this call added. Attribution, not capacity.
    pub kernel_devices_added: u32,
    /// `/dev/loopN` nodes created for devices that had none.
    pub device_nodes_created: u32,
}

impl LoopCapacityReport {
    pub fn changed_nothing(&self) -> bool {
        self.kernel_devices_added == 0 && self.device_nodes_created == 0
    }

    /// Devices the node ended up with.
    pub fn present(&self) -> u32 {
        self.kernel_devices_after
    }
}

/// Loop-device indices the kernel currently has, read from `sysfs` names.
///
/// Rejects anything that is not exactly `loop` followed by digits, so a
/// partitioned device (`loop3p1`, present when the driver runs with a non-zero
/// `part_shift`) is not mistaken for device 31.
fn kernel_indices_from_names<I, S>(names: I) -> BTreeSet<u32>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    names
        .into_iter()
        .filter_map(|name| name.as_ref().strip_prefix("loop")?.parse().ok())
        .collect()
}

/// Indices below `target` that the kernel does not have yet, lowest first.
fn missing_indices(present: &BTreeSet<u32>, target: u32) -> Vec<u32> {
    (0..target).filter(|idx| !present.contains(idx)).collect()
}

fn read_kernel_indices(sys_block: &Path) -> Result<BTreeSet<u32>, LoopCapacityError> {
    let entries =
        std::fs::read_dir(sys_block).map_err(|source| LoopCapacityError::ListDevices {
            path: sys_block.to_path_buf(),
            source,
        })?;
    let names = entries
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned());
    Ok(kernel_indices_from_names(names))
}

/// Mode and group to give a device node we create, copied from an existing
/// `/dev/loop*` node so the ones this module makes are indistinguishable from
/// the ones the kernel made. Falls back to a conservative root-owned 0660 when
/// there is nothing to copy.
fn node_permissions(dev_dir: &Path, present: &BTreeSet<u32>) -> (u32, u32) {
    present
        .iter()
        .find_map(|idx| std::fs::metadata(dev_dir.join(format!("loop{idx}"))).ok())
        .map_or((FALLBACK_NODE_MODE, 0), |meta| {
            (meta.mode() & 0o7777, meta.gid())
        })
}

/// The two privileged operations an ensure performs. Behind a trait for two
/// reasons: a test can reproduce the case that makes the final invariant check
/// necessary — an add that reports success while the device never appears in
/// `/sys/block` — and neither operation is available to an unprivileged test
/// process, since `LOOP_CTL_ADD` needs the control device and `mknod` needs
/// `CAP_MKNOD`. The real implementations are exercised by the root-only
/// integration test.
trait LoopDeviceProvisioner {
    /// Add kernel loop device `index`. `Ok(false)` means it was already there.
    fn add_device(&self, index: u32) -> Result<bool, LoopCapacityError>;

    /// Create `dev_dir/loopN`. `Ok(false)` means it already existed.
    fn create_node(
        &self,
        dev_dir: &Path,
        index: u32,
        mode: u32,
        gid: u32,
    ) -> Result<bool, LoopCapacityError>;
}

/// `LOOP_CTL_ADD` against `/dev/loop-control`, opened on first use so an ensure
/// that needs no new device never touches it.
#[derive(Default)]
struct LoopControl {
    file: std::cell::RefCell<Option<std::fs::File>>,
}

impl LoopDeviceProvisioner for LoopControl {
    fn create_node(
        &self,
        dev_dir: &Path,
        index: u32,
        mode: u32,
        gid: u32,
    ) -> Result<bool, LoopCapacityError> {
        create_device_node(dev_dir, index, mode, gid)
    }

    fn add_device(&self, index: u32) -> Result<bool, LoopCapacityError> {
        let mut slot = self.file.borrow_mut();
        let file = match slot.as_ref() {
            Some(file) => file,
            None => {
                let opened = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(LOOP_CONTROL)
                    .map_err(|source| LoopCapacityError::ControlUnavailable { source })?;
                slot.insert(opened)
            }
        };
        add_kernel_device(std::os::fd::AsRawFd::as_raw_fd(file), index)
    }
}

fn add_kernel_device(control_fd: libc::c_int, index: u32) -> Result<bool, LoopCapacityError> {
    // SAFETY: `control_fd` is an open descriptor on /dev/loop-control for the
    // duration of the call, and LOOP_CTL_ADD takes the index by value.
    let rc = unsafe { libc::ioctl(control_fd, LOOP_CTL_ADD as _, index as libc::c_long) };
    if rc >= 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    // Another process (or a racing node service) got there first.
    if err.raw_os_error() == Some(libc::EEXIST) {
        return Ok(false);
    }
    Err(LoopCapacityError::AddDevice { index, source: err })
}

fn create_device_node(
    dev_dir: &Path,
    index: u32,
    mode: u32,
    gid: u32,
) -> Result<bool, LoopCapacityError> {
    let path = dev_dir.join(format!("loop{index}"));
    if path.exists() {
        return Ok(false);
    }
    use nix::sys::stat::{Mode, SFlag, makedev, mknod};
    if let Err(err) = mknod(
        &path,
        SFlag::S_IFBLK,
        Mode::from_bits_retain(mode),
        makedev(LOOP_BLOCK_MAJOR.into(), index.into()),
    ) {
        let err = std::io::Error::from(err);
        // Lost a race with devtmpfs or a concurrent caller; the node is there.
        if err.raw_os_error() == Some(libc::EEXIST) {
            return Ok(false);
        }
        return Err(LoopCapacityError::CreateNode { path, source: err });
    }
    // mknod masks its mode argument with the process umask, so the node above
    // came out at `mode & !umask` — 0640 rather than 0660 under the usual 022.
    // chmod is not masked. Both this and the chown matter for the same reason:
    // a node this module creates has to be indistinguishable from one the
    // kernel created, or losetup run by a non-root member of the device's
    // group can open it but not attach to it. Neither failure is worth failing
    // startup over, since the node is already usable by root.
    use std::os::unix::fs::PermissionsExt;
    if let Err(error) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)) {
        tracing::warn!(%error, path = %path.display(), "could not set loop device permissions");
    }
    if let Err(error) = nix::unistd::chown(
        &path,
        Some(nix::unistd::Uid::from_raw(0)),
        Some(nix::unistd::Gid::from_raw(gid)),
    ) {
        tracing::warn!(%error, path = %path.display(), "could not set loop device ownership");
    }
    Ok(true)
}

/// Give the node at least `target` loop devices, each with a device node.
///
/// Idempotent: a second call with the same target does nothing and reports
/// nothing changed. Reads `/sys/block` for what the kernel has and `/dev` for
/// what is openable, so it corrects either half independently — a device the
/// kernel already made but whose node is missing gets only the node.
pub fn ensure_capacity(target: LoopDeviceTarget) -> Result<LoopCapacityReport, LoopCapacityError> {
    let sys_block = Path::new(SYS_BLOCK);
    let outcome = ensure_capacity_in(
        sys_block,
        Path::new(DEV_DIR),
        target,
        &LoopControl::default(),
    );
    emit_capacity_telemetry(target, outcome.as_ref(), || {
        read_kernel_indices(sys_block)
            .ok()
            .and_then(|present| u32::try_from(present.len()).ok())
    });
    outcome
}

/// Report the outcome either way.
///
/// Startup deliberately continues when this fails, so the failing case is
/// exactly the one an operator needs on a dashboard. Emitting only on success
/// leaves a degraded node indistinguishable from a healthy one that has not
/// restarted. `present` is read through a callback so the failure path pays for
/// a re-enumeration only when it has no count of its own; when even that fails
/// the gauge is left alone rather than published as a zero the node does not
/// have.
fn emit_capacity_telemetry(
    target: LoopDeviceTarget,
    outcome: Result<&LoopCapacityReport, &LoopCapacityError>,
    present_fallback: impl FnOnce() -> Option<u32>,
) {
    match outcome {
        Ok(report) => {
            pplx_space_metrics::node_manager::loop_device_capacity(report.present(), report.target);
            pplx_space_metrics::node_manager::loop_devices_created(
                report.kernel_devices_added,
                report.device_nodes_created,
            );
        }
        Err(error) => {
            pplx_space_metrics::node_manager::loop_device_capacity_failed(error.phase().as_str());
            let present = match error {
                LoopCapacityError::CapacityNotReached { present, .. } => Some(*present),
                _ => present_fallback(),
            };
            if let Some(present) = present {
                pplx_space_metrics::node_manager::loop_device_capacity(present, target.get());
            }
        }
    }
}

/// Indices below `target` that have no kernel device, and those that have one
/// but no `/dev` node. Both are shortfalls; they are fixed differently.
fn shortfall(present: &BTreeSet<u32>, dev_dir: &Path, target: u32) -> (Vec<u32>, Vec<u32>) {
    let missing = missing_indices(present, target);
    let nodeless = present
        .iter()
        .copied()
        .filter(|index| *index < target && !dev_dir.join(format!("loop{index}")).exists())
        .collect();
    (missing, nodeless)
}

fn ensure_capacity_in(
    sys_block: &Path,
    dev_dir: &Path,
    target: LoopDeviceTarget,
    provisioner: &dyn LoopDeviceProvisioner,
) -> Result<LoopCapacityReport, LoopCapacityError> {
    let mut present = read_kernel_indices(sys_block)?;
    let found = u32::try_from(present.len()).unwrap_or(u32::MAX);
    let mut report = LoopCapacityReport {
        target: target.get(),
        kernel_devices_before: found,
        kernel_devices_after: found,
        ..LoopCapacityReport::default()
    };
    let (mode, gid) = node_permissions(dev_dir, &present);

    // A pass fixes what it can see; the next one re-reads to find out whether it
    // took. More than one pass exists for the reserved-but-never-created index:
    // the reserving caller's cleanup releases the index, so a later add can
    // succeed where the first saw EEXIST. Bounded, because a target the kernel
    // simply will not grant must fail rather than spin.
    let mut passes = 0;
    let (mut missing, mut nodeless) = shortfall(&present, dev_dir, target.get());
    while passes < RECONCILE_PASSES && !(missing.is_empty() && nodeless.is_empty()) {
        passes += 1;
        for index in nodeless {
            if provisioner.create_node(dev_dir, index, mode, gid)? {
                report.device_nodes_created += 1;
            }
        }
        for index in missing {
            if provisioner.add_device(index)? {
                report.kernel_devices_added += 1;
            }
            if provisioner.create_node(dev_dir, index, mode, gid)? {
                report.device_nodes_created += 1;
            }
        }
        present = read_kernel_indices(sys_block)?;
        (missing, nodeless) = shortfall(&present, dev_dir, target.get());
    }

    report.kernel_devices_after = u32::try_from(present.len()).unwrap_or(u32::MAX);
    if !missing.is_empty() || !nodeless.is_empty() {
        return Err(LoopCapacityError::CapacityNotReached {
            target: target.get(),
            present: report.kernel_devices_after,
            passes,
            missing_devices: missing.len(),
            missing_nodes: nodeless.len(),
        });
    }
    Ok(report)
}

#[cfg(test)]
mod tests;

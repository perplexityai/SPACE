//! Evidence for a dm-crypt close the kernel refused as busy.
//!
//! `release` detaches every mount it can see before closing the mapping, so a
//! busy close means something outside node-manager's own mount table still
//! references the device: a file descriptor or mapping on the volume's
//! filesystem, a raw open of the block device, a stacked device-mapper holder,
//! or a copy of the mount in another mount namespace. A lazy unmount does not
//! reach a copy in a namespace whose mounts are private. This module records
//! which of these is present, before `force_free_mapper` renames the mapping
//! and the evidence becomes harder to attribute.
//!
//! The `/proc` walk avoids stat-ing a process's file targets, because a stat
//! on a hung network or FUSE filesystem would block in the kernel.
//! Descriptors are matched by the mount ID in `/proc/<pid>/fdinfo/<fd>`, and
//! mappings by the superblock device `/proc/<pid>/maps` prints. Two kinds of
//! target are stat-ed: those under `/dev/`, to recognise a raw open of the
//! block device, and, while the filesystem is alive, descriptors on a mount
//! no namespace lists. That is how a copy that reached another namespace by
//! propagation looks once the unmount has detached it: its mount ID was never
//! recorded. Those use `statx(AT_STATX_DONT_SYNC)`, and the walk's deadline
//! bounds a stall either way.
//!
//! Collection is best-effort and bounded. Every probe failure is recorded in
//! the report instead of being returned, so diagnostics can never change the
//! outcome of the release they describe.

use std::collections::BTreeSet;
use std::fmt;

use crate::host_command::PROBE_BUDGET;
use crate::rootfs_dmcrypt::parse_device_numbers;

/// Wall-clock bound for each pass of the `/proc` walk.
#[cfg(target_os = "linux")]
const PROC_PASS_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
/// Deadline for the whole `/proc` walk, including a syscall that never
/// returns. Past it the report is logged without the walk's results; the
/// blocking thread finishes on its own and holds no resource meanwhile.
const PROC_SCAN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
#[cfg(target_os = "linux")]
const MAX_NAMESPACES: usize = 16;
#[cfg(target_os = "linux")]
const MAX_PIDS_PER_NAMESPACE: usize = 8;
#[cfg(target_os = "linux")]
const MAX_MOUNTPOINTS_PER_NAMESPACE: usize = 8;
#[cfg(target_os = "linux")]
const MAX_FILE_HOLDERS: usize = 32;
const MAX_COMMAND_OUTPUT_BYTES: usize = 4096;
#[cfg(target_os = "linux")]
const MAX_CMDLINE_BYTES: usize = 256;

/// What identifies the busy volume. Everything but `block` is recorded while
/// node-manager still has the volume mounted, because `release` unmounts it
/// before the close.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct BusyDevice {
    pub(super) mapper: String,
    /// The dm block device (`major:minor`), when `dmsetup` resolved it.
    pub(super) block: Option<(u32, u32)>,
    /// The filesystem's superblock device: mountinfo field 3, and the device
    /// `/proc/<pid>/maps` prints for a file mapped from it. btrfs gives it an
    /// anonymous `0:N` number, distinct both from the dm block device and from
    /// the per-subvolume `st_dev` that `stat` reports, so no `stat` result is
    /// compared with it. The number returns to the pool when the superblock
    /// is torn down, so it identifies the volume only while [`Self::fsid`]
    /// shows the filesystem alive.
    pub(super) superblock: Option<(u32, u32)>,
    /// Mount IDs of the volume's mounts in node-manager's namespace. A
    /// descriptor opened through one keeps its mount ID after the lazy
    /// unmount detaches the mount.
    pub(super) mount_ids: BTreeSet<u64>,
    /// The btrfs filesystem UUID, for the liveness check.
    pub(super) fsid: Option<uuid::Uuid>,
    /// `st_dev` of the mounted filesystem's root, as `stat` reports it for
    /// files on the volume (btrfs: the top subvolume's anonymous device).
    /// Like [`Self::superblock`], it identifies the volume only while the
    /// filesystem is alive.
    pub(super) file_dev: Option<(u32, u32)>,
}

/// A mount namespace that still has the filesystem mounted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NamespaceHolder {
    /// `mnt:[inode]` as read from `/proc/<pid>/ns/mnt`.
    pub(super) namespace: String,
    pub(super) is_node_manager: bool,
    pub(super) mountpoints: Vec<String>,
    /// `pid comm cmdline` for the first processes found in the namespace.
    pub(super) processes: Vec<String>,
}

/// One process reference to the filesystem or block device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileHolder {
    pub(super) pid: u32,
    pub(super) comm: String,
    /// `fd/N`, `mmap`, `cwd` or `root`.
    pub(super) via: String,
    pub(super) path: String,
}

#[derive(Debug)]
pub(super) struct BusyHolderReport {
    pub(super) dmsetup_info: String,
    pub(super) open_count: Option<u32>,
    pub(super) sysfs_holders: Result<Vec<String>, String>,
    /// Whether `/sys/fs/btrfs/<fsid>` exists. `Some(true)`: something still
    /// references the filesystem, a mount copy or an open file. `Some(false)`:
    /// the superblock is gone, so only a raw opener of the block device or a
    /// stacked device can hold it. `None`: no fsid was recorded.
    pub(super) filesystem_alive: Option<bool>,
    pub(super) namespaces: Vec<NamespaceHolder>,
    pub(super) files: Vec<FileHolder>,
    /// The `/proc` walk hit a bound; the lists above are incomplete.
    pub(super) truncated: bool,
    /// The `/proc` walk failed or overran its deadline.
    pub(super) scan_error: Option<String>,
}

/// Collect holder evidence for `device`. Never fails; see the module docs.
/// Takes at most about 10 s: `dmsetup info` within [`PROBE_BUDGET`], then the
/// `/proc` walk within [`PROC_SCAN_DEADLINE`].
pub(super) async fn collect(device: &BusyDevice) -> BusyHolderReport {
    let dmsetup_info = command_output("dmsetup", &["info", &device.mapper]).await;
    let open_count = parse_open_count(&dmsetup_info);
    let block = device
        .block
        .or_else(|| parse_dmsetup_device_numbers(&dmsetup_info));
    let sysfs_holders = match block {
        Some((major, minor)) => read_sysfs_holders(major, minor).await,
        None => Err("block device number unknown".to_string()),
    };

    let scan_device = BusyDevice {
        block,
        ..device.clone()
    };
    // Dropping the JoinHandle on timeout detaches the walk; it opens nothing
    // that outlives it.
    let scan = match tokio::time::timeout(
        PROC_SCAN_DEADLINE,
        tokio::task::spawn_blocking(move || scan_proc(&scan_device)),
    )
    .await
    {
        Ok(Ok(scan)) => scan,
        Ok(Err(error)) => ProcScan::failed(format!("/proc scan task failed: {error}")),
        Err(_) => ProcScan::failed(format!(
            "/proc scan exceeded {}s",
            PROC_SCAN_DEADLINE.as_secs()
        )),
    };

    BusyHolderReport {
        dmsetup_info,
        open_count,
        sysfs_holders,
        filesystem_alive: scan.filesystem_alive,
        namespaces: scan.namespaces,
        files: scan.files,
        truncated: scan.truncated,
        scan_error: scan.error,
    }
}

impl BusyHolderReport {
    /// One-line summary of the namespaces still holding a mount.
    pub(super) fn namespaces_summary(&self) -> String {
        join_display(&self.namespaces)
    }

    /// One-line summary of process file references.
    pub(super) fn files_summary(&self) -> String {
        join_display(&self.files)
    }

    pub(super) fn sysfs_holders_summary(&self) -> String {
        match &self.sysfs_holders {
            Ok(holders) if holders.is_empty() => "none".to_string(),
            Ok(holders) => holders.join(","),
            Err(error) => format!("unavailable: {error}"),
        }
    }
}

impl fmt::Display for NamespaceHolder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{} mounts=[{}] procs=[{}]",
            self.namespace,
            if self.is_node_manager {
                " (node-manager)"
            } else {
                ""
            },
            self.mountpoints.join(","),
            self.processes.join("; "),
        )
    }
}

impl fmt::Display for FileHolder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {} {}", self.pid, self.comm, self.via, self.path)
    }
}

fn join_display<T: fmt::Display>(items: &[T]) -> String {
    if items.is_empty() {
        return "none".to_string();
    }
    items
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" | ")
}

/// Run a diagnostic tool and keep its exit status and both streams.
async fn command_output(program: &str, args: &[&str]) -> String {
    let label = format!("{program} {}", args.join(" "));
    let mut command = std::process::Command::new(program);
    command.args(args);
    let result = tokio::task::spawn_blocking(move || {
        crate::host_command::output(&mut command, PROBE_BUDGET, &[])
    })
    .await;
    let text = match result {
        Ok(Ok(output)) => format!(
            "exit={} stdout={} stderr={}",
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string()),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        ),
        Ok(Err(error)) => format!("unavailable: {error}"),
        Err(error) => format!("unavailable: task failed: {error}"),
    };
    truncate(format!("`{label}` {text}"), MAX_COMMAND_OUTPUT_BYTES)
}

async fn read_sysfs_holders(major: u32, minor: u32) -> Result<Vec<String>, String> {
    let dir = format!("/sys/dev/block/{major}:{minor}/holders");
    let mut entries = tokio::fs::read_dir(&dir)
        .await
        .map_err(|error| format!("{dir}: {error}"))?;
    let mut holders = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|error| format!("{dir}: {error}"))?
    {
        holders.push(entry.file_name().to_string_lossy().into_owned());
    }
    holders.sort();
    Ok(holders)
}

/// `Open count:` from `dmsetup info <name>`.
pub(super) fn parse_open_count(info: &str) -> Option<u32> {
    info.lines()
        .find_map(|line| line.trim().strip_prefix("Open count:"))
        .and_then(|value| value.trim().parse().ok())
}

/// `Major, minor:` from `dmsetup info <name>`.
fn parse_dmsetup_device_numbers(info: &str) -> Option<(u32, u32)> {
    let value = info
        .lines()
        .find_map(|line| line.trim().strip_prefix("Major, minor:"))?;
    let (major, minor) = value.split_once(',')?;
    Some((major.trim().parse().ok()?, minor.trim().parse().ok()?))
}

/// The fields of one mountinfo line this module reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MountLine<'a> {
    pub(super) id: u64,
    pub(super) numbers: Option<(u32, u32)>,
    pub(super) point: &'a str,
    source: &'a str,
}

pub(super) fn parse_mountinfo_line(line: &str) -> Option<MountLine<'_>> {
    let (head, tail) = line.split_once(" - ")?;
    let mut fields = head.split_whitespace();
    let id = fields.next()?.parse().ok()?;
    let numbers = parse_device_numbers(fields.nth(1)?);
    let point = fields.nth(1)?;
    let source = tail.split_whitespace().nth(1)?;
    Some(MountLine {
        id,
        numbers,
        point,
        source,
    })
}

/// Does a mount belong to `device`? It matches on the block device, on a
/// source spelled `/dev/mapper/<name>` or `/dev/dm-<minor>`, or on
/// `superblock` when the caller has established that the superblock is still
/// the volume's. A mapper name alone is not proof after `force_free_mapper`
/// renamed it, which is why the numeric identities are checked too.
pub(super) fn is_volume_mount(
    mount: &MountLine<'_>,
    device: &BusyDevice,
    superblock: Option<(u32, u32)>,
) -> bool {
    let numbers_match = mount
        .numbers
        .is_some_and(|numbers| Some(numbers) == superblock || Some(numbers) == device.block);
    let dm_source = device
        .block
        .is_some_and(|(_, minor)| mount.source == format!("/dev/dm-{minor}"));
    numbers_match || dm_source || mount.source == format!("/dev/mapper/{}", device.mapper)
}

/// `mnt_id:` from `/proc/<pid>/fdinfo/<fd>`.
#[cfg(any(target_os = "linux", test))]
pub(super) fn parse_fdinfo_mount_id(fdinfo: &str) -> Option<u64> {
    fdinfo.lines().find_map(|line| {
        line.strip_prefix("mnt_id:")
            .and_then(|value| value.trim().parse().ok())
    })
}

/// `MM:mm` device field of a `/proc/<pid>/maps` line (hex) and its path.
#[cfg(any(target_os = "linux", test))]
pub(super) fn parse_maps_line(line: &str) -> Option<((u32, u32), &str)> {
    // address perms offset dev inode [path]
    let mut fields = line.splitn(6, ' ');
    let _address = fields.next()?;
    let _perms = fields.next()?;
    let _offset = fields.next()?;
    let (major, minor) = fields.next()?.split_once(':')?;
    let _inode = fields.next()?;
    let path = fields.next().unwrap_or("").trim();
    Some((
        (
            u32::from_str_radix(major, 16).ok()?,
            u32::from_str_radix(minor, 16).ok()?,
        ),
        path,
    ))
}

/// Escape control characters, so a command line or file name cannot inject
/// line breaks or terminal sequences into a log field.
#[cfg(any(target_os = "linux", test))]
pub(super) fn printable(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c.is_control() {
            out.extend(c.escape_default());
        } else {
            out.push(c);
        }
    }
    out
}

fn truncate(mut text: String, limit: usize) -> String {
    if text.len() > limit {
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push('…');
    }
    text
}

#[derive(Debug, Default)]
struct ProcScan {
    filesystem_alive: Option<bool>,
    namespaces: Vec<NamespaceHolder>,
    files: Vec<FileHolder>,
    truncated: bool,
    error: Option<String>,
}

impl ProcScan {
    fn failed(error: String) -> Self {
        Self {
            error: Some(error),
            ..Self::default()
        }
    }
}

#[cfg(target_os = "linux")]
fn scan_proc(device: &BusyDevice) -> ProcScan {
    use std::collections::BTreeMap;
    use std::time::Instant;

    let mut scan = ProcScan {
        filesystem_alive: device
            .fsid
            .map(|fsid| super::mount_identity::btrfs_sysfs_dir(fsid).exists()),
        ..ProcScan::default()
    };
    let pids = match list_pids() {
        Ok(pids) => pids,
        Err(error) => return ProcScan::failed(format!("list /proc: {error}")),
    };
    let own_namespace = std::fs::read_link("/proc/self/ns/mnt")
        .ok()
        .map(|link| link.to_string_lossy().into_owned());
    // A dead superblock's number may already belong to another filesystem,
    // and nothing can hold files on a dead filesystem.
    let filesystem_dead = scan.filesystem_alive == Some(false);
    let mut superblock = device.superblock.filter(|_| !filesystem_dead);

    // Pass 1: one mountinfo read per mount namespace. Mount IDs are global, so
    // a recorded ID seen on another filesystem has been reused.
    let started = Instant::now();
    let mut volume_mount_ids = BTreeSet::new();
    let mut other_mount_ids = BTreeSet::new();
    let mut seen = BTreeSet::new();
    let mut holders: BTreeMap<String, NamespaceHolder> = BTreeMap::new();
    for &pid in &pids {
        if started.elapsed() > PROC_PASS_BUDGET {
            scan.truncated = true;
            break;
        }
        let Ok(link) = std::fs::read_link(format!("/proc/{pid}/ns/mnt")) else {
            continue;
        };
        let namespace = link.to_string_lossy().into_owned();
        if let Some(holder) = holders.get_mut(&namespace) {
            if holder.processes.len() < MAX_PIDS_PER_NAMESPACE {
                holder.processes.push(describe_process(pid));
            } else {
                scan.truncated = true;
            }
            continue;
        }
        if !seen.insert(namespace.clone()) {
            continue;
        }
        let Ok(mountinfo) = std::fs::read_to_string(format!("/proc/{pid}/mountinfo")) else {
            continue;
        };
        let mut mountpoints = Vec::new();
        for mount in mountinfo.lines().filter_map(parse_mountinfo_line) {
            if !is_volume_mount(&mount, device, superblock) {
                other_mount_ids.insert(mount.id);
                continue;
            }
            volume_mount_ids.insert(mount.id);
            // A mount listed now pins its superblock, so its number is
            // current even when none was recorded.
            if superblock.is_none() {
                superblock = mount
                    .numbers
                    .filter(|numbers| Some(*numbers) != device.block);
            }
            mountpoints.push(printable(mount.point));
        }
        if mountpoints.is_empty() {
            continue;
        }
        if holders.len() >= MAX_NAMESPACES {
            scan.truncated = true;
            continue;
        }
        if mountpoints.len() > MAX_MOUNTPOINTS_PER_NAMESPACE {
            mountpoints.truncate(MAX_MOUNTPOINTS_PER_NAMESPACE);
            scan.truncated = true;
        }
        holders.insert(
            namespace.clone(),
            NamespaceHolder {
                is_node_manager: own_namespace.as_deref() == Some(namespace.as_str()),
                namespace,
                mountpoints,
                processes: vec![describe_process(pid)],
            },
        );
    }
    scan.namespaces = holders.into_values().collect();

    // Pass 2: descriptors, mappings, cwd and root on the filesystem, plus raw
    // opens of the block device.
    let mut filesystem_mounts = volume_mount_ids;
    if !filesystem_dead {
        filesystem_mounts.extend(device.mount_ids.difference(&other_mount_ids));
    }
    let file_dev = device
        .file_dev
        .filter(|_| scan.filesystem_alive == Some(true));
    if filesystem_mounts.is_empty()
        && superblock.is_none()
        && file_dev.is_none()
        && device.block.is_none()
    {
        return scan;
    }
    // A mount ID no namespace lists may be a detached propagated copy of the
    // volume; only those fall back to `st_dev`.
    let on_filesystem = |link: &str, mount_id: Option<u64>| match mount_id {
        Some(id) if filesystem_mounts.contains(&id) => true,
        Some(id) if other_mount_ids.contains(&id) => false,
        _ => file_dev.is_some_and(|dev| statx_dev(link) == Some(dev)),
    };
    let started = Instant::now();
    'pids: for &pid in &pids {
        if started.elapsed() > PROC_PASS_BUDGET || scan.files.len() >= MAX_FILE_HOLDERS {
            scan.truncated = true;
            break;
        }
        let push = |via: String, path: String, scan: &mut ProcScan| {
            scan.files.push(FileHolder {
                pid,
                comm: read_comm(pid),
                via,
                path: printable(&path),
            });
            scan.files.len() < MAX_FILE_HOLDERS
        };
        for link in ["cwd", "root"] {
            let path = format!("/proc/{pid}/{link}");
            if on_filesystem(&path, link_mount_id(&path))
                && !push(link.to_string(), read_link_lossy(&path), &mut scan)
            {
                break 'pids;
            }
        }
        if let Ok(entries) = std::fs::read_dir(format!("/proc/{pid}/fd")) {
            for entry in entries.flatten() {
                let fd = entry.file_name();
                let fd = fd.to_string_lossy();
                let link = format!("/proc/{pid}/fd/{fd}");
                // Sockets, pipes and anonymous inodes have no path.
                let Ok(target) = std::fs::read_link(&link) else {
                    continue;
                };
                let target = target.to_string_lossy().into_owned();
                if !target.starts_with('/') {
                    continue;
                }
                let mount_id = std::fs::read_to_string(format!("/proc/{pid}/fdinfo/{fd}"))
                    .ok()
                    .and_then(|fdinfo| parse_fdinfo_mount_id(&fdinfo));
                if (on_filesystem(&link, mount_id) || is_block_device(&link, &target, device.block))
                    && !push(format!("fd/{fd}"), target, &mut scan)
                {
                    break 'pids;
                }
            }
        }
        if let (Some(superblock), Ok(maps)) = (
            superblock,
            std::fs::read_to_string(format!("/proc/{pid}/maps")),
        ) {
            let paths: BTreeSet<&str> = maps
                .lines()
                .filter_map(parse_maps_line)
                .filter(|(numbers, _)| *numbers == superblock)
                .map(|(_, path)| path)
                .collect();
            for path in paths {
                if !push("mmap".to_string(), path.to_string(), &mut scan) {
                    break 'pids;
                }
            }
        }
    }
    scan
}

#[cfg(not(target_os = "linux"))]
fn scan_proc(_device: &BusyDevice) -> ProcScan {
    ProcScan::failed("/proc scan is only supported on Linux".to_string())
}

/// Is the descriptor at `link` a raw open of the `block` device? Only
/// `/dev/` targets are stat-ed; see the module docs.
#[cfg(target_os = "linux")]
fn is_block_device(link: &str, target: &str, block: Option<(u32, u32)>) -> bool {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let Some((major, minor)) = block else {
        return false;
    };
    target.starts_with("/dev/")
        && std::fs::metadata(link).is_ok_and(|metadata| {
            metadata.file_type().is_block_device() && metadata.rdev() == libc::makedev(major, minor)
        })
}

/// `st_dev` of the file `path` resolves to, without syncing attributes from
/// a remote filesystem.
#[cfg(target_os = "linux")]
fn statx_dev(path: &str) -> Option<(u32, u32)> {
    use rustix::fs::{AtFlags, StatxFlags};

    let stat = rustix::fs::statx(
        rustix::fs::CWD,
        path,
        AtFlags::STATX_DONT_SYNC,
        StatxFlags::empty(),
    )
    .ok()?;
    Some((stat.stx_dev_major, stat.stx_dev_minor))
}

/// Mount ID of the directory a `/proc/<pid>/{cwd,root}` link names, read
/// from an `O_PATH` descriptor's fdinfo instead of a stat of the target.
#[cfg(target_os = "linux")]
fn link_mount_id(path: &str) -> Option<u64> {
    use rustix::fs::{Mode, OFlags};
    use std::os::fd::AsRawFd;

    let fd = rustix::fs::open(path, OFlags::PATH | OFlags::CLOEXEC, Mode::empty()).ok()?;
    let fdinfo = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", fd.as_raw_fd())).ok()?;
    parse_fdinfo_mount_id(&fdinfo)
}

#[cfg(target_os = "linux")]
fn list_pids() -> std::io::Result<Vec<u32>> {
    let mut pids: Vec<u32> = std::fs::read_dir("/proc")?
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse().ok())
        .collect();
    pids.sort_unstable();
    Ok(pids)
}

#[cfg(target_os = "linux")]
fn read_comm(pid: u32) -> String {
    std::fs::read_to_string(format!("/proc/{pid}/comm"))
        .map(|comm| printable(comm.trim()))
        .unwrap_or_else(|_| "?".to_string())
}

#[cfg(target_os = "linux")]
fn read_link_lossy(path: &str) -> String {
    std::fs::read_link(path)
        .map(|target| target.to_string_lossy().into_owned())
        .unwrap_or_else(|error| format!("? ({error})"))
}

#[cfg(target_os = "linux")]
fn describe_process(pid: u32) -> String {
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline"))
        .map(|raw| {
            String::from_utf8_lossy(&raw)
                .split('\0')
                .filter(|arg| !arg.is_empty())
                .map(printable)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();
    truncate(
        format!("{pid} {} {cmdline}", read_comm(pid)),
        MAX_CMDLINE_BYTES,
    )
}

#[cfg(test)]
mod tests;

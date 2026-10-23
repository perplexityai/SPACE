//! btrfs subvolume helpers used by SPACE template + sandbox lifecycle.
//!
//! SPACE's `/space/cache` is a btrfs filesystem on every node. Sandbox
//! `instance/` directories are btrfs subvolumes today; this module adds the
//! plumbing needed to make template directories subvolumes too, and to stamp
//! each template subvolume with a content-addressed `received_uuid` so that
//! `btrfs send -p` streams produced on one node can be received on any
//! other node that has installed the same `(template_name, content_hash)`
//! build locally.
//!
//! # The deterministic-UUID story
//!
//! `btrfs send -p` writes the parent's UUID into the stream header as a
//! 16-byte field. The receiver looks up a subvolume on its local filesystem
//! by that UUID (checking `uuid` and `received_uuid`) and applies the diff
//! against it. So two template subvolumes — independently built on two
//! different nodes from the exact same artifacts — need to share a UUID
//! to be interchangeable as send-stream parents.
//!
//! Rather than distribute btrfs send streams for the templates themselves
//! (which would force a build-pipeline change and add a second cross-node
//! distribution mechanism), we derive the UUID from the template's
//! `(name, content_hash)` fingerprint via UUIDv5. Any node that installs
//! the same `(name, content_hash)` locally arrives at the exact same 16
//! bytes without coordination. We then stamp that value as the subvolume's
//! `received_uuid` via the `BTRFS_IOC_SET_RECEIVED_SUBVOL` ioctl — the same
//! kernel path `btrfs receive` uses internally.
//!
//! # Failure mode
//!
//! Every operation here can fail when:
//!  - The path isn't on btrfs (e.g. an integration test on tmpfs).
//!  - The kernel `btrfs` module is missing / disabled.
//!  - `btrfs-progs` isn't installed (we shell out for `create`/`snapshot`
//!    rather than carry a third set of ioctl wrappers — the CLI is the
//!    documented stable interface).
//!
//! Callers fall back to plain directories on error so a non-btrfs host
//! degrades to the legacy `create + reflink` lifecycle.
//!
//! It is its own crate so that code can use these helpers without depending
//! on `pplx-space-common-core`. `pplx-space-common-core` re-exports it from
//! `host::btrfs`.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use uuid::Uuid;

const BTRFS_FILESYSTEM_USAGE_TIMEOUT: Duration = Duration::from_secs(2);
// `balance status` is a fast ioctl; `balance start --bg` forks the worker and
// returns once the kernel accepts the operation. Neither should approach this.
const BTRFS_BALANCE_CMD_TIMEOUT: Duration = Duration::from_secs(30);
const COMMAND_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const COMMAND_KILL_REAP_GRACE: Duration = Duration::from_millis(100);
static FILESYSTEM_USAGE_PROBE_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

/// Errors from btrfs subvolume operations. Per-operation context (the path
/// involved and a short description) is preserved so the caller can decide
/// whether to fall back to a non-btrfs path or propagate.
#[derive(Debug, thiserror::Error)]
pub enum BtrfsError {
    /// The `btrfs` CLI was not on `PATH` or could not be spawned.
    #[error("spawn `btrfs` CLI: {0}")]
    SpawnBtrfs(#[source] std::io::Error),

    /// `btrfs subvolume create|snapshot|show` exited non-zero. Carries the
    /// stderr verbatim — the CLI's error messages are typically the most
    /// useful diagnostic the caller can surface to its own logs.
    #[error("btrfs {op} {path}: {stderr}")]
    CliFailed {
        op: &'static str,
        path: PathBuf,
        stderr: String,
    },

    /// The `btrfs` CLI did not finish inside the operation's bounded probe
    /// window. This keeps admission refreshers recoverable when the filesystem
    /// is sick enough to wedge a userspace probe.
    #[error("btrfs {op} {path} timed out after {timeout:?}")]
    CliTimedOut {
        op: &'static str,
        path: PathBuf,
        timeout: Duration,
    },

    /// `open(path, O_RDONLY)` failed when preparing for an ioctl. Usually
    /// "no such file" if the caller hands us a path that doesn't exist
    /// yet, or `EPERM`/`EACCES` if the helper isn't running as root.
    #[error("open {path} for ioctl: {source}")]
    OpenFd {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// `BTRFS_IOC_SET_RECEIVED_SUBVOL` returned an error. Most commonly
    /// `ENOTTY` (filesystem isn't btrfs) or `EPERM` (not root).
    #[error("set_received_subvol ioctl on {path}: {source}")]
    SetReceivedUuid {
        path: PathBuf,
        #[source]
        source: nix::Error,
    },

    /// `btrfs subvolume show` output could not be parsed. Output format is
    /// stable in practice but explicit fallthrough keeps us from panicking
    /// on a future change.
    #[error("parse `btrfs subvolume show` output for {path}: {detail}")]
    ParseShow { path: PathBuf, detail: String },

    /// `btrfs filesystem usage --raw` output could not be parsed.
    #[error("parse `btrfs filesystem usage --raw` output for {path}: {detail}")]
    ParseFilesystemUsage { path: PathBuf, detail: String },
}

/// True iff `path` is a btrfs subvolume root (not a regular directory
/// inside a subvolume and not a non-btrfs directory). A `false` answer
/// covers both fallback-worthy cases.
///
/// Detection is stat-based and needs no privilege: btrfs assigns inode
/// number 256 (`BTRFS_FIRST_FREE_OBJECTID`) to the root directory of
/// every subvolume, and ordinary directories never get it, so
/// "directory + inode 256 + btrfs superblock magic" identifies a
/// subvolume root exactly. The magic check keeps a coincidental
/// inode-256 directory on a non-btrfs filesystem from false-positiving.
///
/// Probing with `btrfs subvolume show` instead would report `false`
/// for every real subvolume when the caller lacks `CAP_SYS_ADMIN`
/// (its B-tree search ioctl is privileged), and several callers —
/// production template-manager among them — run unprivileged. A
/// privilege-dependent answer here silently routes read-only
/// subvolumes onto plain `remove_dir_all` paths, which fail with
/// EROFS.
///
/// Note that a btrfs filesystem's top-level root (e.g. a mount point
/// like `/space/cache`) is itself a subvolume root and answers `true`.
/// Callers on cleanup paths must not pass a mount point expecting a
/// plain-directory answer.
#[cfg(target_os = "linux")]
pub fn is_subvolume(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    const BTRFS_FIRST_FREE_OBJECTID: u64 = 256;
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    if !metadata.is_dir() || metadata.ino() != BTRFS_FIRST_FREE_OBJECTID {
        return false;
    }
    nix::sys::statfs::statfs(path)
        .map(|fs| fs.filesystem_type() == nix::sys::statfs::BTRFS_SUPER_MAGIC)
        .unwrap_or(false)
}

/// Non-Linux hosts have no btrfs; every path is a plain directory to
/// the callers' fallback logic.
#[cfg(not(target_os = "linux"))]
pub fn is_subvolume(_path: &Path) -> bool {
    false
}

/// Create `path` as a fresh btrfs subvolume. The parent directory must
/// exist; the path itself must not. Errors if `btrfs subvolume create`
/// reports failure — the caller is expected to either retry, fall back to
/// `std::fs::create_dir`, or propagate.
pub fn create_subvolume(path: &Path) -> Result<(), BtrfsError> {
    let out = Command::new("btrfs")
        .args(["subvolume", "create", "--"])
        .arg(path)
        .output()
        .map_err(BtrfsError::SpawnBtrfs)?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op: "subvolume create",
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(())
}

/// Snapshot `src` (a subvolume) into `dst`. The destination must not exist;
/// the parent must. `readonly: true` produces an immutable read-only
/// snapshot (used at suspend time for an atomic capture); `readonly: false`
/// produces a writable snapshot (used for sandbox `instance` creation from
/// a template).
///
/// The resulting subvolume has `Parent UUID = <src.uuid>` and the
/// `received_uuid` stamped on `src` (if any) is **not** inherited — that's
/// btrfs semantics, and it's what we want: the per-attempt sandbox
/// instance gets a fresh identity, while the parent reference traces back
/// to the template.
pub fn snapshot_subvolume(src: &Path, dst: &Path, readonly: bool) -> Result<(), BtrfsError> {
    let mut cmd = Command::new("btrfs");
    cmd.args(["subvolume", "snapshot"]);
    if readonly {
        cmd.arg("-r");
    }
    cmd.arg("--").arg(src).arg(dst);
    let out = cmd.output().map_err(BtrfsError::SpawnBtrfs)?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op: "subvolume snapshot",
            path: dst.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(())
}

/// Frozen namespace for `deterministic_template_received_uuid`. Generated
/// once with `uuidgen --random`; pinned here forever because the UUIDv5
/// values it seeds become wire-format in every btrfs send stream that
/// references a template-as-parent. Regenerating this constant would
/// invalidate every existing stream. **Do not change.**
pub const SPACE_TEMPLATE_RECEIVED_UUID_NS: Uuid =
    Uuid::from_u128(0x1f3a_9c44_5e6b_47d8_b9c1_2a0f_8e51_7d36);

/// Compute the deterministic `received_uuid` to stamp on a template
/// subvolume installed from `(name, content_hash)`.
///
/// Two nodes that independently install the same `(name, content_hash)`
/// build arrive at the exact same 16-byte UUID via UUIDv5 (SHA-1 of
/// `namespace || name`), enabling `btrfs send -p` streams produced on one
/// node to be received on the other without distributing template send
/// streams.
///
/// `content_hash` is the template build's content-addressed digest
/// (typically `"sha256:..."`); it's included in the v5 name so two
/// distinct builds of the same template name produce distinct UUIDs and
/// cannot be confused for each other.
pub fn deterministic_template_received_uuid(name: &str, content_hash: &str) -> Uuid {
    let name = format!("{name}@{content_hash}");
    Uuid::new_v5(&SPACE_TEMPLATE_RECEIVED_UUID_NS, name.as_bytes())
}

/// Stamp `uuid` as the subvolume's `received_uuid` via
/// `BTRFS_IOC_SET_RECEIVED_SUBVOL`. The kernel call also sets / clears the
/// associated stransid + flags fields; we leave them at zero, matching
/// what `btrfs receive` writes for a fresh stream with no incremental
/// parent context.
///
/// Requires root (or CAP_SYS_ADMIN on the cache mount).
pub fn set_received_uuid(path: &Path, uuid: Uuid) -> Result<(), BtrfsError> {
    let fd = open_subvol_fd(path)?;
    set_received_uuid_on_fd(&fd, uuid).map_err(|source| BtrfsError::SetReceivedUuid {
        path: path.to_path_buf(),
        source,
    })
}

/// Stamp a pinned subvolume without reopening its mutable pathname.
pub fn set_received_uuid_on_fd(fd: &File, uuid: Uuid) -> Result<(), nix::Error> {
    let mut args = BtrfsIoctlReceivedSubvolArgs {
        uuid: *uuid.as_bytes(),
        ..BtrfsIoctlReceivedSubvolArgs::zeroed()
    };
    // SAFETY: args is the complete repr(C) SET_RECEIVED_SUBVOL buffer and
    // fd remains borrowed for the entire synchronous kernel operation.
    unsafe { btrfs_ioc_set_received_subvol(fd.as_raw_fd(), &mut args) }?;
    Ok(())
}

/// Toggle the subvolume's read-only flag. `readonly = true` makes the
/// subvolume immutable — required for it to serve as a `btrfs send -p`
/// parent (the kernel rejects parent references to writable
/// subvolumes). Templates land RO at install time so the suspend
/// codec can run `btrfs send -p <template_subvol> <instance_ro_snap>`
/// without any extra preparation step; sandbox creation still uses
/// `btrfs subvolume snapshot <ro_template> <rw_instance>` to produce a
/// writable instance.
///
/// Returns `Ok(())` if the flag already matches the requested state —
/// the kernel call is idempotent and we don't reach for the CLI to
/// check first.
pub fn set_readonly(path: &Path, readonly: bool) -> Result<(), BtrfsError> {
    let value = if readonly { "true" } else { "false" };
    let mut cmd = Command::new("btrfs");
    cmd.args(["property", "set", "-ts"]);
    // Going RO -> RW on a subvolume that was produced by `btrfs receive`
    // (received_uuid != 0) is refused by the kernel unless `-f` is set,
    // because flipping it loses the send/receive lineage guarantee. The
    // restore path deliberately flips received instances to RW so FC can
    // write to them — they're not meant to be re-sent. RW -> RO never
    // needs force, so only opt in for the RW direction.
    if !readonly {
        cmd.arg("-f");
    }
    let out = cmd
        .arg(path)
        .args(["ro", value])
        .output()
        .map_err(BtrfsError::SpawnBtrfs)?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op: "property set ro",
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(())
}

/// Identity fields read from `btrfs subvolume show <path>`. Used by the
/// suspend codec to verify, before opening a `btrfs send -p`, that the
/// sandbox `instance` actually descends from the local template (so the
/// emitted stream is decodable on any other node that installed the
/// same `(template_name, content_hash)`).
///
/// All three fields are `None` when the kernel reports `-` for that
/// slot (e.g. a freshly-`subvolume create`d subvolume has no `Parent
/// UUID` and no `Received UUID`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubvolumeInfo {
    pub uuid: Option<Uuid>,
    pub parent_uuid: Option<Uuid>,
    pub received_uuid: Option<Uuid>,
}

impl SubvolumeInfo {
    fn empty() -> Self {
        Self {
            uuid: None,
            parent_uuid: None,
            received_uuid: None,
        }
    }
}

/// Parse `btrfs subvolume show <path>` and return its identity fields.
/// Errors when the CLI fails (path not a subvolume, fs not btrfs,
/// permission denied); a successful call with all-`-` fields returns a
/// `SubvolumeInfo` with three `None`s.
pub fn read_subvolume_info(path: &Path) -> Result<SubvolumeInfo, BtrfsError> {
    let out = Command::new("btrfs")
        .args(["subvolume", "show", "--"])
        .arg(path)
        .output()
        .map_err(BtrfsError::SpawnBtrfs)?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op: "subvolume show",
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut info = SubvolumeInfo::empty();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("UUID:") {
            info.uuid = parse_show_uuid(path, "UUID", rest)?;
        } else if let Some(rest) = trimmed.strip_prefix("Parent UUID:") {
            info.parent_uuid = parse_show_uuid(path, "Parent UUID", rest)?;
        } else if let Some(rest) = trimmed.strip_prefix("Received UUID:") {
            info.received_uuid = parse_show_uuid(path, "Received UUID", rest)?;
        }
    }
    Ok(info)
}

fn parse_show_uuid(path: &Path, field: &str, raw: &str) -> Result<Option<Uuid>, BtrfsError> {
    let value = raw.trim();
    if value.is_empty() || value == "-" {
        return Ok(None);
    }
    Uuid::parse_str(value)
        .map(Some)
        .map_err(|e| BtrfsError::ParseShow {
            path: path.to_path_buf(),
            detail: format!("invalid {field} `{value}`: {e}"),
        })
}

/// Delete a btrfs subvolume at `path`. No-op if `path` doesn't exist.
/// Used by cleanup paths that need to wipe an install-in-progress or
/// hash-mismatched build whose top-level directory might be a
/// subvolume root — `std::fs::remove_dir_all` recurses into contents
/// fine but cannot `rmdir` the subvolume root on default-mounted
/// btrfs, which would leave an empty undeletable subvolume that future
/// retries mistake for a half-finished install. Also used by the
/// suspend codec to clean up the read-only `instance.ro` snapshot
/// that `btrfs send -p` reads from, and by the restore codec to wipe
/// stray `instance.ro` subvolumes left by a crashed receive.
///
/// Callers should clear the subvolume's read-only flag first if it's
/// already RO — `btrfs subvolume delete` rejects RO targets.
pub fn delete_subvolume_if_present(path: &Path) -> Result<(), BtrfsError> {
    if !path.exists() {
        return Ok(());
    }
    let out = Command::new("btrfs")
        .args(["subvolume", "delete", "--"])
        .arg(path)
        .output()
        .map_err(BtrfsError::SpawnBtrfs)?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op: "subvolume delete",
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(())
}

/// Best-effort delete-subvolume that handles the RO-rejects-delete
/// invariant: clears the RO flag (silently ignoring errors — the
/// subvolume might be RW already, or might not exist at all) and
/// then runs `delete_subvolume_if_present`.
///
/// Use this from cleanup paths that wipe subvolumes which might be
/// in either RW or RO state — e.g. the suspend codec's `instance.ro`
/// (RO from `btrfs subvolume snapshot -r`), the restore codec's
/// received subvolumes (RO until the explicit force toggle), or
/// stale leftovers from crashed prior attempts. Plain
/// `delete_subvolume_if_present` would silently fail on these because
/// `btrfs subvolume delete` rejects RO targets.
pub fn force_delete_subvolume_if_present(path: &Path) -> Result<(), BtrfsError> {
    if !path.exists() {
        return Ok(());
    }
    // Ignore the result: `set_readonly(false)` is allowed to fail if the
    // subvolume is already RW, or if path isn't a subvolume at all. The
    // delete below surfaces the useful cleanup error in that case.
    let _ = set_readonly(path, false).is_err();
    delete_subvolume_if_present(path)
}

/// Read the subvolume's current `Received UUID` field, or `None` if it's
/// unset (the kernel reports `-` in this case). Used to make the
/// stamp-received-uuid call idempotent: callers can skip the ioctl if the
/// value is already what they intend to write.
pub fn read_received_uuid(path: &Path) -> Result<Option<Uuid>, BtrfsError> {
    let out = Command::new("btrfs")
        .args(["subvolume", "show", "--"])
        .arg(path)
        .output()
        .map_err(BtrfsError::SpawnBtrfs)?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op: "subvolume show",
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("Received UUID:") {
            let value = rest.trim();
            if value.is_empty() || value == "-" {
                return Ok(None);
            }
            let parsed = Uuid::parse_str(value).map_err(|e| BtrfsError::ParseShow {
                path: path.to_path_buf(),
                detail: format!("invalid Received UUID `{value}`: {e}"),
            })?;
            return Ok(Some(parsed));
        }
    }
    Err(BtrfsError::ParseShow {
        path: path.to_path_buf(),
        detail: "no `Received UUID:` line in `btrfs subvolume show` output".to_string(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilesystemUsage {
    pub device_unallocated_bytes: u64,
    pub metadata_total_bytes: u64,
    pub metadata_used_bytes: u64,
    pub data_total_bytes: u64,
    pub data_used_bytes: u64,
}

impl FilesystemUsage {
    pub fn metadata_available_bytes(self) -> u64 {
        self.metadata_total_bytes
            .saturating_sub(self.metadata_used_bytes)
    }

    /// Free bytes trapped inside allocated data block groups. This is the
    /// space a data balance can return to the unallocated pool; it is *not*
    /// available for metadata chunk growth until a balance reclaims it.
    pub fn data_slack_bytes(self) -> u64 {
        self.data_total_bytes.saturating_sub(self.data_used_bytes)
    }
}

pub fn read_filesystem_usage(path: &Path) -> Result<FilesystemUsage, BtrfsError> {
    let op = "filesystem usage --raw";
    let Some(probe_guard) = ProbeInFlightGuard::try_acquire(&FILESYSTEM_USAGE_PROBE_IN_FLIGHT)
    else {
        return Err(BtrfsError::CliTimedOut {
            op,
            path: path.to_path_buf(),
            timeout: BTRFS_FILESYSTEM_USAGE_TIMEOUT,
        });
    };
    let out = command_output_with_timeout_guarded(
        Command::new("btrfs")
            .args(["filesystem", "usage", "--raw"])
            .arg(path),
        BTRFS_FILESYSTEM_USAGE_TIMEOUT,
        Some(probe_guard),
    )
    .map_err(BtrfsError::SpawnBtrfs)?
    .ok_or_else(|| BtrfsError::CliTimedOut {
        op,
        path: path.to_path_buf(),
        timeout: BTRFS_FILESYSTEM_USAGE_TIMEOUT,
    })?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op,
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    parse_filesystem_usage(path, &String::from_utf8_lossy(&out.stdout))
}

/// True iff a balance is currently running on the filesystem containing
/// `path`. `btrfs balance status` exits 0 when idle, 1 when a balance is
/// running, 2 on error.
pub fn balance_running(path: &Path) -> Result<bool, BtrfsError> {
    let op = "balance status";
    let out = command_output_with_timeout_guarded(
        Command::new("btrfs").args(["balance", "status"]).arg(path),
        BTRFS_BALANCE_CMD_TIMEOUT,
        None,
    )
    .map_err(BtrfsError::SpawnBtrfs)?
    .ok_or_else(|| BtrfsError::CliTimedOut {
        op,
        path: path.to_path_buf(),
        timeout: BTRFS_BALANCE_CMD_TIMEOUT,
    })?;
    match out.status.code() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        _ => Err(BtrfsError::CliFailed {
            op,
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        }),
    }
}

/// Start a background data-chunk balance stage on the filesystem containing
/// `path`: relocates data block groups at or below `usage_percent`
/// utilization (at most `chunk_limit` of them when set), returning their
/// space to the unallocated pool. `--bg` hands the operation to the kernel
/// and returns immediately; poll [`balance_running`] for completion.
/// Metadata chunks are deliberately never balanced here — compacting
/// metadata under space pressure risks the ENOSPC wedge that reclaiming
/// data chunks exists to prevent.
pub fn start_data_balance(
    path: &Path,
    usage_percent: u8,
    chunk_limit: Option<u32>,
) -> Result<(), BtrfsError> {
    let op = "balance start";
    let mut filter = format!("-dusage={usage_percent}");
    if let Some(limit) = chunk_limit {
        filter.push_str(&format!(",limit={limit}"));
    }
    let out = command_output_with_timeout_guarded(
        Command::new("btrfs")
            .args(["balance", "start", "--bg", &filter])
            .arg(path),
        BTRFS_BALANCE_CMD_TIMEOUT,
        None,
    )
    .map_err(BtrfsError::SpawnBtrfs)?
    .ok_or_else(|| BtrfsError::CliTimedOut {
        op,
        path: path.to_path_buf(),
        timeout: BTRFS_BALANCE_CMD_TIMEOUT,
    })?;
    if !out.status.success() {
        return Err(BtrfsError::CliFailed {
            op,
            path: path.to_path_buf(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        });
    }
    Ok(())
}

fn command_output_with_timeout_guarded(
    command: &mut Command,
    timeout: Duration,
    probe_guard: Option<ProbeInFlightGuard>,
) -> std::io::Result<Option<Output>> {
    command_output_with_timeout_and_reap_grace(
        command,
        timeout,
        COMMAND_KILL_REAP_GRACE,
        None,
        probe_guard,
    )
}

fn command_output_with_timeout_and_reap_grace(
    command: &mut Command,
    timeout: Duration,
    kill_reap_grace: Duration,
    reaped_tx: Option<std::sync::mpsc::Sender<()>>,
    mut probe_guard: Option<ProbeInFlightGuard>,
) -> std::io::Result<Option<Output>> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let started_at = Instant::now();

    loop {
        if child.try_wait()?.is_some() {
            return child.wait_with_output().map(Some);
        }

        if started_at.elapsed() >= timeout {
            let _ = child.kill();
            if wait_for_child_exit(&mut child, kill_reap_grace)? {
                return Ok(None);
            }
            reap_child_async(child, reaped_tx, probe_guard.take());
            return Ok(None);
        }

        std::thread::sleep(
            COMMAND_WAIT_POLL_INTERVAL.min(timeout.saturating_sub(started_at.elapsed())),
        );
    }
}

fn wait_for_child_exit(child: &mut Child, grace: Duration) -> std::io::Result<bool> {
    let started_at = Instant::now();
    while started_at.elapsed() < grace {
        if child.try_wait()?.is_some() {
            return Ok(true);
        }
        std::thread::sleep(
            COMMAND_WAIT_POLL_INTERVAL.min(grace.saturating_sub(started_at.elapsed())),
        );
    }
    Ok(false)
}

struct ProbeInFlightGuard {
    active: &'static AtomicBool,
}

impl ProbeInFlightGuard {
    fn try_acquire(active: &'static AtomicBool) -> Option<Self> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(Self { active })
    }
}

impl Drop for ProbeInFlightGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

fn reap_child_async(
    mut child: Child,
    reaped_tx: Option<std::sync::mpsc::Sender<()>>,
    probe_guard: Option<ProbeInFlightGuard>,
) {
    let _join_handle = std::thread::spawn(move || {
        let _probe_guard = probe_guard;
        let _ = child.wait();
        if let Some(reaped_tx) = reaped_tx {
            let _ = reaped_tx.send(());
        }
    });
}

pub fn parse_filesystem_usage(path: &Path, raw: &str) -> Result<FilesystemUsage, BtrfsError> {
    let mut device_unallocated_bytes = None;
    let mut metadata_total_bytes = 0_u64;
    let mut metadata_used_bytes = 0_u64;
    let mut data_total_bytes = 0_u64;
    let mut data_used_bytes = 0_u64;

    for line in raw.lines().map(str::trim) {
        if let Some(rest) = line.strip_prefix("Device unallocated:") {
            device_unallocated_bytes = Some(parse_first_u64(path, "Device unallocated", rest)?);
            continue;
        }
        if line.starts_with("Metadata,") {
            let (total, used) = parse_size_used(path, line)?;
            metadata_total_bytes = metadata_total_bytes.saturating_add(total);
            metadata_used_bytes = metadata_used_bytes.saturating_add(used);
        }
        if line.starts_with("Data,") {
            let (total, used) = parse_size_used(path, line)?;
            data_total_bytes = data_total_bytes.saturating_add(total);
            data_used_bytes = data_used_bytes.saturating_add(used);
        }
    }

    Ok(FilesystemUsage {
        device_unallocated_bytes: device_unallocated_bytes.ok_or_else(|| {
            BtrfsError::ParseFilesystemUsage {
                path: path.to_path_buf(),
                detail: "missing `Device unallocated:` line".to_string(),
            }
        })?,
        metadata_total_bytes,
        metadata_used_bytes,
        data_total_bytes,
        data_used_bytes,
    })
}

fn parse_size_used(path: &Path, line: &str) -> Result<(u64, u64), BtrfsError> {
    let size = parse_labelled_u64(path, line, "Size:")?;
    let used = parse_labelled_u64(path, line, "Used:")?;
    Ok((size, used))
}

fn parse_labelled_u64(path: &Path, line: &str, label: &'static str) -> Result<u64, BtrfsError> {
    let Some((_, rest)) = line.split_once(label) else {
        return Err(BtrfsError::ParseFilesystemUsage {
            path: path.to_path_buf(),
            detail: format!("missing `{label}` in `{line}`"),
        });
    };
    parse_first_u64(path, label, rest)
}

fn parse_first_u64(path: &Path, field: &str, raw: &str) -> Result<u64, BtrfsError> {
    let token = raw
        .trim()
        .split(|ch: char| !ch.is_ascii_digit())
        .find(|part| !part.is_empty())
        .ok_or_else(|| BtrfsError::ParseFilesystemUsage {
            path: path.to_path_buf(),
            detail: format!("missing numeric value for `{field}`"),
        })?;
    token
        .parse::<u64>()
        .map_err(|error| BtrfsError::ParseFilesystemUsage {
            path: path.to_path_buf(),
            detail: format!("parse `{field}` value `{token}`: {error}"),
        })
}

fn open_subvol_fd(path: &Path) -> Result<File, BtrfsError> {
    // The ioctl just needs a file descriptor for the subvolume's root
    // directory. `std::fs::File::open` on a directory returns a fd in
    // O_RDONLY mode on Linux — exactly what we need — and saves us a
    // dependency on `nix`'s `fs` feature for the open call itself.
    File::open(path).map_err(|source| BtrfsError::OpenFd {
        path: path.to_path_buf(),
        source,
    })
}

// ---- ioctl plumbing ------------------------------------------------------

/// Mirror of `struct btrfs_ioctl_received_subvol_args` from
/// `include/uapi/linux/btrfs.h`. Stable kernel ABI; the 16-slot
/// `reserved` tail is required for forward compatibility — the kernel
/// validates the size of the buffer it copies in/out.
///
/// **Field order matches the kernel exactly.** `rtransid` comes between
/// `stransid` and `stime` in the kernel header, not after `stime` — a
/// rearranged struct silently misaligns every field after `uuid`. Today
/// this code only writes `uuid` (and zeroes the rest), so a transposed
/// `stime`/`rtransid` would be undetectable; the moment anyone wires
/// up the timestamp slots or reads `rtransid`/`rtime` back, every IOC
/// would touch the wrong byte. Keep this layout byte-for-byte aligned
/// with the kernel header.
#[repr(C)]
struct BtrfsIoctlReceivedSubvolArgs {
    uuid: [u8; BTRFS_UUID_SIZE],
    stransid: u64,
    rtransid: u64,
    stime_sec: u64,
    stime_nsec: u32,
    _pad0: u32,
    rtime_sec: u64,
    rtime_nsec: u32,
    _pad1: u32,
    flags: u64,
    reserved: [u64; 16],
}

const _: () = assert!(std::mem::size_of::<BtrfsIoctlReceivedSubvolArgs>() == 200);

impl BtrfsIoctlReceivedSubvolArgs {
    fn zeroed() -> Self {
        Self {
            uuid: [0; BTRFS_UUID_SIZE],
            stransid: 0,
            rtransid: 0,
            stime_sec: 0,
            stime_nsec: 0,
            _pad0: 0,
            rtime_sec: 0,
            rtime_nsec: 0,
            _pad1: 0,
            flags: 0,
            reserved: [0; 16],
        }
    }
}

const BTRFS_UUID_SIZE: usize = 16;
const BTRFS_IOCTL_MAGIC: u8 = 0x94;
const BTRFS_IOC_SET_RECEIVED_SUBVOL_NR: u8 = 37;

// _IOWR(BTRFS_IOCTL_MAGIC, 37, struct btrfs_ioctl_received_subvol_args)
nix::ioctl_readwrite!(
    btrfs_ioc_set_received_subvol,
    BTRFS_IOCTL_MAGIC,
    BTRFS_IOC_SET_RECEIVED_SUBVOL_NR,
    BtrfsIoctlReceivedSubvolArgs
);

#[cfg(test)]
mod tests;

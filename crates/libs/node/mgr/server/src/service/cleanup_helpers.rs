use super::*;
use pplx_space_common_core::host::linux::paths;
use std::path::{Path, PathBuf};

pub(super) fn validate_sandbox_uid(
    sandbox_id: &str,
    requested_uid: u32,
    slot_uid: u32,
) -> Result<u32, Status> {
    if requested_uid < net::SANDBOX_UID_RANGE_START {
        return Err(Status::permission_denied(format!(
            "sandbox UID for {sandbox_id} must be >= {}",
            net::SANDBOX_UID_RANGE_START
        )));
    }
    if requested_uid != slot_uid {
        return Err(Status::permission_denied(format!(
            "sandbox UID {requested_uid} for {sandbox_id} does not match allocated slot UID {slot_uid}"
        )));
    }
    Ok(slot_uid)
}

pub(super) fn validate_sandbox_uid_gid(
    sandbox_id: &str,
    requested_uid: u32,
    requested_gid: u32,
    slot_uid: u32,
) -> Result<u32, Status> {
    validate_sandbox_uid(sandbox_id, requested_uid, slot_uid)?;
    if requested_gid < net::SANDBOX_UID_RANGE_START {
        return Err(Status::permission_denied(format!(
            "sandbox GID for {sandbox_id} must be >= {}",
            net::SANDBOX_UID_RANGE_START
        )));
    }
    if requested_gid != requested_uid {
        return Err(Status::permission_denied(format!(
            "sandbox GID {requested_gid} for {sandbox_id} must match UID {requested_uid}"
        )));
    }
    Ok(slot_uid)
}

pub(super) fn reserved_expose_port_reason(port: u16) -> Option<&'static str> {
    RESERVED_EXPOSE_PORTS
        .iter()
        .find_map(|(reserved, reason)| (*reserved == port).then_some(*reason))
}

/// Runs a blocking btrfs/filesystem section on the blocking pool and maps a
/// join failure to `Status::internal`. Same rationale as
/// [`spawn_lifecycle_blocking`] for paths that have no lifecycle
/// permit: `btrfs` subprocesses and commit-forcing ioctls can block for
/// seconds under filesystem commit contention, and inline calls park runtime
/// workers until the health probes time out.
impl NodeManagerService {
    /// Reject excess diagnostics before queueing blocking work. The closure,
    /// not the RPC waiter, owns admission so cancellation cannot bypass it.
    pub(super) async fn off_runtime_probe<T, F>(
        &self,
        operation: &'static str,
        section: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, Status> + Send + 'static,
    {
        let permit = self
            .blocking_probe_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("node diagnostic capacity exhausted; retry"))?;
        self.off_runtime(operation, move || {
            let _permit = permit;
            section()
        })
        .await
    }

    pub(super) async fn off_runtime<T, F>(
        &self,
        operation: &'static str,
        section: F,
    ) -> Result<T, Status>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, Status> + Send + 'static,
    {
        let owner = self.artifact_work.clone();
        let span = tracing::Span::current();
        self.device_op_tasks
            .spawn_blocking(move || {
                if let Some(owner) = &owner {
                    owner.check()?;
                }
                let result = span.in_scope(section);
                if let Some(owner) = &owner {
                    owner.check()?;
                }
                result
            })
            .await
            .map_err(|error| {
                Status::internal(format!("{operation}: blocking task failed to run: {error}"))
            })?
    }

    /// Best-effort blocking cleanup, tracked like [`Self::off_runtime`]. Join
    /// failures are logged rather than returned.
    pub(super) async fn off_runtime_best_effort(&self, section: impl FnOnce() + Send + 'static) {
        let owner = self.artifact_work.clone();
        let span = tracing::Span::current();
        if let Err(error) = self
            .device_op_tasks
            .spawn_blocking(move || {
                let _owner = owner;
                span.in_scope(section);
            })
            .await
        {
            tracing::warn!(%error, "best-effort blocking cleanup task failed");
        }
    }
}

// Run an external command on the blocking pool so the async lifecycle handlers
// don't park Tokio worker threads on fork/exec/waitpid. Under load,
// inline std::process::Command calls in launch_sandbox / prepare_sandbox /
// cleanup_sandbox were starving the runtime, causing fresh UDS connections and
// gRPC health probes to time out at 1s.
pub(super) fn spawn_lifecycle_blocking<F, R>(
    permit: &crate::lifecycle_limiter::LifecyclePermit,
    f: F,
) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let permit = permit.clone();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
}

pub(super) fn run_permission_command_blocking(
    command: &'static str,
    args: &[&str],
    path: &str,
    target: &str,
) -> Result<(), Status> {
    let output = crate::host_command::output(
        std::process::Command::new(command).args(args),
        crate::host_command::MUTATION_BUDGET,
        &[],
    )
    .map_err(|error| {
        tracing::error!(
            command,
            args = ?args,
            path,
            target,
            error = %error,
            "permission command failed"
        );
        Status::internal(format!(
            "{command} {} failed for {path}: {error}",
            args.join(" ")
        ))
    })?;

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        tracing::error!(
            command,
            args = ?args,
            path,
            target,
            status = %output.status,
            stdout = %stdout.trim(),
            stderr = %stderr.trim(),
            "permission command failed"
        );
        return Err(Status::internal(format!(
            "{command} {} failed for {path}: {}",
            args.join(" "),
            output.status
        )));
    }

    Ok(())
}

pub(super) fn chown_path_blocking(path: &str, uid: u32, gid: u32) -> Result<(), Status> {
    let file = super::trusted_paths::permission_inode(Path::new(path))?;
    nix::unistd::fchown(
        &file,
        Some(nix::unistd::Uid::from_raw(uid)),
        Some(nix::unistd::Gid::from_raw(gid)),
    )
    .map_err(|error| Status::internal(format!("chown pinned inode: {error}")))
}

pub(super) fn chgrp_path_blocking(path: &str, gid: u32) -> Result<(), Status> {
    let file = super::trusted_paths::permission_inode(Path::new(path))?;
    nix::unistd::fchown(&file, None, Some(nix::unistd::Gid::from_raw(gid)))
        .map_err(|error| Status::internal(format!("chgrp pinned inode: {error}")))
}

pub(super) fn chmod_path_blocking(path: &str, mode: u32) -> Result<(), Status> {
    let file = super::trusted_paths::permission_inode(Path::new(path))?;
    super::trusted_paths::set_permissions(&file, None, mode)
}

pub(super) async fn run_permission_command(
    command: &'static str,
    args: &[&str],
    path: &str,
    target: &str,
    permit: &crate::lifecycle_limiter::LifecyclePermit,
) -> Result<(), Status> {
    let args_owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
    let path_owned = path.to_string();
    let target_owned = target.to_string();
    spawn_lifecycle_blocking(permit, move || {
        let args: Vec<&str> = args_owned.iter().map(String::as_str).collect();
        run_permission_command_blocking(command, &args, &path_owned, &target_owned)
    })
    .await
    .map_err(|error| Status::internal(format!("permission command task failed: {error}")))?
}

pub(super) trait CleanupCommandRunner {
    fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError>;
    fn remove_tree(&mut self, path: &Path) -> Result<(), CleanupCommandError>;
    fn set_readonly(&mut self, path: &Path, readonly: bool) -> Result<(), CleanupCommandError>;

    fn rewrite_ownership(
        &mut self,
        root: &str,
        prune_snapshots: bool,
        uid: Option<u32>,
    ) -> Result<(), CleanupCommandError>;
}

pub(super) struct SystemCleanupCommandRunner;

#[derive(Debug, thiserror::Error)]
pub(crate) enum CleanupCommandError {
    #[error("{0}")]
    Message(String),
    #[error("failed to run {program}: {detail}")]
    Spawn { program: String, detail: String },
    #[error("failed to execute {program}: {source}")]
    Execution {
        program: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{program} {args} exited with {status}")]
    Failed {
        program: String,
        args: String,
        status: String,
    },
    #[error("{program} {args} exited with {status}: {detail}")]
    FailedWithDetail {
        program: String,
        args: String,
        status: String,
        detail: String,
    },
    #[error("{program} {args} exited with {status}: {detail}")]
    MissingPath {
        program: String,
        args: String,
        status: String,
        detail: String,
    },
    #[error("{program} {args} exited with {status}: {detail}")]
    NotSubvolume {
        program: String,
        args: String,
        status: String,
        detail: String,
    },
}

fn is_missing_path_command_error(error: &CleanupCommandError) -> bool {
    matches!(error, CleanupCommandError::MissingPath { .. })
        || matches!(error, CleanupCommandError::Execution { source, .. } if source.kind() == std::io::ErrorKind::NotFound)
}

fn is_not_subvolume_command_error(error: &CleanupCommandError) -> bool {
    matches!(error, CleanupCommandError::NotSubvolume { .. })
}

// A probe failure that means the host has no usable btrfs for this path — the
// `btrfs` binary is absent (`Spawn`), or the path is on a non-btrfs filesystem.
// On such a host there are no real subvolumes to toggle, so the snapshot entry
// is a plain directory and can be re-owned directly; treating it as "not a
// subvolume" preserves the old `Err(_) => continue` tolerance for non-btrfs /
// fallback hosts without swallowing a transient error on a genuine subvolume.
fn is_btrfs_unavailable_command_error(error: &CleanupCommandError) -> bool {
    match error {
        CleanupCommandError::Spawn { .. } => true,
        CleanupCommandError::FailedWithDetail { detail, .. } => {
            stderr_indicates_btrfs_unavailable(detail)
        }
        _ => false,
    }
}

// Mirrors `btrfs_unavailable_error` in snapshots.rs. The non-btrfs-filesystem
// `Inappropriate ioctl for device` case usually arrives as `NotSubvolume` first
// (btrfs prints `Could not get subvolume flags: Inappropriate ioctl for device`
// for `property get -ts`); this is the fallback for other `btrfs` invocations
// that report it without that prefix.
fn stderr_indicates_btrfs_unavailable(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("not a btrfs filesystem")
        || stderr.contains("not on a btrfs filesystem")
        || stderr.contains("inappropriate ioctl for device")
        || stderr.contains("operation not supported")
}

// A chmod/chown that hit a read-only btrfs subvolume (EROFS). The cache-wide
// sweep prunes the snapshots subtree so it can never see this, but the
// per-snapshot rewrite runs rooted at a snapshot with no prune — if a directory
// entry is read-only at rewrite time despite not being toggled writable (a
// concurrent flip, or a probe that misclassified it), the rewrite surfaces EROFS
// here so the per-snapshot path can recover (toggle writable, retry) instead of
// re-introducing the slot-quarantine failure this whole change prevents.
fn is_read_only_fs_command_error(error: &CleanupCommandError) -> bool {
    match error {
        CleanupCommandError::Execution { source, .. } => source.raw_os_error() == Some(libc::EROFS),
        CleanupCommandError::FailedWithDetail { detail, .. } => detail
            .to_ascii_lowercase()
            .contains("read-only file system"),
        _ => false,
    }
}

fn stderr_has_missing_path(stderr: &str) -> bool {
    stderr
        .lines()
        .map(str::trim)
        .any(|line| line.ends_with("No such file or directory"))
}

// `btrfs property get -ts <path> ro` forces the subvolume object type, so for a
// path that is not a btrfs subvolume root (a plain-dir "imported" snapshot) it
// skips autodetect and fails the `BTRFS_IOC_SUBVOL_GETFLAGS` ioctl, printing
// `ERROR: Could not get subvolume flags: <errno>` (the errno tail varies:
// `Invalid argument` for a plain dir on btrfs, `Inappropriate ioctl for device`
// on a non-btrfs filesystem). Older / different btrfs-progs spellings also say
// `not a subvolume` / `Not a Btrfs subvolume`, so match case-insensitively
// (the sibling `btrfs_unavailable_error` in snapshots.rs lower-cases too).
// getflags only reads in-memory flags, so this never stands in for a
// transient/IO failure — those surface from `open` or a different command and
// must propagate rather than be treated as "not a subvolume".
fn stderr_indicates_not_subvolume(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("could not get subvolume flags")
        || stderr.contains("not a btrfs subvolume")
        || stderr.contains("not a subvolume")
}

impl CleanupCommandRunner for SystemCleanupCommandRunner {
    fn set_readonly(&mut self, path: &Path, readonly: bool) -> Result<(), CleanupCommandError> {
        super::artifact_fs::set_readonly(path, readonly)
            .map_err(|e| CleanupCommandError::Message(e.to_string()))
    }
    fn remove_tree(&mut self, path: &Path) -> Result<(), CleanupCommandError> {
        super::artifact_fs::delete(path).map_err(|e| CleanupCommandError::Message(e.to_string()))
    }

    fn rewrite_ownership(
        &mut self,
        root: &str,
        prune_snapshots: bool,
        uid: Option<u32>,
    ) -> Result<(), CleanupCommandError> {
        super::permission_tree::rewrite(
            Path::new(root),
            prune_snapshots,
            uid,
            0,
            pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW,
        )
        .map_err(|source| CleanupCommandError::Execution {
            program: "cache ownership rewrite".into(),
            source,
        })
    }

    fn run(&mut self, program: &str, args: &[&str]) -> Result<String, CleanupCommandError> {
        let output = crate::host_command::output(
            std::process::Command::new(program).args(args),
            crate::host_command::MUTATION_BUDGET,
            &[],
        )
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                CleanupCommandError::Spawn {
                    program: program.to_string(),
                    detail: error.to_string(),
                }
            } else {
                CleanupCommandError::Execution {
                    program: program.to_string(),
                    source: error,
                }
            }
        })?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            let args_joined = args.join(" ");
            let status = output.status.to_string();
            let stderr = String::from_utf8_lossy(&output.stderr);
            let detail = stderr.trim();
            if detail.is_empty() {
                Err(CleanupCommandError::Failed {
                    program: program.to_string(),
                    args: args_joined,
                    status,
                })
            } else if stderr_indicates_not_subvolume(detail) {
                Err(CleanupCommandError::NotSubvolume {
                    program: program.to_string(),
                    args: args_joined,
                    status,
                    detail: detail.to_string(),
                })
            } else if stderr_has_missing_path(detail) {
                Err(CleanupCommandError::MissingPath {
                    program: program.to_string(),
                    args: args_joined,
                    status,
                    detail: detail.to_string(),
                })
            } else {
                Err(CleanupCommandError::FailedWithDetail {
                    program: program.to_string(),
                    args: args_joined,
                    status,
                    detail: detail.to_string(),
                })
            }
        }
    }
}

pub(super) fn parse_btrfs_read_only_property(output: &str) -> Option<bool> {
    output.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "ro").then(|| matches!(value.trim(), "true"))
    })
}

/// One entry under `{cache_dir}/snapshots`. `read_only` is true only for a
/// btrfs subvolume whose `ro` property is set — those need a temporary
/// writable toggle before their ownership can be rewritten. Plain-dir
/// "imported" snapshots and stray files are returned with `read_only = false`
/// (their ownership is still rewritten, but no btrfs toggle is attempted).
#[derive(Debug, Clone)]
pub(super) struct SnapshotEntry {
    path: PathBuf,
    read_only: bool,
}

/// Enumerate every entry under `{cache_dir}/snapshots`, classifying each as a
/// read-only subvolume or not.
///
/// Unlike the previous `Err(_) => continue`, this distinguishes "not a btrfs
/// subvolume" (a plain-dir import or stray file — safe to skip the toggle) from
/// any other `btrfs` failure (transient probe error on a genuine subvolume),
/// which is propagated. Swallowing the latter is what let an undetected
/// read-only subvolume survive into the recursive ownership rewrite and fail it
/// with EROFS, looping the slot reset to quarantine.
fn enumerate_snapshot_entries(
    cache_dir: &Path,
    runner: &mut impl CleanupCommandRunner,
) -> Result<Vec<SnapshotEntry>, String> {
    let snapshots_dir = cache_dir.join("snapshots");
    // Refuse to enumerate through a symlinked `snapshots` directory. The sandbox
    // owns the cache dir and could replace `snapshots` with a link to a host
    // path; `read_dir` would follow it and the per-snapshot `find`s would then
    // resolve that link and chmod/chown files outside the cache. The outer sweep
    // still re-owns the link itself (chown -h skips the target), so it is safe to
    // treat a symlinked snapshots dir as having no entries to neutralize.
    match std::fs::symlink_metadata(&snapshots_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            tracing::warn!(
                snapshots_dir = %snapshots_dir.display(),
                "refusing to enumerate symlinked snapshots directory during retained-cache cleanup"
            );
            return Ok(Vec::new());
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "failed to stat snapshots dir {}: {error}",
                snapshots_dir.display()
            ));
        }
    }
    let entries = match std::fs::read_dir(&snapshots_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(format!(
                "failed to read snapshots dir {}: {error}",
                snapshots_dir.display()
            ));
        }
    };

    let mut snapshots = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            format!(
                "failed to read entry in {}: {error}",
                snapshots_dir.display()
            )
        })?;
        let snapshot_path = entry.path();
        // Only a directory can be a btrfs subvolume root, so only directories
        // are probed. Symlinks, regular files, and special files (FIFOs,
        // sockets, devices) are never passed to `btrfs property -ts`: the CLI
        // open(2)s its operand, which would resolve a symlink onto a subvolume
        // outside the cache or block forever on a FIFO. Non-directories are
        // re-owned directly by the per-snapshot ownership pass (chown -h handles
        // symlinks; the chmod pass skips symlinks via `! -type l`).
        // `DirEntry::file_type` does not follow symlinks.
        let is_dir = entry
            .file_type()
            .map_err(|error| format!("failed to stat {}: {error}", snapshot_path.display()))?
            .is_dir();
        let read_only = if is_dir {
            let snapshot_path_string = snapshot_path.to_string_lossy().into_owned();
            match runner.run(
                "btrfs",
                &["property", "get", "-ts", &snapshot_path_string, "ro"],
            ) {
                Ok(output) => parse_btrfs_read_only_property(&output).unwrap_or(false),
                Err(error) if is_not_subvolume_command_error(&error) => false,
                // Non-btrfs / no-btrfs-progs host: the entry is a plain dir, not
                // a read-only subvolume, so re-own it directly without a toggle.
                Err(error) if is_btrfs_unavailable_command_error(&error) => false,
                // The snapshot raced away between `read_dir` and this probe —
                // there is nothing left to neutralize, so skip it rather than
                // fail the whole sweep (a deletion-during-cleanup is benign, and
                // the prior `Err(_) => continue` tolerated it).
                Err(error) if is_missing_path_command_error(&error) => continue,
                Err(error) => {
                    return Err(format!(
                        "failed to probe snapshot {}: {error}",
                        snapshot_path.display()
                    ));
                }
            }
        } else {
            false
        };
        snapshots.push(SnapshotEntry {
            path: snapshot_path,
            read_only,
        });
    }
    snapshots.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(snapshots)
}

fn make_snapshots_writable(
    runner: &mut impl CleanupCommandRunner,
    snapshots: &[SnapshotEntry],
) -> Vec<String> {
    let mut errors = Vec::new();
    for snapshot in snapshots {
        if snapshot.read_only
            && let Err(error) = set_btrfs_snapshot_read_only(runner, &snapshot.path, false)
        {
            errors.push(error);
        }
    }
    errors
}

fn restore_snapshots_read_only(
    runner: &mut impl CleanupCommandRunner,
    snapshots: &[SnapshotEntry],
) -> Vec<String> {
    let mut errors = Vec::new();
    for snapshot in snapshots.iter().rev() {
        if snapshot.read_only
            && let Err(error) = set_btrfs_snapshot_read_only(runner, &snapshot.path, true)
        {
            // The snapshot is left writable — a published d-stop artifact has lost
            // its immutability. Surface it so it's observable even if the error is
            // later folded into a broader cleanup failure.
            tracing::warn!(
                snapshot = %snapshot.path.display(),
                %error,
                "failed to restore read-only flag on snapshot during retained-cache cleanup; \
                 published artifact may be left mutable"
            );
            errors.push(error);
        }
    }
    errors
}

/// Run one snapshot's ownership rewrite rooted at the snapshot itself (never
/// pruned). `uid = Some` applies the uid-scoped passes; `None` the whole-tree
/// passes.
fn run_per_snapshot_rewrite(
    runner: &mut impl CleanupCommandRunner,
    root: &str,
    uid: Option<u32>,
) -> Result<(), CleanupCommandError> {
    runner.rewrite_ownership(root, false, uid)
}

/// Re-own one snapshot's contents, tolerating a read-only subvolume the up-front
/// `make_snapshots_writable` toggle missed (a probe misclassification, or a
/// snapshot flipped read-only after enumeration). The cache-wide sweep is
/// EROFS-proof via its prune; this gives the per-snapshot pass the same
/// guarantee: on EROFS, force the snapshot writable, retry the rewrite, then
/// restore read-only — so a stray read-only snapshot can never fail the reset
/// into quarantine.
fn reown_snapshot_contents(
    runner: &mut impl CleanupCommandRunner,
    snapshot: &SnapshotEntry,
    uid: Option<u32>,
) -> Result<(), CleanupCommandError> {
    let root = snapshot.path.to_string_lossy().into_owned();
    match run_per_snapshot_rewrite(runner, &root, uid) {
        Ok(()) => Ok(()),
        Err(error) if is_read_only_fs_command_error(&error) => {
            set_btrfs_snapshot_read_only(runner, &snapshot.path, false)
                .map_err(CleanupCommandError::Message)?;
            let rewrite = run_per_snapshot_rewrite(runner, &root, uid);
            let restore = set_btrfs_snapshot_read_only(runner, &snapshot.path, true);
            if let Err(restore_error) = restore {
                tracing::warn!(
                    snapshot = %snapshot.path.display(),
                    error = %restore_error,
                    "failed to restore read-only flag on snapshot during retained-cache cleanup; \
                     published artifact may be left mutable"
                );
                if let Err(rewrite_error) = rewrite {
                    return Err(CleanupCommandError::Message(format!(
                        "{rewrite_error}; {restore_error}"
                    )));
                }
                return Err(CleanupCommandError::Message(restore_error));
            }
            rewrite?;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub(super) fn set_btrfs_snapshot_read_only(
    runner: &mut impl CleanupCommandRunner,
    path: &Path,
    read_only: bool,
) -> Result<(), String> {
    runner
        .set_readonly(path, read_only)
        .map_err(|error| error.to_string())
}

pub(super) fn unmount_if_present(path: impl AsRef<Path>) -> Result<bool, String> {
    let path: &Path = path.as_ref();
    if !path
        .try_exists()
        .map_err(|error| format!("inspect mount {}: {error}", path.display()))?
    {
        return Ok(false);
    }
    unmount_existing_path(path)
}

#[cfg(target_os = "linux")]
pub(super) fn unmount_existing_path(path: impl AsRef<Path>) -> Result<bool, String> {
    let path: &Path = path.as_ref();
    // Callers name binds inside a sandbox-writable jail. Never follow a final
    // link planted there into a host mount.
    match nix::mount::umount2(
        path,
        nix::mount::MntFlags::MNT_DETACH | nix::mount::MntFlags::UMOUNT_NOFOLLOW,
    ) {
        Ok(()) => Ok(true),
        Err(nix::errno::Errno::EINVAL | nix::errno::Errno::ENOENT) => Ok(false),
        Err(error) => Err(format!("failed to unmount {}: {error}", path.display())),
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn unmount_existing_path(_path: impl AsRef<Path>) -> Result<bool, String> {
    Ok(false)
}

pub(super) fn remove_dir_all_if_exists(path: &str) -> Result<bool, String> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!("failed to remove {path}: {error}")),
    }
}

/// Force-delete the throwaway read-only `instance.ro` snapshots that can sit
/// directly under the cache dir (a restore-receive landing, pre-rename to
/// `instance/`) or under `snapshot/` (the suspend codec's `btrfs send -p`
/// source). Both are disposable read-only subvolumes; left in place, the
/// retained-cache ownership rewrite descends into them and its recursive
/// `chmod`/`chown` fails with EROFS, looping the slot reset into quarantine.
///
/// Never touches `instance/` — that is the live writable instance the
/// retained-cache reset preserves. The `snapshots/` (plural) published d-stop
/// artifacts are handled separately by the enumerate + prune + per-snapshot
/// toggle in the neutralize functions; this covers only the two singular
/// `instance.ro` locations that prune never sees.
fn remove_instance_ro_snapshots(
    cache_dir: &Path,
    runner: &mut impl CleanupCommandRunner,
) -> Result<(), String> {
    for instance_ro in [
        cache_dir.join("instance.ro"),
        cache_dir.join("snapshot").join("instance.ro"),
    ] {
        runner
            .remove_tree(&instance_ro)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

pub(super) fn neutralize_retained_cache(sandbox_id: &str) -> Result<bool, String> {
    let cache_dir = format!("{CACHE_DIR}/{sandbox_id}");
    neutralize_retained_cache_dir(Path::new(&cache_dir))
}

pub(crate) fn neutralize_retained_cache_owned_by_uid(
    sandbox_id: &str,
    uid: u32,
) -> Result<bool, RetainedCacheCleanupError> {
    let cache_dir = format!("{CACHE_DIR}/{sandbox_id}");
    neutralize_retained_cache_dir_owned_by_uid(Path::new(&cache_dir), uid)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RetainedCacheCleanupError {
    #[error(transparent)]
    Command(#[from] CleanupCommandError),
    #[error("retained cache ownership cleanup raced with concurrent delete: {detail}")]
    ConcurrentDelete { detail: String },
}

pub(super) fn neutralize_retained_cache_dir(cache_dir: &Path) -> Result<bool, String> {
    if !cache_dir.exists() {
        return Ok(false);
    }
    neutralize_retained_cache_dir_with_runner(cache_dir, &mut SystemCleanupCommandRunner)
}

pub(super) fn neutralize_retained_cache_dir_owned_by_uid(
    cache_dir: &Path,
    uid: u32,
) -> Result<bool, RetainedCacheCleanupError> {
    if !cache_dir.exists() {
        return Ok(false);
    }
    neutralize_retained_cache_dir_owned_by_uid_with_runner(
        cache_dir,
        uid,
        &mut SystemCleanupCommandRunner,
    )
}

pub(super) fn neutralize_retained_cache_dir_with_runner(
    cache_dir: &Path,
    runner: &mut impl CleanupCommandRunner,
) -> Result<bool, String> {
    let cache_dir_path = cache_dir.to_path_buf();
    // Drop the throwaway read-only `instance.ro` snapshots before the ownership
    // rewrite so the recursive chmod/chown can't hit EROFS descending into one.
    remove_instance_ro_snapshots(cache_dir, runner)?;
    let snapshots = enumerate_snapshot_entries(cache_dir, runner)?;
    let make_writable_errors = make_snapshots_writable(runner, &snapshots);
    if !make_writable_errors.is_empty() {
        // Restore any snapshot we already toggled writable so a partial toggle
        // doesn't leave a published d-stop snapshot permanently mutable.
        let mut errors = make_writable_errors;
        errors.extend(restore_snapshots_read_only(runner, &snapshots));
        return Err(errors.join("; "));
    }

    let cache_dir = cache_dir.to_string_lossy().into_owned();
    let neutralize_result: Result<(), CleanupCommandError> = (|| {
        // Re-own the whole cache tree, but prune the snapshot subvolumes so the
        // walker never descends into a read-only one (which would fail with
        // EROFS). The snapshots directory itself is still re-owned here.
        runner.rewrite_ownership(&cache_dir, true, None)?;
        // Re-own each snapshot's contents (rooted at it, EROFS-resilient).
        for snapshot in &snapshots {
            reown_snapshot_contents(runner, snapshot, None)?;
        }
        Ok(())
    })();

    let mut restore_errors = restore_snapshots_read_only(runner, &snapshots);

    if let Err(error) = neutralize_result {
        if !cache_dir_path.exists() {
            return Ok(false);
        }
        if is_missing_path_command_error(&error) {
            if restore_errors.is_empty() {
                return Err(format!(
                    "retained cache ownership cleanup raced with concurrent delete: {error}"
                ));
            }
            restore_errors.insert(
                0,
                format!("retained cache ownership cleanup raced with concurrent delete: {error}"),
            );
            return Err(restore_errors.join("; "));
        }
        if restore_errors.is_empty() {
            return Err(error.to_string());
        }
        restore_errors.insert(0, error.to_string());
        return Err(restore_errors.join("; "));
    }
    if !restore_errors.is_empty() {
        if !cache_dir_path.exists() {
            return Ok(false);
        }
        return Err(restore_errors.join("; "));
    }
    Ok(true)
}

pub(super) fn neutralize_retained_cache_dir_owned_by_uid_with_runner(
    cache_dir: &Path,
    uid: u32,
    runner: &mut impl CleanupCommandRunner,
) -> Result<bool, RetainedCacheCleanupError> {
    let cache_dir_path = cache_dir.to_path_buf();
    // Drop the throwaway read-only `instance.ro` snapshots before the ownership
    // rewrite so the recursive chmod/chown can't hit EROFS descending into one.
    remove_instance_ro_snapshots(cache_dir, runner).map_err(CleanupCommandError::Message)?;
    let snapshots =
        enumerate_snapshot_entries(cache_dir, runner).map_err(CleanupCommandError::Message)?;
    let make_writable_errors = make_snapshots_writable(runner, &snapshots);
    if !make_writable_errors.is_empty() {
        // Restore any snapshot we already toggled writable so a partial toggle
        // doesn't leave a published d-stop snapshot permanently mutable.
        let mut errors = make_writable_errors;
        errors.extend(restore_snapshots_read_only(runner, &snapshots));
        return Err(RetainedCacheCleanupError::Command(
            CleanupCommandError::Message(errors.join("; ")),
        ));
    }

    let cache_dir = cache_dir.to_string_lossy().into_owned();
    let neutralize_result: Result<(), CleanupCommandError> = (|| {
        // Prune the snapshot subvolumes from the cache-wide sweep so the
        // recursive chmod/chown can never descend into a read-only one (EROFS) —
        // including one a concurrent `CreateBtrfsSnapshot` raced in after the
        // enumeration above. The snapshots directory itself is still re-owned.
        // Each snapshot's contents are re-owned
        // below (rooted at it, EROFS-resilient) while it is temporarily writable.
        runner.rewrite_ownership(&cache_dir, true, Some(uid))?;
        for snapshot in &snapshots {
            reown_snapshot_contents(runner, snapshot, Some(uid))?;
        }
        Ok(())
    })();

    let mut restore_errors = restore_snapshots_read_only(runner, &snapshots);

    if let Err(error) = neutralize_result {
        if !cache_dir_path.exists() {
            return Ok(false);
        }
        if is_missing_path_command_error(&error) {
            let detail = if restore_errors.is_empty() {
                error.to_string()
            } else {
                let mut all_errors = vec![error.to_string()];
                all_errors.extend(restore_errors);
                all_errors.join("; ")
            };
            return Err(RetainedCacheCleanupError::ConcurrentDelete { detail });
        }
        if restore_errors.is_empty() {
            return Err(RetainedCacheCleanupError::Command(error));
        }
        restore_errors.insert(0, error.to_string());
        return Err(RetainedCacheCleanupError::Command(
            CleanupCommandError::Message(restore_errors.join("; ")),
        ));
    }
    if !restore_errors.is_empty() {
        if !cache_dir_path.exists() {
            return Ok(false);
        }
        return Err(RetainedCacheCleanupError::Command(
            CleanupCommandError::Message(restore_errors.join("; ")),
        ));
    }
    Ok(true)
}

pub(super) async fn neutralize_retained_cache_blocking(
    sandbox_id: &str,
    permit: &crate::lifecycle_limiter::LifecyclePermit,
) -> Result<bool, String> {
    let sandbox_id = sandbox_id.to_string();
    spawn_lifecycle_blocking(permit, move || neutralize_retained_cache(&sandbox_id))
        .await
        .map_err(|error| format!("retained cache ownership cleanup task failed: {error}"))?
}

pub(super) use crate::cache_ownership::first_path_owned_by_uid;

pub(super) fn verify_retained_cache_not_owned_by_uid(
    sandbox_id: &str,
    uid: u32,
) -> Result<(), String> {
    let cache_dir = PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}"));
    if let Some(path) = first_path_owned_by_uid(&cache_dir, uid)? {
        return Err(format!(
            "retained cache artifact is still owned by sandbox uid {uid}: {}",
            path.display()
        ));
    }
    Ok(())
}

pub(super) async fn verify_retained_cache_not_owned_by_uid_blocking(
    sandbox_id: &str,
    uid: u32,
    permit: &crate::lifecycle_limiter::LifecyclePermit,
) -> Result<(), String> {
    let sandbox_id = sandbox_id.to_string();
    spawn_lifecycle_blocking(permit, move || {
        verify_retained_cache_not_owned_by_uid(&sandbox_id, uid)
    })
    .await
    .map_err(|error| format!("retained cache ownership verification task failed: {error}"))?
}

pub(super) fn verify_path_absent(path: &str) -> Result<(), String> {
    if Path::new(path).exists() {
        Err(format!("cleanup left path behind: {path}"))
    } else {
        Ok(())
    }
}

pub(super) fn cleanup_action(
    result: Result<bool, String>,
    success_action: impl FnOnce() -> String,
    errors: &mut Vec<String>,
    actions: &mut Vec<String>,
) {
    match result {
        Ok(true) => actions.push(success_action()),
        Ok(false) => {}
        Err(error) => errors.push(error),
    }
}

pub(super) fn tracked_sandbox_pid_alive(sandbox_id: &str, pid: u32) -> bool {
    launch::is_pid_alive(pid) && launch::pid_belongs_to_sandbox(pid, sandbox_id)
}

pub(super) fn live_tracked_processes(
    sandbox_id: &str,
    pids: &[(&'static str, Option<u32>)],
) -> Vec<String> {
    pids.iter()
        .filter_map(|(name, pid)| {
            let pid = (*pid)?;
            tracked_sandbox_pid_alive(sandbox_id, pid)
                .then(|| format!("{name} PID {pid} is still alive"))
        })
        .collect()
}

pub(super) fn live_cgroup_processes(sandbox_id: &str) -> Vec<String> {
    let mut occupied: Vec<String> = crate::cgroup::sandbox_cgroup_processes(sandbox_id)
        .into_iter()
        .filter(|(_, pid)| launch::is_pid_alive(*pid))
        .map(|(path, pid)| format!("cgroup {} still contains live PID {pid}", path.display()))
        .collect();
    // `cgroup.procs` lists only thread-group leaders. A non-leader thread — e.g.
    // a Firecracker vCPU thread still finishing VM/memory teardown after its
    // leader has exited — keeps the cgroup populated without appearing above.
    // The kernel `populated` flag is the authoritative occupancy signal, so a
    // quiescence wait must honor it; otherwise it declares the sandbox drained
    // and the subsequent `rmdir` fails with EBUSY.
    for path in crate::cgroup::sandbox_cgroups_populated(sandbox_id) {
        let path = path.display().to_string();
        if !occupied.iter().any(|entry| entry.contains(&path)) {
            occupied.push(format!(
                "cgroup {path} still populated (non-leader task present)"
            ));
        }
    }
    occupied
}

pub(super) async fn stop_sandbox_runtime_with_supervisor(
    service: &NodeManagerService,
    sandbox_id: &str,
    immediate_kill: bool,
    unbound_cleanup: Option<pplx_space_node_mgr_client::execution_fence::ExecutionFence>,
) -> Result<Vec<String>, Status> {
    // Preserve every failure for the lifecycle owner, not just backpressure.
    // Do not replay here: transport uncertainty may follow an executed mutation.
    let bound = service
        .pool
        .execution_fence(sandbox_id)
        .map_err(slot_store_status)?;
    // The durable binding wins. Only cleanup validated under the cgroup gate
    // may supply request authority when allocation never published a binding.
    // The supervisor still requires quiescence if its own binding is absent.
    let fence = bound
        .or(unbound_cleanup)
        .ok_or_else(|| Status::failed_precondition("runtime stop requires execution identity"))?;
    let response = service
        .runtime_supervisor
        .stop_sandbox_runtime(
            pplx_space_node_runtime_core::proto::StopSandboxRuntimeRequest {
                previous_execution_id: fence.previous().map(|id| id.to_string()),
                execution_id: fence.current().to_string(),
                sandbox_id: sandbox_id.to_string(),
                immediate_kill,
            },
        )
        .await?;
    Ok(response.into_inner().actions)
}

pub(super) async fn wait_for_cleanup_quiescence(
    sandbox_id: &str,
    pids: &[(&'static str, Option<u32>)],
    timeout: Duration,
) -> Vec<String> {
    wait_for_cleanup_quiescence_with(
        timeout,
        || live_tracked_processes(sandbox_id, pids),
        || live_cgroup_processes(sandbox_id),
    )
    .await
}

async fn wait_for_cleanup_quiescence_with<Tracked, Cgroups>(
    timeout: Duration,
    mut tracked_processes: Tracked,
    mut cgroup_processes: Cgroups,
) -> Vec<String>
where
    Tracked: FnMut() -> Vec<String>,
    Cgroups: FnMut() -> Vec<String>,
{
    let started = tokio::time::Instant::now();
    loop {
        let mut live = tracked_processes();
        live.extend(cgroup_processes());
        if let Some(live) = quiescence_wait_result(live, started.elapsed(), timeout) {
            return live;
        }
        tokio::time::sleep(CLEANUP_QUIESCENCE_POLL).await;
    }
}

fn quiescence_wait_result(
    live: Vec<String>,
    elapsed: Duration,
    timeout: Duration,
) -> Option<Vec<String>> {
    (live.is_empty() || elapsed >= timeout).then_some(live)
}

// Async wrapper around `cleanup_runtime_artifacts` so cleanup_sandbox doesn't
// run umount2 / remove_dir_all on a Tokio worker thread.
pub(super) async fn cleanup_runtime_artifacts_blocking(
    sandbox_id: &str,
    permit: &crate::lifecycle_limiter::LifecyclePermit,
) -> Result<Vec<String>, Status> {
    let sandbox_id = sandbox_id.to_string();
    spawn_lifecycle_blocking(permit, move || cleanup_runtime_artifacts(&sandbox_id))
        .await
        .map_err(|error| {
            Status::internal(format!("runtime artifact cleanup task failed: {error}"))
        })?
        .map_err(|error| Status::internal(error.to_string()))
}

#[derive(Debug, thiserror::Error)]
#[error("runtime artifacts retained for cleanup retry: {}", .0.join("; "))]
struct RuntimeArtifactCleanupError(Vec<String>);

fn cleanup_runtime_artifacts(sandbox_id: &str) -> Result<Vec<String>, RuntimeArtifactCleanupError> {
    cleanup_runtime_artifacts_at(
        &paths::isolated_net_gateway_run_dir(sandbox_id),
        &format!("{RUN_DIR}/{sandbox_id}"),
        &format!("{JAIL_DIR}/firecracker/{sandbox_id}"),
        &PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}/instance")),
        || teardown_encrypted_volumes_blocking(sandbox_id),
    )
}

fn cleanup_runtime_artifacts_at(
    gateway_dir: &str,
    run_dir: &str,
    jail_dir: &str,
    instance: &Path,
    teardown: impl FnOnce() -> (Vec<String>, Vec<String>),
) -> Result<Vec<String>, RuntimeArtifactCleanupError> {
    let mut actions = Vec::new();
    // Each phase is a prerequisite for the next. In particular, never unlink
    // device backing files (or the evidence that selects device teardown) after
    // unknown/failed teardown, and never remove a tree containing a live bind.
    for name in [
        "rootfs.ext4",
        "memory",
        super::snapshots::ENC_SNAPSHOT_JAIL_SUBDIR,
    ] {
        let bind = format!("{jail_dir}/root/{name}");
        if unmount_if_present(&bind).map_err(|error| RuntimeArtifactCleanupError(vec![error]))? {
            actions.push(format!("Unmounted {bind}"));
        }
    }
    cleanup_device_backings_with(instance, teardown)
        .map(|device_actions| actions.extend(device_actions))?;
    for path in [gateway_dir, run_dir, jail_dir] {
        if remove_dir_all_if_exists(path)
            .map_err(|error| RuntimeArtifactCleanupError(vec![error]))?
        {
            actions.push(format!("Removed {path}"));
        }
    }
    Ok(actions)
}

fn cleanup_device_backings_with(
    instance: &Path,
    teardown: impl FnOnce() -> (Vec<String>, Vec<String>),
) -> Result<Vec<String>, RuntimeArtifactCleanupError> {
    if !device_teardown_needed(instance).map_err(|error| {
        RuntimeArtifactCleanupError(vec![format!("inspect {}: {error}", instance.display())])
    })? {
        return Ok(Vec::new());
    }
    let (errors, actions) = teardown();
    if errors.is_empty() {
        Ok(actions)
    } else {
        Err(RuntimeArtifactCleanupError(errors))
    }
}

/// Resolve the device node, then use the volume owner's mountinfo parser.
/// Keep strict stat errors and deepest-first ordering for destructive cleanup.
fn mountpoints_of_device(device: &str) -> std::io::Result<Vec<String>> {
    let rdev = std::fs::metadata(device)?.rdev();
    #[cfg(target_os = "linux")]
    let numbers = (libc::major(rdev), libc::minor(rdev));
    #[cfg(not(target_os = "linux"))]
    let numbers = (
        libc::major(rdev as libc::dev_t).cast_unsigned(),
        libc::minor(rdev as libc::dev_t).cast_unsigned(),
    );
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
    let mut points =
        crate::encrypted_volume::mountpoints_of_device(&mountinfo, device, Some(numbers));
    points.sort_by_key(|path| std::cmp::Reverse(path.len()));
    Ok(points)
}

fn record_device_release(
    device: crate::rootfs_dmcrypt::ReleaseDevice<'_>,
    action: String,
    errors: &mut Vec<String>,
    actions: &mut Vec<String>,
    output: impl FnOnce(
        &mut std::process::Command,
        Duration,
        &[u8],
    ) -> std::io::Result<std::process::Output>,
) {
    match device.blocking_with(output) {
        Ok(()) => actions.push(action),
        Err(error) => errors.push(format!("{action}: {error}")),
    }
}

/// Best-effort teardown of any BYOK dm-crypt / dm-snapshot device stack for a
/// sandbox. vol-mgr names these `sbxroot-<id>` (the dm-snapshot composite),
/// `sbxroot-<id>-cow` (its dm-crypt COW), and `sbxvol-<id>-<name>` (encrypted
/// data volumes such as the snapshot capture). Enumerate dm devices carrying
/// the sandbox id, remove snapshot composites before the crypt devices they
/// sit on, then detach the loop devices backing the sandbox's cache files.
/// Reconcile mapper nodes even when a prior attempt already removed the kernel
/// devices. Returns (errors, actions).
fn teardown_encrypted_volumes_blocking(sandbox_id: &str) -> (Vec<String>, Vec<String>) {
    teardown_encrypted_volumes_with(
        sandbox_id,
        crate::host_command::output,
        mountpoints_of_device,
    )
}

fn teardown_encrypted_volumes_with(
    sandbox_id: &str,
    mut output: impl FnMut(
        &mut std::process::Command,
        Duration,
        &[u8],
    ) -> std::io::Result<std::process::Output>,
    mut mountpoints: impl FnMut(&str) -> std::io::Result<Vec<String>>,
) -> (Vec<String>, Vec<String>) {
    use crate::host_command::{MUTATION_BUDGET, PROBE_BUDGET};
    use crate::rootfs_dmcrypt::checked_output_with;
    use std::process::Command;
    let mut errors = Vec::new();
    let mut actions = Vec::new();

    let ls = match checked_output_with(
        Command::new("dmsetup").arg("ls"),
        &[],
        PROBE_BUDGET,
        &mut output,
    ) {
        Ok(o) => o,
        Err(error) => {
            errors.push(format!("dmsetup ls: {error}"));
            return (errors, actions);
        }
    };
    let listing = String::from_utf8_lossy(&ls);
    let names: Vec<String> = listing
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .filter(|name| mapper_belongs_to_sandbox(name, sandbox_id))
        .map(str::to_string)
        .collect();

    // Partition snapshot composites vs crypt devices by `dmsetup status`
    // (target type is the 3rd field). Composites reference the crypt COW as a
    // target, so they must be removed first.
    let mut snapshots = Vec::new();
    let mut crypts = Vec::new();
    for name in names {
        let target = match checked_output_with(
            Command::new("dmsetup").args(["status", &name]),
            &[],
            PROBE_BUDGET,
            &mut output,
        ) {
            Ok(o) => String::from_utf8_lossy(&o)
                .split_whitespace()
                .nth(2)
                .unwrap_or("")
                .to_string(),
            Err(error) => {
                errors.push(format!("dmsetup status {name}: {error}"));
                continue;
            }
        };
        if target == "snapshot" {
            snapshots.push(name);
        } else {
            crypts.push(name);
        }
    }
    for name in snapshots {
        record_device_release(
            crate::rootfs_dmcrypt::ReleaseDevice::Snapshot(&name),
            format!("dmsetup remove {name}"),
            &mut errors,
            &mut actions,
            &mut output,
        );
    }
    for name in crypts {
        // An encrypted data volume (sbxvol-<id>-<name>) carries a btrfs mount on
        // top of the crypt device — and a restored sandbox mounts it twice (its
        // own dir plus a bind into the jail). Every mountpoint must be unmounted
        // before the device can be closed, so enumerate them all rather than
        // unmounting once. Harmless for crypts with no filesystem (e.g. the
        // rootfs COW sbxroot-<id>-cow): the mountpoint list is simply empty.
        let dev = format!("/dev/mapper/{name}");
        let mounts = match mountpoints(&dev) {
            Ok(mounts) => mounts,
            Err(error) => {
                errors.push(format!("enumerate mounts of {dev}: {error}"));
                // A missing userspace node does not prove kernel absence.
                // Still attempt a non-forced close by name; mounted/busy
                // devices refuse it. Retain the probe error so backing files
                // and the lease survive until a later pass confirms cleanup.
                Vec::new()
            }
        };
        for target in mounts {
            record_device_release(
                crate::rootfs_dmcrypt::ReleaseDevice::Unmount(&target),
                format!("umount {target}"),
                &mut errors,
                &mut actions,
                &mut output,
            );
        }
        record_device_release(
            crate::rootfs_dmcrypt::ReleaseDevice::Crypt(&name),
            format!("cryptsetup close {name}"),
            &mut errors,
            &mut actions,
            &mut output,
        );
    }

    // On a host with no udev daemon (containers, minimal nodes), `dmsetup
    // remove` tears down the kernel device but leaves a dangling
    // `/dev/mapper/<name>` node behind — the mirror of the `mknodes` we run
    // after `dmsetup create`. `dmsetup mknodes` (no args) reconciles the
    // whole `/dev/mapper` tree against the live kernel table, pruning nodes
    // for devices that no longer exist. Kernel absence does not discharge this
    // obligation: a failed reconciliation or process exit can leave only nodes
    // behind. Retry it independently of this attempt's removal inventory.
    match checked_output_with(
        Command::new("dmsetup").arg("mknodes"),
        &[],
        MUTATION_BUDGET,
        &mut output,
    ) {
        Ok(_) => actions.push("dmsetup mknodes".to_string()),
        Err(e) => errors.push(format!("dmsetup mknodes: {e}")),
    }

    // Detach loop devices backing this sandbox's cache files (the dm-snapshot
    // origin loop). `losetup -a` lines: `/dev/loopN: [..]:.. (/path)`.
    // The installation root may be a symlink; accept its resolved spelling,
    // not an arbitrary backing path that happens to contain the sandbox UUID.
    let cache_roots = match sandbox_backing_roots(Path::new(CACHE_DIR), sandbox_id) {
        Ok(roots) => roots,
        Err(error) => {
            errors.push(format!("resolve sandbox loop backing root: {error}"));
            return (errors, actions);
        }
    };
    let loops = checked_output_with(
        Command::new("losetup").arg("-a"),
        &[],
        PROBE_BUDGET,
        &mut output,
    );
    if let Ok(o) = &loops {
        let text = String::from_utf8_lossy(o);
        for line in text.lines() {
            if line
                .split_once(" (")
                .and_then(|(_, path)| path.strip_suffix(')'))
                .is_some_and(|path| backing_belongs_to_sandbox(path, &cache_roots))
                && let Some(dev) = line.split(':').next()
            {
                record_device_release(
                    crate::rootfs_dmcrypt::ReleaseDevice::Loop(dev),
                    format!("losetup -d {dev}"),
                    &mut errors,
                    &mut actions,
                    &mut output,
                );
            }
        }
    } else if let Err(error) = loops {
        errors.push(format!("losetup -a: {error}"));
    }

    (errors, actions)
}

/// Backing files, kernel devices and stale mapper nodes all retain teardown
/// obligations. Unreadable inventory is never absence.
fn device_teardown_needed(instance_dir: &Path) -> std::io::Result<bool> {
    let entries = match std::fs::read_dir(instance_dir) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    for entry in entries.into_iter().flatten() {
        let name = entry?.file_name();
        if name.to_str().is_some_and(|name| {
            name == crate::rootfs_dmcrypt::POOL_COW_FILE || name.ends_with(".luks")
        }) {
            return Ok(true);
        }
    }
    // Origin loops precede COW creation. Missing markers cannot prove that a
    // crashed prepare acquired no kernel resources, even if instance/ is gone.
    // Stale mapper nodes also outlive kernel removal and must trigger retry.
    #[cfg(target_os = "linux")]
    {
        let id = instance_dir
            .parent()
            .and_then(Path::file_name)
            .and_then(|id| id.to_str())
            .ok_or_else(|| std::io::Error::other("missing sandbox identity"))?;
        host_device_stack_exists(
            Path::new("/sys/block"),
            Path::new("/dev/mapper"),
            Path::new(CACHE_DIR),
            id,
        )
    }
    #[cfg(not(target_os = "linux"))]
    Ok(false)
}

#[cfg(any(target_os = "linux", test))]
fn host_device_stack_exists(
    sys_block: &Path,
    mapper_dir: &Path,
    cache_root: &Path,
    sandbox_id: &str,
) -> std::io::Result<bool> {
    let cache_roots = sandbox_backing_roots(cache_root, sandbox_id)?;
    for entry in std::fs::read_dir(sys_block)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let probe = if name.starts_with("loop") {
            "loop/backing_file"
        } else if name.starts_with("dm-") {
            "dm/name"
        } else {
            continue;
        };
        let value = match std::fs::read_to_string(entry.path().join(probe)) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let value = value.trim();
        if (name.starts_with("loop") && backing_belongs_to_sandbox(value, &cache_roots))
            || mapper_belongs_to_sandbox(value, sandbox_id)
        {
            return Ok(true);
        }
    }
    let nodes = match std::fs::read_dir(mapper_dir) {
        Ok(nodes) => nodes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in nodes {
        // Inspect directory entries, not their targets: dangling symlinks are
        // precisely the debt left by an interrupted device-node reconciliation.
        if entry?
            .file_name()
            .to_str()
            .is_some_and(|name| mapper_belongs_to_sandbox(name, sandbox_id))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn mapper_belongs_to_sandbox(name: &str, sandbox_id: &str) -> bool {
    name == format!("sbxroot-{sandbox_id}")
        || name == format!("sbxroot-{sandbox_id}-cow")
        || name.starts_with(&format!("sbxvol-{sandbox_id}-"))
}

fn sandbox_backing_roots(
    cache_root: &Path,
    sandbox_id: &str,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut roots = vec![cache_root.join(sandbox_id)];
    match cache_root.canonicalize() {
        Ok(resolved) if resolved != cache_root => roots.push(resolved.join(sandbox_id)),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(roots)
}

fn backing_belongs_to_sandbox(backing: &str, roots: &[std::path::PathBuf]) -> bool {
    let path = Path::new(backing.strip_suffix(" (deleted)").unwrap_or(backing));
    roots.iter().any(|root| {
        path.strip_prefix(root)
            .is_ok_and(|relative| !relative.as_os_str().is_empty())
    })
}

pub(super) async fn kill_launched_process_after_pid_persist_failure(
    service: &NodeManagerService,
    sandbox_id: &str,
    pid: u32,
) {
    // Supervisor cleanup can reap the original process. Keep its identity across
    // that await so fallback cleanup cannot signal a reused numeric PID.
    let handle = match launch::ProcessHandle::open(pid) {
        Ok(handle) => Some(handle),
        Err(error) if error.is_not_found() => None,
        Err(error) => {
            tracing::warn!(sandbox_id, pid, %error, "failed to pin launched process for cleanup");
            None
        }
    };
    if let Err(error) = stop_sandbox_runtime_with_supervisor(service, sandbox_id, true, None).await
    {
        tracing::warn!(
            sandbox_id,
            error = %error,
            "runtime supervisor failed to stop sandbox after pid persist failure"
        );
    }
    if let Some(handle) = handle {
        match handle.signal_if_owned(sandbox_id, libc::SIGKILL) {
            Ok(true) => {}
            Ok(false) => tracing::debug!(
                sandbox_id,
                pid,
                "sandbox process exited or ownership changed before SIGKILL"
            ),
            Err(error) => {
                tracing::warn!(sandbox_id, pid, %error, "failed to SIGKILL sandbox process");
            }
        }
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

#[cfg(test)]
mod tests;

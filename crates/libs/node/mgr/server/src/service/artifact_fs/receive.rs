use std::fs::File;
use std::path::Path;

use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::{btrfs, directory, error, remove_at, trusted_paths};

pub(in crate::service) struct ReceiveRoot {
    directory: File,
}

impl ReceiveRoot {
    pub(in crate::service) fn open(path: &Path) -> Result<Self, Status> {
        Ok(Self {
            directory: trusted_paths::open_directory(path)?,
        })
    }

    pub(in crate::service) fn stream(&self) -> Result<File, Status> {
        let snapshot = directory(&self.directory, "snapshot".as_ref()).map_err(error)?;
        trusted_paths::open_regular_file(&snapshot, "rootfs.btrfs-send".as_ref())
    }

    pub(in crate::service) fn receive(
        &self,
        stream: File,
        cancel: Option<&CancellationToken>,
    ) -> Result<(), Status> {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(Status::cancelled("btrfs receive cancelled before scrub"));
        }
        let mut command = std::process::Command::new("btrfs");
        configure_receiver(&mut command, &self.directory).map_err(error)?;
        remove_at(&self.directory, "instance".as_ref())?;
        remove_at(&self.directory, "instance.ro".as_ref())?;
        let output = crate::host_command::stream_in_namespace(
            &mut command,
            crate::host_command::MUTATION_BUDGET,
            std::process::Stdio::from(stream),
            std::process::Stdio::piped(),
            cancel,
        )
        .map_err(|error| Status::unavailable(format!("btrfs receive failed: {error}")))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if hit_memory_cap(&stderr) {
                // Healthy commands stay far below the cap, so this stream is
                // corrupt (e.g. an appended second stream); retrying cannot help.
                return Err(Status::failed_precondition(format!(
                    "btrfs receive hit its {} MiB memory cap; the send stream is likely corrupt: {stderr}",
                    RECEIVE_DATA_LIMIT_BYTES >> 20,
                )));
            }
            return Err(Status::internal(format!(
                "btrfs receive exited with {}: {stderr}",
                output.status,
            )));
        }
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(Status::cancelled(
                "btrfs receive cancelled before publication",
            ));
        }
        let received = directory(&self.directory, "instance.ro".as_ref()).map_err(error)?;
        let rootfs = trusted_paths::open_regular_file(&received, "rootfs.ext4".as_ref())?;
        pplx_space_common_core::sandbox_storage::validate_rootfs_bytes(
            rootfs.metadata().map_err(error)?.len(),
        )
        .map_err(error)?;
        btrfs::set_readonly(&received, false).map_err(error)?;
        nix::fcntl::renameat(&self.directory, "instance.ro", &self.directory, "instance")
            .map_err(error)?;
        self.directory.sync_all().map_err(error)
    }
}

/// The CLI realpaths its destination and reopens descendants while receiving;
/// passing /proc/self/fd alone would silently lose the pinned identity. Give
/// this child a private bind of the pinned directory, then let receive chroot
/// into it. The filesystem-root fd opened by -m still allows incremental
/// receive to find the template outside that destination. The fd-based mount
/// API requires Linux 5.2 or later; there is no pathname-based fallback.
#[cfg(target_os = "linux")]
fn configure_receiver(
    command: &mut std::process::Command,
    directory: &File,
) -> std::io::Result<()> {
    command.args(["receive", "--chroot", "-m", "/space/cache", "--", "/tmp"]);
    attach_destination_at_tmp(command, directory)?;
    limit_receiver_memory(command);
    Ok(())
}

/// Heap/anonymous-mapping cap for one `btrfs receive` child.
///
/// btrfs-progs `read_cmd()` allocates each command's declared length before
/// checking its CRC. A corrupt stream (prod-1, 2026-10-20: an appended second
/// stream whose `btrf` magic decoded as a 1.6 GiB length) therefore makes the
/// child allocate and fill ~1.6 GiB inside node-manager's container, and the
/// container OOM kill takes node-manager with it. Healthy commands are at most
/// 64 KiB (protocol v1) or a few hundred KiB (v2), and healthy receives add
/// tens of MiB, so 256 MiB leaves ample headroom while failing a corrupt
/// stream with ENOMEM. RLIMIT_DATA does not count page cache, so large
/// healthy restores are not throttled the way a memory cgroup would be.
const RECEIVE_DATA_LIMIT_BYTES: u64 = 256 << 20;

#[cfg(target_os = "linux")]
fn limit_receiver_memory(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;

    use nix::sys::resource::{Resource, setrlimit};

    // SAFETY: setrlimit is async-signal-safe and touches only this child.
    unsafe {
        command.pre_exec(|| {
            setrlimit(
                Resource::RLIMIT_DATA,
                RECEIVE_DATA_LIMIT_BYTES,
                RECEIVE_DATA_LIMIT_BYTES,
            )?;
            Ok(())
        });
    }
}

/// btrfs-progs reports a failed command-buffer allocation as
/// "not enough memory"; other allocation sites use strerror(ENOMEM).
fn hit_memory_cap(stderr: &str) -> bool {
    stderr.contains("not enough memory") || stderr.contains("Cannot allocate memory")
}

/// Run `command` in its own mount namespace with `directory` bound at `/tmp`.
///
/// The namespace's other mounts are slaves of node-manager's: an unmount in
/// node-manager propagates in, so a receive that started while an encrypted
/// volume was mounted can't keep that volume busy after its release. This
/// relies on node-manager's `/space/cache` being a shared (Bidirectional)
/// mount; under a private parent the copy would stay. Nothing mounted here
/// propagates out, and `/tmp` is private.
#[cfg(target_os = "linux")]
fn attach_destination_at_tmp(
    command: &mut std::process::Command,
    directory: &File,
) -> std::io::Result<()> {
    use std::ffi::CStr;
    use std::os::unix::process::CommandExt;

    use nix::mount::{MsFlags, mount};
    use nix::sched::{CloneFlags, unshare};

    // Clone before unshare: legacy bind mounting an fd from the old mount
    // namespace is rejected by the kernel. A detached mount can be moved
    // into the child's namespace without re-resolving any cache pathname.
    use rustix::mount::{MoveMountFlags, OpenTreeFlags, move_mount, open_tree};
    let tree = open_tree(
        directory,
        c"",
        OpenTreeFlags::OPEN_TREE_CLONE
            | OpenTreeFlags::OPEN_TREE_CLOEXEC
            | OpenTreeFlags::AT_EMPTY_PATH,
    )?;
    // The hook performs only namespace/mount syscalls on preallocated
    // C strings. The captured detached tree remains owned until exec; the
    // attached mount owns the directory thereafter. Slave propagation keeps
    // the /tmp mount child-local, including on Kubernetes hosts, while host
    // unmounts still reach this namespace.
    let attach = move || {
        unshare(CloneFlags::CLONE_NEWNS)?;
        mount(
            None::<&CStr>,
            c"/",
            None::<&CStr>,
            MsFlags::MS_REC | MsFlags::MS_SLAVE,
            None::<&CStr>,
        )?;
        move_mount(
            &tree,
            c"",
            rustix::fs::CWD,
            c"/tmp",
            MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH,
        )?;
        // open_tree can clone a shared source mount. Detach that peer group
        // too, rather than only privatizing the namespace's previous mounts.
        mount(
            None::<&CStr>,
            c"/tmp",
            None::<&CStr>,
            MsFlags::MS_PRIVATE,
            None::<&CStr>,
        )?;
        Ok(())
    };
    // SAFETY: attach uses only async-signal-safe namespace/mount syscalls.
    unsafe {
        command.pre_exec(attach);
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn configure_receiver(
    _command: &mut std::process::Command,
    _directory: &File,
) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

#[cfg(all(test, target_os = "linux"))]
mod tests;

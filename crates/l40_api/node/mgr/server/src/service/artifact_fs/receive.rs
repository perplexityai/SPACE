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

    /// Receive the staged stream. A node-manager that predates the framing
    /// check on upload could store a send the kernel restarted mid-pipe: its
    /// partial output followed by a complete stream of the same read-only
    /// snapshot, which `btrfs receive` rejects. When the receive fails and the
    /// staged stream is one, receive its complete stream instead. Returns the
    /// offset received from when it salvaged one.
    pub(in crate::service) fn receive_staged(
        &self,
        cancel: Option<&CancellationToken>,
    ) -> Result<Option<u64>, Status> {
        let failure = match self.receive(self.stream()?, cancel) {
            Ok(()) => return Ok(None),
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::Cancelled | tonic::Code::DeadlineExceeded
                ) =>
            {
                return Err(status);
            }
            Err(status) => status,
        };
        let mut stream = self.stream()?;
        let cancelled = || cancel.is_some_and(CancellationToken::is_cancelled);
        let complete = crate::service::btrfs_send_stream::last_complete_stream(&stream, &cancelled)
            .map_err(error)?;
        if cancelled() {
            return Err(Status::cancelled(
                "btrfs receive cancelled while looking for a complete stream",
            ));
        }
        match complete {
            Some(offset) if offset > 0 => {
                use std::io::{Seek, SeekFrom};
                stream.seek(SeekFrom::Start(offset)).map_err(error)?;
                self.receive(stream, cancel)?;
                Ok(Some(offset))
            }
            _ => Err(failure),
        }
    }

    pub(in crate::service) fn receive(
        &self,
        stream: File,
        cancel: Option<&CancellationToken>,
    ) -> Result<(), Status> {
        let deadline = std::time::Instant::now() + crate::host_command::MUTATION_BUDGET;
        let stream = ValidatedStream::prepare(&self.directory, &stream, cancel, deadline)?;
        self.receive_validated(stream, cancel)
    }

    fn receive_validated(
        &self,
        stream: ValidatedStream,
        cancel: Option<&CancellationToken>,
    ) -> Result<(), Status> {
        // Validation has its own bounded allowance; keep the full mutation
        // budget for scrub and receive regardless of how large the stream is.
        let deadline = std::time::Instant::now() + crate::host_command::MUTATION_BUDGET;
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            return Err(Status::cancelled("btrfs receive cancelled before scrub"));
        }
        let mut command = std::process::Command::new("btrfs");
        configure_receiver(&mut command, &self.directory).map_err(error)?;
        remove_at(&self.directory, "instance".as_ref())?;
        remove_at(&self.directory, "instance.ro".as_ref())?;
        let budget = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| Status::deadline_exceeded("btrfs receive timed out before execution"))?;
        let output = crate::host_command::stream_in_namespace(
            &mut command,
            budget,
            std::process::Stdio::from(stream.0),
            std::process::Stdio::piped(),
            cancel,
        )
        .map_err(|error| Status::unavailable(format!("btrfs receive failed: {error}")))?;
        if !output.status.success() {
            return Err(Status::internal(format!(
                "btrfs receive exited with {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr),
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

/// The receiver consumes the same private inode that passed validation. An
/// open source fd alone is insufficient: cache peers can still write that inode.
struct ValidatedStream(File);

impl ValidatedStream {
    #[cfg(target_os = "linux")]
    fn prepare(
        directory: &File,
        mut source: &File,
        cancel: Option<&CancellationToken>,
        deadline: std::time::Instant,
    ) -> Result<Self, Status> {
        use std::io::{Read, Seek};

        use nix::fcntl::{OFlag, openat};
        use nix::sys::stat::Mode;

        use super::super::btrfs_send_stream::SendStreamFraming;

        let check_budget = || {
            if cancel.is_some_and(CancellationToken::is_cancelled) {
                return Err(Status::cancelled("btrfs receive validation cancelled"));
            }
            if std::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded(
                    "btrfs receive validation timed out",
                ));
            }
            Ok(())
        };
        check_budget()?;
        if !source.metadata().map_err(error)?.is_file() {
            return Err(Status::invalid_argument(
                "btrfs send input must be a regular file",
            ));
        }
        // No directory entry, even temporarily: a peer with write access to
        // the staging directory cannot replace or open this root-owned inode.
        // Closing it on success, rejection, cancellation or restart reclaims it.
        let mut stream = File::from(
            openat(
                directory,
                ".",
                OFlag::O_TMPFILE | OFlag::O_RDWR | OFlag::O_CLOEXEC,
                Mode::S_IRUSR | Mode::S_IWUSR,
            )
            .map_err(error)?,
        );
        pplx_space_common_io::reflink::preserve_nocow(source, &stream).map_err(error)?;
        // Both files live on the cache btrfs filesystem. Do not silently turn
        // failure into a potentially unbounded byte copy under the mutation lock.
        rustix::fs::ioctl_ficlone(&stream, source).map_err(error)?;
        // Recovery seeks past an interrupted send. Validate and receive that
        // same suffix of the private inode, never the discarded prefix.
        let offset = source.stream_position().map_err(error)?;
        stream
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(error)?;
        let mut framing = SendStreamFraming::new();
        let mut buffer = [0_u8; 128 * 1024];
        loop {
            check_budget()?;
            let count = stream.read(&mut buffer).map_err(error)?;
            if count == 0 {
                break;
            }
            let data = buffer
                .get(..count)
                .ok_or_else(|| error("invalid read length"))?;
            framing.feed(data).map_err(|error| {
                Status::invalid_argument(format!("btrfs send stream rejected: {error}"))
            })?;
        }
        framing.finish().map_err(|error| {
            Status::invalid_argument(format!("btrfs send stream rejected: {error}"))
        })?;
        stream
            .seek(std::io::SeekFrom::Start(offset))
            .map_err(error)?;
        Ok(Self(stream))
    }

    #[cfg(not(target_os = "linux"))]
    fn prepare(
        _directory: &File,
        _source: &File,
        _cancel: Option<&CancellationToken>,
        _deadline: std::time::Instant,
    ) -> Result<Self, Status> {
        Err(error("btrfs receive requires Linux"))
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
    command.args([
        "receive",
        "-e",
        "--chroot",
        "-m",
        "/space/cache",
        "--",
        "/tmp",
    ]);
    attach_destination_at_tmp(command, directory)
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

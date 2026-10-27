//! Pool cloning crosses a privilege boundary: cache entries are group-writable.
//! Pin directories and regular source files without following links, create
//! outputs exclusively, and keep every mutation relative to those descriptors.
//! No recursive permission command or caller-controlled pathname reaches root.

use std::fs::File;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use nix::fcntl::{OFlag, open, openat};
use nix::sys::stat::{Mode, fchmod, mkdirat};
use nix::unistd::{Gid, fchown};

use crate::proto::CloneDirectoryIdentity;

const DIRECTORY: OFlag = OFlag::O_RDONLY
    .union(OFlag::O_DIRECTORY)
    .union(OFlag::O_NOFOLLOW)
    .union(OFlag::O_CLOEXEC);

pub(crate) struct CloneTree {
    cache: File,
    target_name: String,
    target: File,
    instance: File,
    vmstate: File,
    memory: File,
    /// The producer of `vmstate` and `memory`, which restore selects its
    /// consumer from. Older donors have none, which reads as the initial
    /// environment.
    runtime: Option<File>,
    run: File,
}

impl CloneTree {
    /// IDs have already been validated by the RPC boundary. A prepared clone
    /// never creates its root; absence or replacement means cleanup won.
    pub(crate) fn open(
        cache: &Path,
        run: &Path,
        donor: &str,
        target: &str,
        expected: &CloneDirectoryIdentity,
    ) -> io::Result<Self> {
        let cache = File::from(open(cache, DIRECTORY, Mode::empty())?);
        let target_name = target.to_owned();
        let target = directory(&cache, target)?;
        {
            let actual = target.metadata()?;
            if actual.dev() != expected.device || actual.ino() != expected.inode {
                return Err(io::Error::other("prepared clone destination was replaced"));
            }
            if actual.mode() & 0o777 != 0o700 {
                return Err(io::Error::other(
                    "prepared clone destination is no longer private",
                ));
            }
        }
        let donor = directory(&cache, donor)?;
        let instance = directory(&donor, "instance")?;
        let snapshot = directory(&donor, "snapshot")?;
        let vmstate = regular_file(&snapshot, "vmstate")?;
        let memory = regular_file(&snapshot, "memory")?;
        let runtime = match regular_file(&snapshot, "runtime.json") {
            Ok(runtime) => Some(runtime),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let run = File::from(open(run, DIRECTORY, Mode::empty())?);
        Ok(Self {
            cache,
            target_name,
            target,
            instance,
            vmstate,
            memory,
            runtime,
            run,
        })
    }

    pub(crate) fn copy(self, cache_gid: u32, run_gid: u32) -> io::Result<()> {
        self.copy_with_snapshot(cache_gid, run_gid, snapshot)
    }

    fn copy_with_snapshot(
        self,
        cache_gid: u32,
        run_gid: u32,
        snapshot: impl FnOnce(&File, &File) -> io::Result<()>,
    ) -> io::Result<()> {
        snapshot(&self.instance, &self.target)?;
        let instance = directory(&self.target, "instance")?;
        permissions(&instance, cache_gid, 0o2775)?;
        // The clone inherits donor ownership, but the backing file must remain
        // accessible to cache services. Never recursively traverse guest data.
        let rootfs = regular_file(&instance, "rootfs.ext4")?;
        fchown(&rootfs, None, Some(Gid::from_raw(cache_gid)))?;
        let snapshot = create_directory(&self.target, "snapshot")?;
        copy_file(&self.vmstate, &snapshot, "vmstate", cache_gid)?;
        copy_file(&self.memory, &snapshot, "memory", cache_gid)?;
        if let Some(runtime) = &self.runtime {
            copy_file(runtime, &snapshot, "runtime.json", cache_gid)?;
        }
        permissions(&snapshot, cache_gid, 0o2775)?;
        let snapshots = create_directory(&self.target, "snapshots")?;
        permissions(&snapshots, cache_gid, 0o2775)?;
        let run = create_directory(&self.run, &self.target_name)?;
        permissions(&run, run_gid, 0o2770)?;
        // Sharing is irreversible for automatic compensation: cache peers can
        // now graft foreign directories below the root. If the reply or durable
        // publication is lost, disposal must hold this tree for operator repair.
        permissions(&self.target, cache_gid, 0o2775)?;
        // A peer can rename directory entries despite our process-local gate.
        // Do not report success for a different tree now occupying the name.
        let current = directory(&self.cache, &self.target_name)?.metadata()?;
        let pinned = self.target.metadata()?;
        if (current.dev(), current.ino()) != (pinned.dev(), pinned.ino()) {
            return Err(io::Error::other(
                "clone destination changed before publication",
            ));
        }
        Ok(())
    }
}

fn directory(parent: &File, name: &str) -> io::Result<File> {
    Ok(File::from(openat(parent, name, DIRECTORY, Mode::empty())?))
}

fn create_directory(parent: &File, name: &str) -> io::Result<File> {
    // Existing entries are never adopted for output. In particular, a planted
    // symlink must fail without changing its target's permissions or contents.
    mkdirat(parent, name, Mode::from_bits_truncate(0o700))?;
    directory(parent, name)
}

fn regular_file(parent: &File, name: &str) -> io::Result<File> {
    let file = File::from(openat(
        parent,
        name,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )?);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(io::Error::other(
            "clone source must be a single-linked regular file",
        ));
    }
    Ok(file)
}

fn copy_file(source: &File, parent: &File, name: &str, gid: u32) -> io::Result<()> {
    let output = File::from(openat(
        parent,
        name,
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )?);
    crate::contained_copy::copy_on_descriptors(source, &output, source.metadata()?.len())?;
    permissions(&output, gid, 0o660)?;
    output.sync_all()
}

fn permissions(file: &File, gid: u32, mode: u32) -> io::Result<()> {
    fchown(file, None, Some(Gid::from_raw(gid)))?;
    fchmod(file, Mode::from_bits_truncate(mode as _))?;
    Ok(())
}

fn snapshot(source: &File, destination: &File) -> io::Result<()> {
    crate::btrfs::snapshot(source, destination, "instance".as_ref(), false)
}

#[cfg(test)]
mod tests;

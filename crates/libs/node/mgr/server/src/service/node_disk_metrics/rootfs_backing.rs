//! Count canonical local rootfs files, not requested policy or logical releases.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;
use pplx_space_common_core::types::SandboxId;

use super::super::{disk::DirectoryBudget, trusted_paths};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Backing {
    Cow,
    NoCow,
    NonBtrfs,
    Unknown,
}

impl Backing {
    const ALL: [Self; 4] = [Self::Cow, Self::NoCow, Self::NonBtrfs, Self::Unknown];

    fn as_str(self) -> &'static str {
        match self {
            Self::Cow => "cow",
            Self::NoCow => "nocow",
            Self::NonBtrfs => "non_btrfs",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Kind {
    Template,
    Instance,
    SuspendCheckpoint,
    DiskSnapshot,
}

impl Kind {
    const ALL: [Self; 4] = [
        Self::Template,
        Self::Instance,
        Self::SuspendCheckpoint,
        Self::DiskSnapshot,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Template => "template",
            Self::Instance => "instance",
            Self::SuspendCheckpoint => "suspend_checkpoint",
            Self::DiskSnapshot => "disk_snapshot",
        }
    }
}

#[derive(Default)]
struct Inventory {
    counts: BTreeMap<(Kind, Backing), u64>,
}

impl Inventory {
    fn record(&mut self, kind: Kind, directory: &Path) -> io::Result<()> {
        // An absent file is normal for incremental checkpoints, BYOK and
        // concurrent cleanup. Other failures must not masquerade as CoW.
        match std::fs::symlink_metadata(directory.join("rootfs.ext4")) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        let backing = inspect(directory).unwrap_or(Backing::Unknown);
        *self.counts.entry((kind, backing)).or_default() += 1;
        Ok(())
    }
}

fn inspect(directory: &Path) -> io::Result<Backing> {
    let directory = trusted_paths::open_directory(directory).map_err(io::Error::other)?;
    let file = File::from(openat(
        &directory,
        "rootfs.ext4",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_NONBLOCK,
        Mode::empty(),
    )?);
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("rootfs is not a regular file"));
    }
    inspect_file(&file)
}

fn inspect_file(file: &File) -> io::Result<Backing> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{IFlags, fstatfs, ioctl_getflags};

        if fstatfs(file)?.f_type != libc::BTRFS_SUPER_MAGIC {
            return Ok(Backing::NonBtrfs);
        }
        Ok(if ioctl_getflags(file)?.contains(IFlags::NOCOW) {
            Backing::NoCow
        } else {
            Backing::Cow
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        Ok(Backing::NonBtrfs)
    }
}

fn collect(sandboxes: &Path, templates: &Path) -> io::Result<Inventory> {
    // Missing cache roots are not evidence that a node has zero rootfs files.
    std::fs::metadata(sandboxes)?;
    std::fs::metadata(templates)?;
    let started = Instant::now();
    let mut budget = DirectoryBudget::default();
    let mut inventory = Inventory::default();
    for name in budget.directories(templates)? {
        let name_string = name.file_name().unwrap_or_default().to_string_lossy();
        if name_string.starts_with('.')
            || name_string.ends_with(".old")
            || name_string.ends_with(".importing")
        {
            continue;
        }
        for digest in budget.directories(&name)? {
            if digest.file_name().is_some_and(|name| {
                let name = name.to_string_lossy();
                // Import, republish and replaced-generation siblings append
                // dot-separated markers; they are not canonical cache entries.
                name.starts_with("sha256:") && !name.contains('.')
            }) {
                inventory.record(Kind::Template, &digest)?;
            }
        }
    }
    for sandbox in budget.directories(sandboxes)? {
        if sandbox
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<SandboxId>().ok())
            .is_none()
        {
            continue;
        }
        inventory.record(Kind::Instance, &sandbox.join("instance"))?;
        inventory.record(Kind::SuspendCheckpoint, &sandbox.join("snapshot"))?;
        for snapshot in budget.directories(&sandbox.join("snapshots"))? {
            inventory.record(Kind::DiskSnapshot, &snapshot)?;
        }
    }
    // The directory budget checks between walks; cover the final file probes
    // too. Never publish a slow or truncated census as a fresh complete one.
    if started.elapsed() >= Duration::from_secs(2) {
        return Err(io::Error::other("rootfs inventory budget exceeded"));
    }
    Ok(inventory)
}

pub(super) fn emit_inventory(sandboxes: &Path, templates: &Path) {
    use pplx_space_metrics::node_disk;

    let observed_at = SystemTime::now();
    match collect(sandboxes, templates) {
        Ok(inventory) => {
            // Explicit zeroes clear categories removed since the last tick.
            for kind in Kind::ALL {
                for backing in Backing::ALL {
                    let count = inventory.counts.get(&(kind, backing)).copied().unwrap_or(0);
                    node_disk::rootfs_backing_count(kind.as_str(), backing.as_str(), count);
                }
            }
            node_disk::rootfs_backing_inventory_success(true);
            if let Ok(timestamp) = observed_at.duration_since(UNIX_EPOCH) {
                node_disk::rootfs_backing_last_success(timestamp.as_secs_f64());
            }
        }
        Err(error) => {
            node_disk::rootfs_backing_inventory_success(false);
            tracing::warn!(%error, "rootfs backing inventory incomplete; retaining previous counts");
        }
    }
}

#[cfg(test)]
mod tests;

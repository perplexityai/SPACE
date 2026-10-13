//! Retained-cache ownership boundaries shared by cleanup and reset.
use std::ffi::OsStr;
use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub(crate) const RETAINED_SOURCE_BUNDLE: &str = "rootfs.btrfs-source";
pub(crate) const RETAINED_SOURCE_GID: u32 =
    pplx_space_common_core::host::linux::identities::gids::SPACE_CACHE_RW;
pub(crate) const RETAINED_SOURCE_MODE: u32 = 0o750;

/// Capture seals immutable send inputs behind this neutral access boundary.
/// Their original UID/GID may survive slot reuse because sandbox processes
/// have no cache-group membership. Preserve the boundary itself as well as
/// its contents; the ordinary rewrite would widen it to group-writable 2770.
/// A name alone, a symlink, or an accessible lookalike grants no exemption.
pub(crate) fn is_private_send_source(name: &OsStr, metadata: &Metadata) -> bool {
    name == RETAINED_SOURCE_BUNDLE
        && metadata.is_dir()
        && metadata.uid() == 0
        && metadata.gid() == RETAINED_SOURCE_GID
        && metadata.mode() & 0o7777 == RETAINED_SOURCE_MODE
}

pub(crate) fn first_path_owned_by_uid(path: &Path, uid: u32) -> Result<Option<PathBuf>, String> {
    let mut stack = vec![path.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(format!("failed to stat {}: {error}", path.display())),
        };
        if metadata.uid() == uid || metadata.gid() == uid {
            return Ok(Some(path));
        }
        if path
            .file_name()
            .is_some_and(|name| is_private_send_source(name, &metadata))
        {
            continue;
        }
        if metadata.is_dir() {
            let entries = std::fs::read_dir(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            for entry in entries {
                let entry = entry.map_err(|error| {
                    format!("failed to read entry in {}: {error}", path.display())
                })?;
                stack.push(entry.path());
            }
        }
    }
    Ok(None)
}

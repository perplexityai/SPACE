use std::fs::{File, OpenOptions};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

/// Sandbox-manager creates the destination; volume-manager writes the pull.
/// Preserve the provisioned cache group and setgid bit across that handoff.
pub(crate) fn prepare(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    let directory: File = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let mode = directory.metadata()?.permissions().mode();
    // Root-owned cache directories may already grant the required access.
    // Neither change their owner nor require a non-owner chmod on retries.
    if mode & 0o770 == 0o770 {
        return Ok(());
    }
    directory.set_permissions(std::fs::Permissions::from_mode((mode & 0o7777) | 0o070))
}

#[cfg(test)]
mod tests;

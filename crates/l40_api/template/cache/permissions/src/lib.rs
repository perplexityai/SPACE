//! The shared template cache's permission rule: the rwx bits template-manager
//! and sandbox-manager apply to template artifacts, preserving the setgid bits
//! that provisioning sets.

use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use pplx_space_host_layout::paths::{TEMPLATE_CACHE_DIR_MODE, TEMPLATE_CACHE_FILE_MODE};

/// Applies the shared SPACE template cache rwx bits to one path.
///
/// Runtime services run with `RestrictSUIDSGID=true`, so they only manage
/// normal rwx bits. Cache parent setgid bits come from provisioning, and
/// on a freshly created child the kernel propagates that setgid bit from
/// the parent inode (so the child also inherits the parent's group). We
/// must preserve those inherited special bits when rewriting the rwx
/// triplet — clobbering them strips the setgid from new staging dirs,
/// which in turn breaks group inheritance for any file copied in
/// afterward (the file gets the creator's primary group instead of
/// `space-cache-rw`, leaving the sibling sandbox-manager service
/// unable to read it).
#[cfg(unix)]
pub fn set_template_cache_permissions(path: impl AsRef<Path>, mode: u32) -> std::io::Result<()> {
    let path = path.as_ref();
    let metadata = std::fs::symlink_metadata(path)?;
    let current_mode = metadata.permissions().mode() & 0o7777;
    let desired_rwx = mode & 0o777;
    if current_mode & 0o777 == desired_rwx {
        return Ok(());
    }

    // Splice the new rwx triplet into the existing mode without touching
    // setuid/setgid/sticky bits.
    let desired_mode = (current_mode & !0o777) | desired_rwx;
    let mut permissions = metadata.permissions();
    permissions.set_mode(desired_mode);
    match std::fs::set_permissions(path, permissions) {
        Ok(()) => Ok(()),
        // Template artifacts can be owned by either space-tmplmgr or space-sbxmgr.
        // If the sibling service already left a safe superset of the requested mode,
        // do not fail just because this service is not the inode owner.
        Err(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied
                && current_mode & desired_rwx == desired_rwx =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// Normalizes template cache directory/file rwx bits.
///
/// Runtime services leave special bits such as setgid to provisioning or
/// root-owned producers.
#[cfg(unix)]
pub fn normalize_template_cache_tree(path: impl AsRef<Path>) -> std::io::Result<()> {
    let path = path.as_ref();
    let metadata = std::fs::symlink_metadata(path)?;

    if metadata.is_dir() {
        set_template_cache_permissions(path, TEMPLATE_CACHE_DIR_MODE)?;
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                normalize_template_cache_tree(entry.path())?;
            } else if file_type.is_file() {
                set_template_cache_permissions(entry.path(), TEMPLATE_CACHE_FILE_MODE)?;
            }
        }
    } else if metadata.is_file() {
        set_template_cache_permissions(path, TEMPLATE_CACHE_FILE_MODE)?;
    }

    Ok(())
}

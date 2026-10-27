use std::fs;
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};

use super::{
    GuestBundleError, GuestBundleOperation, normalize_relative_path, path_arg, privileged_command,
    run_status,
};

pub fn install_current_guest(
    target_root: &Path,
    guest_bin_dir: &Path,
    bundle_version: &str,
) -> Result<(), GuestBundleError> {
    install_current_guest_with_privilege(target_root, guest_bin_dir, bundle_version, true)
}

pub(super) fn install_current_guest_with_privilege(
    target_root: &Path,
    guest_bin_dir: &Path,
    bundle_version: &str,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    validate_target_root(target_root)?;
    for binary in ["spaced", "spaced-svc"] {
        let source = guest_bin_dir.join(binary);
        if !source.is_file() {
            return Err(GuestBundleError::MissingPath { path: source });
        }
        let relative = Path::new("usr/local/bin").join(binary);
        prepare_target_file(
            target_root,
            &relative,
            GuestBundleOperation::Install,
            as_root,
        )?;
        install(&source, &target_root.join(relative), 0o755, as_root)?;
    }

    let mut version_file =
        tempfile::NamedTempFile::new().map_err(|source| GuestBundleError::Io {
            operation: GuestBundleOperation::Install,
            path: target_root.to_path_buf(),
            source,
        })?;
    writeln!(version_file, "{bundle_version}").map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Install,
        path: version_file.path().to_path_buf(),
        source,
    })?;
    let relative = Path::new("etc/spaced/bundle-version");
    prepare_target_file(
        target_root,
        relative,
        GuestBundleOperation::Install,
        as_root,
    )?;
    install(
        version_file.path(),
        &target_root.join(relative),
        0o644,
        as_root,
    )
}

pub(crate) fn install_working_directory(
    target_root: &Path,
    working_dir: &Path,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    validate_target_root(target_root)?;
    let mut file = tempfile::NamedTempFile::new().map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Install,
        path: target_root.to_path_buf(),
        source,
    })?;
    file.write_all(working_dir.as_os_str().as_encoded_bytes())
        .map_err(|source| GuestBundleError::Io {
            operation: GuestBundleOperation::Install,
            path: file.path().to_path_buf(),
            source,
        })?;
    let relative = Path::new("etc/spaced/working-directory");
    prepare_target_file(
        target_root,
        relative,
        GuestBundleOperation::Install,
        as_root,
    )?;
    install(file.path(), &target_root.join(relative), 0o644, as_root)
}

pub(super) fn validate_target_root(target_root: &Path) -> Result<(), GuestBundleError> {
    let metadata = fs::symlink_metadata(target_root).map_err(|source| GuestBundleError::Io {
        operation: GuestBundleOperation::Inspect,
        path: target_root.to_path_buf(),
        source,
    })?;
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        Ok(())
    } else {
        Err(GuestBundleError::UnsafeTarget {
            path: target_root.to_path_buf(),
        })
    }
}

pub(super) fn prepare_target_directory(
    target_root: &Path,
    relative: &Path,
    operation: GuestBundleOperation,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    let relative = normalize_relative_path(relative)?;
    let mut current = target_root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(GuestBundleError::UnsafeTarget {
                path: relative.clone(),
            });
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if let Some(resolved) =
                    resolve_safe_target_directory(target_root, &current, &metadata)
                {
                    if metadata.file_type().is_symlink() {
                        normalize_absolute_directory_symlink(
                            target_root,
                            &current,
                            &resolved,
                            operation,
                            as_root,
                        )?;
                    }
                } else {
                    remove_target_path(&current, operation, as_root)?;
                    create_target_directory(&current, operation, as_root)?;
                }
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                create_target_directory(&current, operation, as_root)?;
            }
            Err(source) => {
                return Err(GuestBundleError::Io {
                    operation,
                    path: current,
                    source,
                });
            }
        }
    }
    Ok(())
}

pub(super) fn replace_target_directory(
    target_root: &Path,
    relative: &Path,
    operation: GuestBundleOperation,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    let relative = normalize_relative_path(relative)?;
    let parent = relative
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| GuestBundleError::UnsafeTarget {
            path: relative.clone(),
        })?;
    prepare_target_directory(target_root, parent, operation, as_root)?;

    let path = target_root.join(relative);
    let path = match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if let Some(resolved) = resolve_safe_target_directory(target_root, &path, &metadata) {
                if metadata.file_type().is_symlink() {
                    normalize_absolute_directory_symlink(
                        target_root,
                        &path,
                        &resolved,
                        operation,
                        as_root,
                    )?;
                }
                resolved
            } else {
                path
            }
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return create_target_directory(&path, operation, as_root);
        }
        Err(source) => {
            return Err(GuestBundleError::Io {
                operation,
                path,
                source,
            });
        }
    };
    remove_target_path(&path, operation, as_root)?;
    create_target_directory(&path, operation, as_root)
}

fn resolve_safe_target_directory(
    target_root: &Path,
    path: &Path,
    metadata: &fs::Metadata,
) -> Option<PathBuf> {
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        return Some(path.to_path_buf());
    }
    if !metadata.file_type().is_symlink() {
        return None;
    }
    let target_root = fs::canonicalize(target_root).ok()?;
    let parent = path.parent()?;
    let canonical_parent = fs::canonicalize(parent).ok()?;
    let link_target = fs::read_link(path).ok()?;
    let resolved = if link_target.is_absolute() {
        let Ok(relative) = link_target.strip_prefix(Path::new("/")) else {
            return None;
        };
        target_root.join(relative)
    } else {
        parent.join(link_target)
    };
    let path = fs::canonicalize(resolved).ok()?;
    (path.starts_with(target_root) && !canonical_parent.starts_with(&path) && path.is_dir())
        .then_some(path)
}

fn normalize_absolute_directory_symlink(
    target_root: &Path,
    path: &Path,
    resolved: &Path,
    operation: GuestBundleOperation,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    let link_target = fs::read_link(path).map_err(|source| GuestBundleError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    })?;
    if !link_target.is_absolute() {
        return Ok(());
    }
    // Host tools resolve absolute image links against the builder's root. An
    // equivalent relative link keeps extraction inside the image rootfs.
    let target_root = fs::canonicalize(target_root).map_err(|source| GuestBundleError::Io {
        operation,
        path: target_root.to_path_buf(),
        source,
    })?;
    let parent = path
        .parent()
        .ok_or_else(|| GuestBundleError::UnsafeTarget {
            path: path.to_path_buf(),
        })?;
    let parent = fs::canonicalize(parent).map_err(|source| GuestBundleError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    })?;
    let parent = parent
        .strip_prefix(&target_root)
        .map_err(|_| GuestBundleError::UnsafeTarget {
            path: path.to_path_buf(),
        })?;
    let resolved =
        resolved
            .strip_prefix(&target_root)
            .map_err(|_| GuestBundleError::UnsafeTarget {
                path: path.to_path_buf(),
            })?;
    let mut relative = PathBuf::new();
    for _ in parent.components() {
        relative.push("..");
    }
    relative.push(resolved);
    remove_target_path(path, operation, as_root)?;
    let mut command = privileged_command("ln", as_root);
    command.args(["-s", "--"]).arg(relative).arg(path);
    run_status(operation, &mut command)
}

fn prepare_target_file(
    target_root: &Path,
    relative: &Path,
    operation: GuestBundleOperation,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    let relative = normalize_relative_path(relative)?;
    let parent = relative
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| GuestBundleError::UnsafeTarget {
            path: relative.clone(),
        })?;
    prepare_target_directory(target_root, parent, operation, as_root)?;
    let target = target_root.join(relative);
    match fs::symlink_metadata(&target) {
        Ok(_) => remove_target_path(&target, operation, as_root),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(GuestBundleError::Io {
            operation,
            path: target,
            source,
        }),
    }
}

fn remove_target_path(
    path: &Path,
    operation: GuestBundleOperation,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    let mut command = privileged_command("rm", as_root);
    command.args(["-rf", "--"]).arg(path);
    run_status(operation, &mut command)
}

fn create_target_directory(
    path: &Path,
    operation: GuestBundleOperation,
    as_root: bool,
) -> Result<(), GuestBundleError> {
    let mut command = privileged_command("mkdir", as_root);
    command.arg(path);
    run_status(operation, &mut command)
}

fn install(source: &Path, target: &Path, mode: u32, as_root: bool) -> Result<(), GuestBundleError> {
    let mut command = privileged_command("install", as_root);
    command.args([
        "-m",
        format!("{mode:o}").as_str(),
        path_arg(source).as_str(),
        path_arg(target).as_str(),
    ]);
    run_status(GuestBundleOperation::Install, &mut command)
}

use pplx_space_common_core::types::guest_image::GuestDiskLayout;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExpandedRootfsError {
    #[error("{operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation} failed for {path}")]
    Command {
        operation: &'static str,
        path: String,
    },
    #[error(
        "requested rootfs size {rootfs_size_mb} MiB is smaller than current rootfs size {current_mb} MiB"
    )]
    TooSmall {
        rootfs_size_mb: u32,
        current_mb: u64,
    },
    #[error("template rootfs is missing: {path}")]
    Missing { path: String },
    #[error("template metadata is invalid for {path}: {source}")]
    InvalidMetadata {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error(
        "template rootfs has wrong size for {path}: expected {expected_bytes} bytes, got {actual_bytes} bytes"
    )]
    WrongSize {
        path: String,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    #[error("invalid template metadata schema: {0}")]
    Schema(#[from] pplx_space_common_core::types::TemplateMetadataSchemaError),
    #[error("invalid template manifest schema: {0}")]
    ManifestSchema(#[from] pplx_space_common_core::types::TemplateManifestSchemaError),
    #[error("template metadata and manifest guest image contracts disagree")]
    GuestImageMismatch,
    #[error("invalid guest profile: {0}")]
    GuestProfile(#[from] pplx_space_execution_catalog::guest::GuestProfileError),
}

impl ExpandedRootfsError {
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::Io { .. } => "io",
            Self::Command { .. } => "command",
            Self::TooSmall { .. } => "too_small",
            Self::Missing { .. } => "missing",
            Self::InvalidMetadata { .. }
            | Self::Schema(_)
            | Self::ManifestSchema(_)
            | Self::GuestImageMismatch
            | Self::GuestProfile(_) => "invalid_metadata",
            Self::WrongSize { .. } => "wrong_size",
        }
    }
}

use pplx_space_common_core::types::{TEMPLATE_ROOTFS_FILE, TemplateManifest, TemplateMetadata};
use pplx_space_common_svc::page_cache;

/// Expand the template rootfs to the size declared in `metadata.json`.
/// Returns `Some((path, final_size_bytes))` when metadata declares a size,
/// `None` when there is no `disk_size_mb` in metadata.
pub(crate) fn expand_rootfs_from_metadata(
    build_dir: impl AsRef<Path>,
) -> Result<Option<(PathBuf, u64)>, ExpandedRootfsError> {
    let build_dir = build_dir.as_ref();
    let metadata = read_metadata(build_dir)?;
    let Some(rootfs_size_mb) = metadata.disk_size_mb.filter(|value| *value > 0) else {
        return Ok(None);
    };
    let layout = metadata.guest_image.unwrap_or_default().disk();
    let size_bytes = u64::from(rootfs_size_mb) * 1024 * 1024;
    expand_rootfs(&build_dir.join("rootfs.ext4"), rootfs_size_mb, layout)
        .map(|path| Some((path, size_bytes)))
}

/// Verify that a published template build_dir contains a runtime-sized
/// rootfs.ext4. When metadata declares `disk_size_mb`, that expanded size is
/// the local-cache invariant on both builders and workers. Builds without a
/// disk size fall back to the manifest rootfs size for older or non-Firecracker
/// artifacts.
pub(crate) fn verify_rootfs_from_metadata(
    build_dir: impl AsRef<Path>,
) -> Result<(), ExpandedRootfsError> {
    let build_dir = build_dir.as_ref();
    let expanded_bytes =
        rootfs_size_mb_from_metadata(build_dir)?.map(|mb| u64::from(mb) * 1024 * 1024);
    let manifest_bytes = if expanded_bytes.is_none() {
        manifest_rootfs_size_bytes(build_dir)?
    } else {
        None
    };
    let expected_bytes = expanded_bytes.or(manifest_bytes);

    if expected_bytes.is_none() {
        // Nothing to verify against; treat as available.
        return Ok(());
    }

    let rootfs_path = build_dir.join("rootfs.ext4");
    let actual_bytes = match std::fs::metadata(&rootfs_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(ExpandedRootfsError::Missing {
                path: rootfs_path.display().to_string(),
            });
        }
        Err(source) => {
            return Err(ExpandedRootfsError::Io {
                operation: "stat template rootfs",
                path: rootfs_path.display().to_string(),
                source,
            });
        }
    };

    if expected_bytes == Some(actual_bytes) {
        return Ok(());
    }
    Err(ExpandedRootfsError::WrongSize {
        path: rootfs_path.display().to_string(),
        expected_bytes: expected_bytes.unwrap_or(actual_bytes),
        actual_bytes,
    })
}

fn manifest_rootfs_size_bytes(build_dir: &Path) -> Result<Option<u64>, ExpandedRootfsError> {
    let manifest_path = build_dir.join("manifest.json");
    let manifest_json = match std::fs::read_to_string(&manifest_path) {
        Ok(json) => json,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ExpandedRootfsError::Io {
                operation: "read manifest",
                path: manifest_path.display().to_string(),
                source,
            });
        }
    };
    let manifest = serde_json::from_str::<TemplateManifest>(&manifest_json).map_err(|source| {
        ExpandedRootfsError::InvalidMetadata {
            path: manifest_path.display().to_string(),
            source,
        }
    })?;
    Ok(manifest
        .files
        .get(TEMPLATE_ROOTFS_FILE)
        .and_then(|f| f.size)
        .filter(|s| *s > 0))
}

pub(crate) fn rootfs_size_mb_from_metadata(
    build_dir: &Path,
) -> Result<Option<u32>, ExpandedRootfsError> {
    Ok(read_metadata(build_dir)?
        .disk_size_mb
        .filter(|value| *value > 0))
}

fn read_metadata(build_dir: &Path) -> Result<TemplateMetadata, ExpandedRootfsError> {
    let metadata_path = build_dir.join("metadata.json");
    let metadata_json = match std::fs::read_to_string(&metadata_path) {
        Ok(metadata_json) => metadata_json,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => "{}".to_owned(),
        Err(source) => {
            return Err(ExpandedRootfsError::Io {
                operation: "read metadata",
                path: metadata_path.display().to_string(),
                source,
            });
        }
    };
    let metadata = serde_json::from_str::<TemplateMetadata>(&metadata_json).map_err(|source| {
        ExpandedRootfsError::InvalidMetadata {
            path: metadata_path.display().to_string(),
            source,
        }
    })?;
    if let Some(execution) = metadata.execution_metadata()? {
        pplx_space_execution_catalog::guest::GuestProfile::resolve(
            execution,
            metadata.guest_image.unwrap_or_default(),
        )?;
    }
    let manifest_path = build_dir.join("manifest.json");
    match std::fs::read_to_string(&manifest_path) {
        Ok(bytes) => {
            let manifest: TemplateManifest = serde_json::from_str(&bytes).map_err(|source| {
                ExpandedRootfsError::InvalidMetadata {
                    path: manifest_path.display().to_string(),
                    source,
                }
            })?;
            manifest.execution_metadata()?;
            if manifest.guest_image != metadata.guest_image {
                return Err(ExpandedRootfsError::GuestImageMismatch);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(ExpandedRootfsError::Io {
                operation: "read manifest",
                path: manifest_path.display().to_string(),
                source,
            });
        }
    }

    Ok(metadata)
}

fn expand_rootfs(
    rootfs_path: &Path,
    rootfs_size_mb: u32,
    layout: GuestDiskLayout,
) -> Result<PathBuf, ExpandedRootfsError> {
    let requested_bytes = u64::from(rootfs_size_mb) * 1024 * 1024;
    let current_bytes = std::fs::metadata(rootfs_path)
        .map(|metadata| metadata.len())
        .map_err(|source| ExpandedRootfsError::Io {
            operation: "stat template rootfs",
            path: rootfs_path.display().to_string(),
            source,
        })?;
    if requested_bytes < current_bytes {
        return Err(ExpandedRootfsError::TooSmall {
            rootfs_size_mb,
            current_mb: current_bytes.div_ceil(1024 * 1024),
        });
    }
    if requested_bytes == current_bytes {
        return Ok(rootfs_path.to_path_buf());
    }

    // Extending an opaque disk preserves its partition table and filesystem;
    // only the guest may resize those structures.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(rootfs_path)
        .map_err(|source| ExpandedRootfsError::Io {
            operation: "open disk",
            path: rootfs_path.display().to_string(),
            source,
        })?;
    file.set_len(requested_bytes)
        .map_err(|source| ExpandedRootfsError::Io {
            operation: "extend disk",
            path: rootfs_path.display().to_string(),
            source,
        })?;
    if layout == GuestDiskLayout::Ext4Filesystem {
        let mut fsck = std::process::Command::new("e2fsck");
        fsck.arg("-fy").arg(rootfs_path);
        run_status_command("e2fsck", &mut fsck, rootfs_path, |code| code == Some(1))?;
        let mut resize = std::process::Command::new("resize2fs");
        resize.arg(rootfs_path);
        run_status_command("resize2fs", &mut resize, rootfs_path, |_| false)?;
    }

    let rootfs_path = checked_rootfs_size(rootfs_path.to_path_buf(), requested_bytes)?;
    page_cache::advise_path_dontneed(&rootfs_path, "expand template rootfs");
    Ok(rootfs_path)
}

fn checked_rootfs_size(path: PathBuf, expected_bytes: u64) -> Result<PathBuf, ExpandedRootfsError> {
    match std::fs::metadata(&path) {
        Ok(metadata) if metadata.len() == expected_bytes => Ok(path),
        Ok(metadata) => Err(ExpandedRootfsError::WrongSize {
            path: path.display().to_string(),
            expected_bytes,
            actual_bytes: metadata.len(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(ExpandedRootfsError::Missing {
                path: path.display().to_string(),
            })
        }
        Err(source) => Err(ExpandedRootfsError::Io {
            operation: "stat template rootfs",
            path: path.display().to_string(),
            source,
        }),
    }
}

fn run_status_command(
    operation: &'static str,
    command: &mut std::process::Command,
    path: &Path,
    accept_code: impl FnOnce(Option<i32>) -> bool,
) -> Result<(), ExpandedRootfsError> {
    let status = command.status().map_err(|source| ExpandedRootfsError::Io {
        operation,
        path: path.display().to_string(),
        source,
    })?;
    if status.success() || accept_code(status.code()) {
        Ok(())
    } else {
        Err(ExpandedRootfsError::Command {
            operation,
            path: path.display().to_string(),
        })
    }
}

#[cfg(test)]
mod tests;

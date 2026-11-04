use std::path::{Component, Path, PathBuf};

use pplx_space_common_core::types::NodeSpacePath;
use tonic::Status;

const VMSTATE_ARTIFACT: &str = "vmstate";
const MEMORY_ARTIFACT: &str = "memory";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeKind {
    Firecracker,
}

#[derive(Clone, Copy, Debug)]
pub struct RuntimePaths {
    kind: RuntimeKind,
}

#[derive(Debug)]
pub struct CreateSnapshotPaths {
    pub vmstate_name: String,
    pub memory_name: String,
    pub dest_dir: Option<PathBuf>,
}

#[derive(Debug)]
pub struct LoadSnapshotPaths {
    pub vmstate_name: String,
    pub memory_name: String,
    pub source_dir: Option<PathBuf>,
}

impl RuntimePaths {
    pub fn firecracker() -> Self {
        Self {
            kind: RuntimeKind::Firecracker,
        }
    }

    pub(crate) fn create_snapshot_paths(
        &self,
        sandbox_id: &str,
        files: &SnapshotFiles,
        dest_dir: Option<&NodeSpacePath>,
    ) -> Result<CreateSnapshotPaths, Status> {
        match self.kind {
            RuntimeKind::Firecracker => {
                let vmstate_name = files.state().to_owned();
                let memory_name = files.memory().to_owned();
                let dest_dir = dest_dir
                    .map(|path| {
                        validate_create_snapshot_dest_path(sandbox_id, path.as_path().to_owned())
                    })
                    .transpose()?;
                Ok(CreateSnapshotPaths {
                    vmstate_name,
                    memory_name,
                    dest_dir,
                })
            }
        }
    }

    pub(crate) fn load_snapshot_paths(
        &self,
        sandbox_id: &str,
        files: &SnapshotFiles,
        source_dir: Option<&NodeSpacePath>,
    ) -> Result<LoadSnapshotPaths, Status> {
        match self.kind {
            RuntimeKind::Firecracker => {
                let vmstate_name = files.state().to_owned();
                let memory_name = files.memory().to_owned();
                let source_dir = source_dir
                    .map(|path| {
                        validate_load_snapshot_source(sandbox_id, path.as_path().to_owned())
                    })
                    .transpose()?;
                Ok(LoadSnapshotPaths {
                    vmstate_name,
                    memory_name,
                    source_dir,
                })
            }
        }
    }
}

pub fn validate_output_file(path: &Path) -> Result<(), Status> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(Status::permission_denied(format!(
                "refusing to overwrite symlink output file: {}",
                path.display()
            )))
        }
        Ok(metadata) if !metadata.is_file() => Err(Status::invalid_argument(format!(
            "snapshot output path exists and is not a regular file: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Status::internal(format!(
            "stat snapshot output path {}: {error}",
            path.display()
        ))),
    }
}

fn validate_firecracker_jail_artifact(
    field: &'static str,
    value: &str,
    expected: &'static str,
) -> Result<String, Status> {
    let value = value.trim_start_matches('/');
    if value != expected {
        return Err(Status::invalid_argument(format!(
            "{field} must reference Firecracker artifact '{expected}'"
        )));
    }
    Ok(value.to_owned())
}

pub(super) fn validate_create_snapshot_dest(
    sandbox_id: &str,
    dest_dir: &str,
) -> Result<PathBuf, Status> {
    let path = normalize_absolute_path("dest_dir", dest_dir)?;
    validate_create_snapshot_dest_path(sandbox_id, path)
}

fn validate_create_snapshot_dest_path(sandbox_id: &str, path: PathBuf) -> Result<PathBuf, Status> {
    let sandbox_root =
        Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR).join(sandbox_id);
    let allowed_roots = [
        sandbox_root.join("snapshot"),
        sandbox_root.join("snapshots"),
        sandbox_root.join("template-snapshot-staging"),
    ];
    validate_under_allowed_roots("dest_dir", &path, &allowed_roots)?;
    Ok(path)
}

fn validate_load_snapshot_source(sandbox_id: &str, path: PathBuf) -> Result<PathBuf, Status> {
    let expected = Path::new(pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR)
        .join(sandbox_id)
        .join("snapshot");
    if path != expected {
        return Err(Status::permission_denied(format!(
            "source_dir must be the sandbox checkpoint directory {}",
            expected.display()
        )));
    }
    validate_under_allowed_roots("source_dir", &path, &[expected])?;
    validate_regular_file_no_symlink(&path.join(VMSTATE_ARTIFACT))?;
    validate_regular_file_no_symlink(&path.join(MEMORY_ARTIFACT))?;
    Ok(path)
}

fn normalize_absolute_path(field: &'static str, value: &str) -> Result<PathBuf, Status> {
    let path = Path::new(value);
    if !path.is_absolute() {
        return Err(Status::invalid_argument(format!(
            "{field} must be absolute"
        )));
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => normalized.push(Path::new("/")),
            Component::Normal(part) => normalized.push(part),
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(Status::invalid_argument(format!(
                    "{field} contains invalid path component"
                )));
            }
        }
    }
    Ok(normalized)
}

fn validate_under_allowed_roots(
    field: &'static str,
    path: &Path,
    allowed_roots: &[PathBuf],
) -> Result<(), Status> {
    let allowed_root = allowed_roots
        .iter()
        .find(|root| path.starts_with(root))
        .ok_or_else(|| {
            Status::permission_denied(format!("{field} is outside allowed runtime roots"))
        })?;

    let existing_path = nearest_existing_path(path)?;
    reject_symlink(existing_path.as_path())?;
    let existing_canonical = std::fs::canonicalize(&existing_path).map_err(|error| {
        Status::internal(format!(
            "canonicalize {} failed: {error}",
            existing_path.display()
        ))
    })?;

    let allowed_existing = nearest_existing_path(allowed_root)?;
    reject_symlink(allowed_existing.as_path())?;
    let allowed_canonical = std::fs::canonicalize(&allowed_existing).map_err(|error| {
        Status::internal(format!(
            "canonicalize {} failed: {error}",
            allowed_existing.display()
        ))
    })?;

    if !existing_canonical.starts_with(&allowed_canonical) {
        return Err(Status::permission_denied(format!(
            "{field} resolves outside allowed runtime root"
        )));
    }
    Ok(())
}

fn nearest_existing_path(path: &Path) -> Result<PathBuf, Status> {
    let mut current = path;
    loop {
        match std::fs::symlink_metadata(current) {
            Ok(_) => return Ok(current.to_path_buf()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                current = current.parent().ok_or_else(|| {
                    Status::not_found(format!("no existing ancestor for {}", path.display()))
                })?;
            }
            Err(error) => {
                return Err(Status::internal(format!(
                    "stat {} failed: {error}",
                    current.display()
                )));
            }
        }
    }
}

fn reject_symlink(path: &Path) -> Result<(), Status> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| Status::internal(format!("stat {} failed: {error}", path.display())))?;
    if metadata.file_type().is_symlink() {
        return Err(Status::permission_denied(format!(
            "path component must not be a symlink: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_regular_file_no_symlink(path: &Path) -> Result<(), Status> {
    let metadata = std::fs::symlink_metadata(path).map_err(|error| {
        Status::not_found(format!(
            "snapshot artifact {} not found: {error}",
            path.display()
        ))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(Status::permission_denied(format!(
            "snapshot artifact must not be a symlink: {}",
            path.display()
        )));
    }
    if !metadata.is_file() {
        return Err(Status::invalid_argument(format!(
            "snapshot artifact is not a regular file: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Normalized fixed filenames for plaintext Firecracker snapshot artifacts.
#[derive(Clone, Debug)]
pub(crate) struct SnapshotFiles {
    state: String,
    memory: String,
}

impl SnapshotFiles {
    pub(crate) fn parse(state: String, memory: String) -> Result<Self, Status> {
        Ok(Self {
            state: validate_firecracker_jail_artifact("snapshot_path", &state, VMSTATE_ARTIFACT)?,
            memory: validate_firecracker_jail_artifact("mem_file_path", &memory, MEMORY_ARTIFACT)?,
        })
    }
    pub fn state(&self) -> &str {
        &self.state
    }
    pub fn memory(&self) -> &str {
        &self.memory
    }
}

//! The supervisor is the consumer of the privileged executable: it opens,
//! authenticates and executes the same jailer inode, without a second path open.

use std::path::{Path, PathBuf};

use pplx_space_common_core::types::EnvironmentReference;
use pplx_space_node_runtime_core::{
    artifact_store::VerifyError,
    catalog::{CompiledEnvironment, compiled_verity_digest},
    runtime_inventory::host_arch,
    runtime_launch::FirecrackerLaunch,
    verity::{SealedFile, VerityDigest, VerityError},
};
use tonic::Status;

#[derive(Debug)]
pub(crate) struct VerifiedLaunch {
    root: PathBuf,
    jailer: VerityDigest,
    firecracker: VerityDigest,
}

impl VerifiedLaunch {
    pub(crate) fn resolve(
        reference: EnvironmentReference,
        request: &FirecrackerLaunch,
    ) -> Result<Self, Status> {
        let architecture =
            host_arch().map_err(|error| Status::failed_precondition(error.to_string()))?;
        let environments = CompiledEnvironment::cached_for_architecture(architecture)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let environment = environments
            .iter()
            .find(|environment| environment.definition().reference() == reference)
            .ok_or_else(|| Status::failed_precondition("unknown compiled environment reference"))?;
        Self::from_environment(
            environment,
            request.firecracker_bin.as_path(),
            request.jailer_bin.as_path(),
        )
    }

    pub(crate) fn from_environment(
        environment: &CompiledEnvironment,
        firecracker: &Path,
        jailer: &Path,
    ) -> Result<Self, Status> {
        let release = environment.release();
        let root = firecracker
            .parent()
            .and_then(Path::parent)
            .ok_or_else(|| Status::invalid_argument("missing execution source paths"))?;
        let directories = pplx_space_node_runtime_core::artifact_store::release_directory_names(
            &release.release,
            release.digest(),
        );
        if !root.is_absolute()
            || !root.components().all(|part| {
                matches!(
                    part,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )
            })
            || !directories
                .iter()
                .any(|directory| root.file_name() == Some(std::ffi::OsStr::new(directory)))
            || firecracker != root.join("bin/firecracker")
            || jailer != root.join("bin/jailer")
        {
            return Err(Status::invalid_argument(
                "execution source paths must name the compiled release directory",
            ));
        }
        let expected = |name| {
            release
                .binaries
                .iter()
                .find(|file| file.path == name)
                .map(|file| compiled_verity_digest(file.sha256))
                .transpose()
                .map_err(|error| Status::failed_precondition(error.to_string()))?
                .flatten()
                .ok_or_else(|| Status::failed_precondition("runtime has no trusted verity digest"))
        };
        Ok(Self {
            root: root.to_owned(),
            jailer: expected("bin/jailer")?,
            firecracker: expected("bin/firecracker")?,
        })
    }

    pub(crate) fn open(&self) -> Result<OpenedRuntime, Status> {
        let root = &self.root;
        let firecracker = root.join("bin/firecracker");
        let firecracker_path = firecracker
            .to_str()
            .ok_or_else(|| Status::failed_precondition("execution cache paths must be UTF-8"))?
            .to_owned();
        Ok(OpenedRuntime {
            jailer: open(&root.join("bin/jailer"), self.jailer)?,
            _firecracker: open(&firecracker, self.firecracker)?,
            firecracker_path,
        })
    }
}

pub(crate) struct OpenedRuntime {
    pub(crate) jailer: SealedFile,
    _firecracker: SealedFile,
    pub(crate) firecracker_path: String,
}

pub(crate) fn open(path: &Path, expected: VerityDigest) -> Result<SealedFile, Status> {
    use std::os::unix::fs::MetadataExt;
    let parent = path
        .parent()
        .ok_or_else(|| Status::failed_precondition("execution file has no parent"))?;
    pplx_space_node_runtime_core::artifact_store::verify_execution_directory(parent, 0)
        .map_err(|error| source_status(SourceError::Directory(error)))?;
    let file = SealedFile::open(path, expected).map_err(|error| {
        source_status(SourceError::Verity {
            path: path.to_owned(),
            source: error,
        })
    })?;
    let metadata = file.file().metadata().map_err(|source| {
        source_status(SourceError::Metadata {
            path: path.to_owned(),
            source,
        })
    })?;
    if !metadata.is_file()
        || metadata.uid() != 0
        || metadata.mode() & 0o022 != 0
        || metadata.mode() & 0o111 == 0
    {
        return Err(Status::failed_precondition(
            "sealed runtime file has unsafe ownership or permissions",
        ));
    }
    Ok(file)
}

/// Repeated in the single-threaded launcher on the descriptor passed to exec.
pub(crate) fn open_catalog_executable(
    path: &Path,
    pins: pplx_space_node_runtime_core::authenticated_source::SourcePins,
) -> Result<pplx_space_node_runtime_core::authenticated_source::AuthenticatedFile, Status> {
    use pplx_space_node_runtime_core::authenticated_source::AuthenticatedFile;
    use std::os::unix::fs::MetadataExt;
    let file = AuthenticatedFile::open_catalog(path, pins).map_err(|error| {
        if error.is_transient() {
            Status::internal(error.to_string())
        } else {
            Status::failed_precondition(error.to_string())
        }
    })?;
    let metadata = file.file().metadata().map_err(|source| {
        source_status(SourceError::Metadata {
            path: path.to_owned(),
            source,
        })
    })?;
    if metadata.mode() & 0o111 == 0 {
        return Err(Status::failed_precondition(
            "authenticated runtime file is not executable",
        ));
    }
    Ok(file)
}

#[derive(Debug, thiserror::Error)]
enum SourceError {
    #[error(transparent)]
    Directory(VerifyError),
    #[error("verify sealed runtime {}: {source}", path.display())]
    Verity {
        path: PathBuf,
        #[source]
        source: VerityError,
    },
    #[error("inspect sealed runtime {}: {source}", path.display())]
    Metadata {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

fn source_status(error: SourceError) -> Status {
    let io = match &error {
        SourceError::Directory(VerifyError::Io { source, .. })
        | SourceError::Verity {
            source: VerityError::Io { source, .. },
            ..
        }
        | SourceError::Metadata { source, .. } => Some(source),
        _ => None,
    };
    if io.is_some_and(pplx_space_common_core::host::path_errors::host_path_io_error_is_transient) {
        Status::internal(error.to_string())
    } else {
        Status::failed_precondition(error.to_string())
    }
}

#[cfg(test)]
mod tests;

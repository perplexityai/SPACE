//! Authenticate the descriptor consumed by a privileged execution adapter.
//! SHA authentication relies on root-only publication and protected ancestry;
//! unlike fs-verity, it does not authenticate subsequent reads from storage.

use std::{
    fs::File,
    io::{BufRead, BufReader},
    os::unix::fs::MetadataExt,
    path::Path,
};

use pplx_space_common_core::types::Digest;

use crate::{
    artifact_store::{VerifyError, verify_execution_directory},
    verity::{self, VerityDigest, VerityError},
};

/// Both identities come from the consumer's compiled catalog.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct SourcePins {
    pub sha256: Digest,
    pub verity: VerityDigest,
}

#[derive(Clone, Copy, Debug)]
pub enum Authentication {
    CatalogSha256(Digest),
    FsVerity(VerityDigest),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error(transparent)]
    Directory(#[from] VerifyError),
    #[error(transparent)]
    Verity(#[from] VerityError),
    #[error(
        "execution source must be a regular, root-owned file without group or other write access"
    )]
    UnsafeFile,
    #[error("execution source SHA-256 {actual} differs from catalog digest {expected}")]
    Mismatch { expected: Digest, actual: Digest },
    #[error("{operation} execution source: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl AuthenticationError {
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Directory(error) => error.is_transient(),
            Self::Verity(VerityError::Io { source, .. }) | Self::Io { source, .. } => {
                pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(source)
            }
            _ => false,
        }
    }
}

/// Keeps the authenticated inode open through exec or linking into a guest root.
#[derive(Debug)]
pub struct AuthenticatedFile(File);

impl AuthenticatedFile {
    pub fn open(path: &Path, authentication: Authentication) -> Result<Self, AuthenticationError> {
        let file = open_protected(path, 0)?;
        authenticate(file, authentication)
    }

    /// A missing or invalid seal on a capable filesystem is a refusal. Only
    /// explicit lack of fs-verity support permits the content-hash fallback.
    pub fn open_catalog(path: &Path, pins: SourcePins) -> Result<Self, AuthenticationError> {
        let file = open_protected(path, 0)?;
        let authentication = catalog_authentication(verity::measure(&file), pins)?;
        authenticate(file, authentication)
    }

    pub fn file(&self) -> &File {
        &self.0
    }

    pub fn link_to(&self, destination: &Path) -> std::io::Result<()> {
        verity::link_file(&self.0, destination)
    }
}

fn open_protected(path: &Path, owner: u32) -> Result<File, AuthenticationError> {
    let parent = path.parent().ok_or(AuthenticationError::UnsafeFile)?;
    verify_execution_directory(parent, owner)?;
    let file = verity::open_readonly(path)?;
    let metadata = file.metadata().map_err(|source| AuthenticationError::Io {
        operation: "inspect",
        source,
    })?;
    if !metadata.is_file() || metadata.uid() != owner || metadata.mode() & 0o022 != 0 {
        return Err(AuthenticationError::UnsafeFile);
    }
    Ok(file)
}

fn catalog_authentication(
    measurement: Result<VerityDigest, VerityError>,
    pins: SourcePins,
) -> Result<Authentication, AuthenticationError> {
    match measurement {
        Ok(actual) if actual == pins.verity => Ok(Authentication::FsVerity(pins.verity)),
        Ok(actual) => Err(VerityError::Mismatch {
            expected: pins.verity,
            actual,
        }
        .into()),
        Err(VerityError::Unsupported) => Ok(Authentication::CatalogSha256(pins.sha256)),
        Err(error) => Err(error.into()),
    }
}

fn authenticate(
    file: File,
    authentication: Authentication,
) -> Result<AuthenticatedFile, AuthenticationError> {
    match authentication {
        Authentication::FsVerity(expected) => verity::check(&file, expected)?,
        Authentication::CatalogSha256(expected) => {
            let mut hash = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
            let mut reader = BufReader::with_capacity(64 * 1024, &file);
            loop {
                match reader.fill_buf() {
                    Ok([]) => break,
                    Ok(bytes) => {
                        hash.update(bytes);
                        let length = bytes.len();
                        reader.consume(length);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(source) => {
                        return Err(AuthenticationError::Io {
                            operation: "hash",
                            source,
                        });
                    }
                }
            }
            let mut bytes = [0; 32];
            bytes.copy_from_slice(hash.finish().as_ref());
            let actual = Digest::sha256(bytes);
            if actual != expected {
                return Err(AuthenticationError::Mismatch { expected, actual });
            }
        }
    }
    Ok(AuthenticatedFile(file))
}

#[cfg(test)]
mod tests;

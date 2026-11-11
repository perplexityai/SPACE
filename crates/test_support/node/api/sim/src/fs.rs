//! Per-sandbox filesystem backing for the simulator's Spaced file RPCs.
//!
//! Each sandbox gets a private subdirectory under `sandbox_fs_root`; guest
//! paths from RPC requests are mapped onto that subdirectory after rejecting
//! any path-traversal attempts. The host kernel enforces the file ops; the
//! simulator just plumbs proto messages through `tokio::fs`.

use std::path::{Component, Path, PathBuf};

use tokio::io::AsyncReadExt;

#[derive(Debug, thiserror::Error)]
pub enum FsError {
    #[error("guest path must be absolute (start with '/'): got {0:?}")]
    NotAbsolute(String),
    #[error("guest path contains '..' or other unsupported component: {0:?}")]
    UnsafePath(String),
    #[error("sandbox id must be a non-empty single path component: got {0:?}")]
    UnsafeSandboxId(String),
    #[error("io error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Reject any sandbox id that could escape the per-sandbox subdirectory when
/// joined as a path component. Production callers always pass UUIDs, but the
/// simulator's gRPC surface accepts arbitrary strings, so we defend at the
/// only place where the id becomes a filesystem path.
pub fn validate_sandbox_id(sandbox_id: &str) -> Result<(), FsError> {
    if sandbox_id.is_empty()
        || sandbox_id.contains('/')
        || sandbox_id.contains('\\')
        || sandbox_id.contains('\0')
        || sandbox_id == "."
        || sandbox_id == ".."
    {
        return Err(FsError::UnsafeSandboxId(sandbox_id.to_string()));
    }
    Ok(())
}

/// Per-sandbox filesystem backing for one simulator instance.
#[derive(Clone)]
pub struct SandboxFs {
    root: PathBuf,
}

impl SandboxFs {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve a `(sandbox_id, guest_path)` pair onto a host path strictly
    /// underneath this sandbox's subdirectory. Rejects path-traversal and
    /// non-absolute inputs so a buggy or malicious client cannot read or
    /// write outside the simulated sandbox tree.
    pub fn host_path(&self, sandbox_id: &str, guest_path: &str) -> Result<PathBuf, FsError> {
        validate_sandbox_id(sandbox_id)?;
        if !guest_path.starts_with('/') {
            return Err(FsError::NotAbsolute(guest_path.to_string()));
        }
        let trimmed = guest_path.trim_start_matches('/');
        let candidate = Path::new(trimmed);
        for component in candidate.components() {
            match component {
                Component::Normal(_) => {}
                _ => return Err(FsError::UnsafePath(guest_path.to_string())),
            }
        }
        Ok(self.root.join(sandbox_id).join(candidate))
    }

    pub fn sandbox_root(&self, sandbox_id: &str) -> Result<PathBuf, FsError> {
        validate_sandbox_id(sandbox_id)?;
        Ok(self.root.join(sandbox_id))
    }

    pub async fn ensure_sandbox_root(&self, sandbox_id: &str) -> Result<(), FsError> {
        let path = self.sandbox_root(sandbox_id)?;
        tokio::fs::create_dir_all(&path)
            .await
            .map_err(|source| FsError::Io {
                path: path.display().to_string(),
                source,
            })
    }

    pub async fn remove_sandbox_root(&self, sandbox_id: &str) -> Result<(), FsError> {
        let path = self.sandbox_root(sandbox_id)?;
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FsError::Io {
                path: path.display().to_string(),
                source,
            }),
        }
    }
}

/// Read up to `length` bytes starting at `offset` from `path`. None means "no
/// limit"; reads to EOF.
pub async fn read_range(path: &Path, offset: u64, length: Option<u64>) -> Result<Vec<u8>, FsError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|source| FsError::Io {
            path: path.display().to_string(),
            source,
        })?;
    if offset > 0 {
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|source| FsError::Io {
                path: path.display().to_string(),
                source,
            })?;
    }
    let mut buf = Vec::new();
    match length {
        Some(len) => {
            let mut limited = file.take(len);
            limited
                .read_to_end(&mut buf)
                .await
                .map_err(|source| FsError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
        }
        None => {
            file.read_to_end(&mut buf)
                .await
                .map_err(|source| FsError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests;

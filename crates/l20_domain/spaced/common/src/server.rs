#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tokio_stream::wrappers::UnixListenerStream;

#[derive(Debug, thiserror::Error)]
pub enum BindUdsError {
    #[error("failed to create unix socket parent directory {path}: {source}")]
    CreateParent {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to remove existing unix socket {path}: {source}")]
    RemoveExisting {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind unix socket {path}: {source}")]
    Bind {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[cfg(unix)]
    #[error("failed to set unix socket permissions on {path}: {source}")]
    SetPermissions {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub fn bind_uds(socket_path: &Path) -> Result<UnixListenerStream, BindUdsError> {
    bind_uds_with_mode(socket_path, None)
}

pub fn bind_uds_with_mode(
    socket_path: &Path,
    mode: Option<u32>,
) -> Result<UnixListenerStream, BindUdsError> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| BindUdsError::CreateParent {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    match std::fs::remove_file(socket_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(BindUdsError::RemoveExisting {
                path: socket_path.to_path_buf(),
                source,
            });
        }
    }
    let listener =
        tokio::net::UnixListener::bind(socket_path).map_err(|source| BindUdsError::Bind {
            path: socket_path.to_path_buf(),
            source,
        })?;
    #[cfg(unix)]
    if let Some(mode) = mode {
        std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(mode)).map_err(
            |source| BindUdsError::SetPermissions {
                path: socket_path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(UnixListenerStream::new(listener))
}

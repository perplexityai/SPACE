//! Binding node-local Unix sockets: atomic replacement through a staging path, permissions and
//! group ownership.

use super::*;

#[derive(Clone, Copy, Debug)]
pub enum SocketGroup {
    Required(&'static str),
    IfPresent(&'static str),
    BestEffort(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub struct UnixSocketOptions {
    pub(super) mode: u32,
    pub(super) group: Option<SocketGroup>,
}

#[derive(Debug, thiserror::Error)]
pub enum BindUnixSocketError {
    #[error("failed to create socket parent directory {path}: {source}")]
    CreateParent {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("refusing to remove non-socket path before binding unix socket: {path}")]
    ExistingPathIsNotSocket { path: std::path::PathBuf },
    #[error("failed to stat unix socket path {path}: {source}")]
    Stat {
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
    #[error("failed to bind unix socket staging path {path}: {source}")]
    BindStaging {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    StagingPath(#[from] UnixSocketStagingPathError),
    #[error("failed to publish unix socket {path} from staging path {staging_path}: {source}")]
    Publish {
        path: std::path::PathBuf,
        staging_path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Permissions(#[from] ApplyUnixSocketPermissionsError),
    #[error("failed to allocate a unique unix socket staging path after {attempts} attempts")]
    StagingPathExhausted {
        attempts: u8,
        #[source]
        source: Option<Box<BindUnixSocketError>>,
    },
}

impl BindUnixSocketError {
    pub(super) fn is_address_in_use(&self) -> bool {
        matches!(
            self,
            Self::Bind { source, .. } | Self::BindStaging { source, .. }
                if source.kind() == std::io::ErrorKind::AddrInUse
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UnixSocketStagingPathError {
    #[error("unix socket path has no file name: {path}")]
    MissingFileName { path: std::path::PathBuf },
    #[error("unix socket path has an empty file name: {path}")]
    EmptyFileName { path: std::path::PathBuf },
    #[error("unix socket staging nonce digit is out of range")]
    NonceDigitOutOfRange,
}

#[derive(Debug, thiserror::Error)]
pub enum ApplyUnixSocketPermissionsError {
    #[error("failed to chmod unix socket {path}: {source}")]
    Chmod {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to look up unix group '{name}': {source}")]
    LookupGroup {
        name: String,
        #[source]
        source: nix::Error,
    },
    #[error("group '{name}' not found for socket {path}")]
    GroupNotFound {
        name: &'static str,
        path: std::path::PathBuf,
    },
    #[error("failed to set socket group '{name}' on {path}: {source}")]
    SetGroup {
        name: &'static str,
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("failed to look up unix group '{name}': {source}")]
pub struct UnixGroupLookupError {
    pub(super) name: String,
    #[source]
    pub(super) source: nix::Error,
}

impl UnixSocketOptions {
    pub const fn new(mode: u32) -> Self {
        Self { mode, group: None }
    }

    pub const fn with_group(self, group: SocketGroup) -> Self {
        Self {
            mode: self.mode,
            group: Some(group),
        }
    }
}

pub fn bind_unix_socket(
    path: &Path,
    options: UnixSocketOptions,
) -> Result<UnixListener, BindUnixSocketError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| BindUnixSocketError::CreateParent {
            path: parent.to_path_buf(),
            source,
        })?;
    }

    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            return bind_unix_socket_replacement(path, options);
        }
        Ok(_) => {
            return Err(BindUnixSocketError::ExistingPathIsNotSocket {
                path: path.to_path_buf(),
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(BindUnixSocketError::Stat {
                path: path.to_path_buf(),
                source: error,
            });
        }
    }
    let listener = UnixListener::bind(path).map_err(|source| BindUnixSocketError::Bind {
        path: path.to_path_buf(),
        source,
    })?;
    apply_unix_socket_permissions(path, options)?;
    Ok(listener)
}

pub(super) fn bind_unix_socket_replacement(
    path: &Path,
    options: UnixSocketOptions,
) -> Result<UnixListener, BindUnixSocketError> {
    const MAX_STAGING_BIND_ATTEMPTS: u8 = 64;

    let seed = Uuid::now_v7();
    let mut last_collision = None;
    for attempt in 0..MAX_STAGING_BIND_ATTEMPTS {
        let staging_path = unix_socket_staging_path_for_attempt(path, seed, attempt)?;
        match bind_unix_socket_replacement_at(path, options, &staging_path) {
            Ok(listener) => return Ok(listener),
            Err(error) if error.is_address_in_use() => {
                last_collision = Some(error);
            }
            Err(error) => return Err(error),
        }
    }

    Err(BindUnixSocketError::StagingPathExhausted {
        attempts: MAX_STAGING_BIND_ATTEMPTS,
        source: last_collision.map(Box::new),
    })
}

pub(super) fn bind_unix_socket_replacement_at(
    path: &Path,
    options: UnixSocketOptions,
    staging_path: &Path,
) -> Result<UnixListener, BindUnixSocketError> {
    let mut owns_staging_path = false;
    let result = (|| {
        let listener = UnixListener::bind(staging_path).map_err(|source| {
            BindUnixSocketError::BindStaging {
                path: staging_path.to_path_buf(),
                source,
            }
        })?;
        owns_staging_path = true;
        apply_unix_socket_permissions(staging_path, options)?;
        std::fs::rename(staging_path, path).map_err(|source| BindUnixSocketError::Publish {
            path: path.to_path_buf(),
            staging_path: staging_path.to_path_buf(),
            source,
        })?;
        Ok(listener)
    })();

    if result.is_err()
        && owns_staging_path
        && let Err(error) = std::fs::remove_file(staging_path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            %error,
            staging_path = %staging_path.display(),
            "failed to clean up unix socket staging path"
        );
    }
    result
}

pub(super) const SOCKET_NONCE_ALPHABET: &[u8; 64] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz-";

pub(super) const SOCKET_STAGING_PREFIX: &[u8] = b".handoff-";

pub(super) fn unix_socket_staging_shape(path: &Path) -> Option<(usize, &'static [u8])> {
    let len = path.file_name()?.as_bytes().len().min(25);
    let prefix = if len >= SOCKET_STAGING_PREFIX.len() + 8 {
        SOCKET_STAGING_PREFIX
    } else {
        &[]
    };
    (len > 0).then_some((len, prefix))
}

pub(super) fn unix_socket_staging_path_for_attempt(
    path: &Path,
    seed: Uuid,
    attempt: u8,
) -> Result<std::path::PathBuf, UnixSocketStagingPathError> {
    let file_name =
        path.file_name()
            .ok_or_else(|| UnixSocketStagingPathError::MissingFileName {
                path: path.to_path_buf(),
            })?;
    let file_name_len = file_name.as_bytes().len();
    if file_name_len == 0 {
        return Err(UnixSocketStagingPathError::EmptyFileName {
            path: path.to_path_buf(),
        });
    }

    let (staging_file_name_len, prefix) = unix_socket_staging_shape(path).ok_or_else(|| {
        UnixSocketStagingPathError::EmptyFileName {
            path: path.to_path_buf(),
        }
    })?;
    let nonce_len = staging_file_name_len - prefix.len();
    let mut staging_file_name = Vec::with_capacity(staging_file_name_len);
    staging_file_name.extend_from_slice(prefix);

    let mut nonce = seed.as_u128().wrapping_add(u128::from(attempt));
    for _ in 0..nonce_len {
        let digit = SOCKET_NONCE_ALPHABET
            .get((nonce & 0x3f) as usize)
            .copied()
            .ok_or(UnixSocketStagingPathError::NonceDigitOutOfRange)?;
        staging_file_name.push(digit);
        nonce >>= 6;
    }

    Ok(path.with_file_name(std::ffi::OsString::from_vec(staging_file_name)))
}

pub fn apply_unix_socket_permissions(
    path: &Path,
    options: UnixSocketOptions,
) -> Result<(), ApplyUnixSocketPermissionsError> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(options.mode)).map_err(
        |source| ApplyUnixSocketPermissionsError::Chmod {
            path: path.to_path_buf(),
            source,
        },
    )?;

    if let Some(group) = options.group {
        apply_socket_group(path, group)?;
    }

    Ok(())
}

pub(super) fn apply_socket_group(
    path: &Path,
    group: SocketGroup,
) -> Result<(), ApplyUnixSocketPermissionsError> {
    let group_name = match group {
        SocketGroup::Required(name)
        | SocketGroup::IfPresent(name)
        | SocketGroup::BestEffort(name) => name,
    };

    let gid = nix::unistd::Group::from_name(group_name)
        .map(|group| group.map(|group| group.gid.as_raw()))
        .map_err(|source| ApplyUnixSocketPermissionsError::LookupGroup {
            name: group_name.to_owned(),
            source,
        })?;

    let Some(gid) = gid else {
        return match group {
            SocketGroup::Required(name) => Err(ApplyUnixSocketPermissionsError::GroupNotFound {
                name,
                path: path.to_path_buf(),
            }),
            SocketGroup::IfPresent(name) | SocketGroup::BestEffort(name) => {
                tracing::warn!(
                    socket_path = %path.display(),
                    socket_group = name,
                    "socket group missing; socket group unchanged"
                );
                Ok(())
            }
        };
    };
    if let Err(err) = std::os::unix::fs::chown(path, None, Some(gid)) {
        return match group {
            SocketGroup::Required(name) | SocketGroup::IfPresent(name) => {
                Err(ApplyUnixSocketPermissionsError::SetGroup {
                    name,
                    path: path.to_path_buf(),
                    source: err,
                })
            }
            SocketGroup::BestEffort(name) => {
                tracing::warn!(
                    socket_path = %path.display(),
                    socket_group = name,
                    error = %err,
                    "failed to set socket group; socket group unchanged"
                );
                Ok(())
            }
        };
    }

    Ok(())
}

pub fn unix_group_exists(name: &str) -> Result<bool, UnixGroupLookupError> {
    unix_group_gid(name).map(|gid| gid.is_some())
}

pub(super) fn unix_group_gid(name: &str) -> Result<Option<libc::gid_t>, UnixGroupLookupError> {
    nix::unistd::Group::from_name(name)
        .map(|group| group.map(|group| group.gid.as_raw()))
        .map_err(|source| UnixGroupLookupError {
            name: name.to_owned(),
            source,
        })
}

//! `UnixSocketPath`, a validated absolute Unix domain socket path, and the
//! error its parser returns.
//!
//! The type needs only the standard library and `thiserror`, so it lives in
//! its own crate: clients that pass a socket path to a node-local service can
//! name it without depending on `pplx-space-common-svc` and the service host
//! behind it.

use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::net::SocketAddr as UnixSocketAddr;

/// Error returned when a Unix socket path is not usable as a pathname socket.
///
/// ```
/// # use pplx_space_unix_socket_path::UnixSocketPath;
/// let err = UnixSocketPath::parse("relative.sock").unwrap_err();
/// assert_eq!(
///     err.to_string(),
///     "unix socket path must be absolute, non-empty, NUL-free, and fit platform limits"
/// );
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unix socket path must be absolute, non-empty, NUL-free, and fit platform limits")]
pub struct UnixSocketPathParseError;

/// Validated absolute Unix domain socket path.
///
/// ```
/// # use pplx_space_unix_socket_path::UnixSocketPath;
/// let path = UnixSocketPath::parse("/run/space/node.sock").unwrap();
/// assert_eq!(path.as_path().to_str(), Some("/run/space/node.sock"));
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UnixSocketPath(PathBuf);

impl UnixSocketPath {
    /// Parses an absolute Unix socket path that fits the platform `sun_path`.
    ///
    /// ```
    /// # use pplx_space_unix_socket_path::UnixSocketPath;
    /// assert!(UnixSocketPath::parse("/run/space/api.sock").is_ok());
    /// assert!(UnixSocketPath::parse("api.sock").is_err());
    /// assert!(UnixSocketPath::parse(format!("/tmp/{}.sock", "a".repeat(120))).is_err());
    /// ```
    pub fn parse(value: impl Into<PathBuf>) -> Result<Self, UnixSocketPathParseError> {
        let value = value.into();
        if value.as_os_str().is_empty() || !value.is_absolute() {
            return Err(UnixSocketPathParseError);
        }
        #[cfg(unix)]
        UnixSocketAddr::from_pathname(&value).map_err(|_| UnixSocketPathParseError)?;
        Ok(Self(value))
    }

    /// Returns the validated socket path.
    ///
    /// ```
    /// # use pplx_space_unix_socket_path::UnixSocketPath;
    /// let path = UnixSocketPath::parse("/run/space/bridge.sock").unwrap();
    /// assert_eq!(path.as_path().to_str(), Some("/run/space/bridge.sock"));
    /// ```
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

#[cfg(test)]
mod tests;

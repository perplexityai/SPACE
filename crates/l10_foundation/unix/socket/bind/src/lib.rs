//! Binding node-local Unix sockets: `bind_unix_socket` and
//! `apply_unix_socket_permissions`, their options (`UnixSocketOptions`,
//! `SocketGroup`) and errors, and `unix_group_exists`.
//!
//! A rebind replaces a live socket by binding a staging path and renaming it
//! over the published one, so the path never disappears while a successor
//! takes it over. It is its own crate so a service can bind its sockets
//! without the rest of `pplx-space-common-svc`, which re-exports these items
//! from `bootstrap`. It does not join `pplx-space-unix-socket-path`, which
//! needs only std and thiserror: binding needs Tokio, nix and libc.

use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::Path;

use tokio::net::UnixListener;
use uuid::Uuid;

mod unix_socket;

pub use unix_socket::*;

#[cfg(test)]
mod service_mutex_handoff_tests;
#[cfg(test)]
mod tests;

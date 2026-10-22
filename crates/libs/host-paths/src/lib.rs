//! Well-known SPACE node and guest filesystem paths.
//!
//! [`host::paths`] names the node's service sockets, databases, state and
//! cache directories and installed binaries, and builds the per-sandbox and
//! per-template paths from string identifiers. [`guest::paths`] and
//! [`guest::network`] name the guest's socket paths and its fixed link
//! addresses.
//!
//! These are plain strings and functions over `&str`, so they are their own
//! crate: a client that needs only a socket path can name it without
//! depending on `pplx-space-common-core` and its domain types.
//! `pplx-space-common-core` re-exports every item at its old path
//! (`host::linux::paths`, `guest::linux` and `types::node_fs`).

pub mod guest;
pub mod host;
mod node_fs;

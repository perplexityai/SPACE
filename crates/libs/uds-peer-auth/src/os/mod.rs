//! Platform-gated peer process inspection.
//!
//! Mirrors the `host::linux` / `host::other` split in
//! `pplx-space-common-core`: every host has exactly one
//! `read_peer_process` implementation, selected at compile time.
//! The Linux path reads `/proc/<pid>/{status,exe}`; every other host
//! returns `io::ErrorKind::Unsupported` so opt-in process checks
//! fail closed.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(super) use linux::{current_process_cap_effective, read_peer_process};

#[cfg(not(target_os = "linux"))]
mod other;
#[cfg(not(target_os = "linux"))]
pub(super) use other::read_peer_process;

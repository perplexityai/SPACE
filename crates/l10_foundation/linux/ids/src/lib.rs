//! Linux host and vsock IDs (UID, PID, PID limit, network slot, netns and
//! interface names, vsock CID and port) with kernel-boundary validation, plus
//! SPACE's six-digit network-slot namespace and 64-byte netns-name limit.
//!
//! They are their own crate so that code can name them without depending on
//! `pplx-space-common-core`: import them from here.

pub mod host_linux;
pub use host_linux::*;
pub mod transport_vsock;
pub use transport_vsock::*;

//! Local (non-Firecracker) node-manager runtime for the kind smoke harness.
//!
//! Implements `space.node.v1.NodeManager` with guest-as-`systemd-nspawn`,
//! reflink rootfs cloning, synthetic networking, file-based snapshots, and
//! btrfs RPCs that decline to force the `Full` ext4 codec — so sandbox-manager,
//! node-api, volume-manager, and the spaced bridge run unchanged on a laptop or
//! CI box with no KVM, no btrfs, and no `NET_ADMIN`.

pub mod config;
mod ext4_lengths;
pub mod paths;
pub mod process;
pub mod service;
pub mod snapshot;
pub mod state;

pub use config::Config;
pub use service::LocalNodeManager;

/// Per-sandbox guest (nspawn) log file, under the sandbox run dir.
pub const NSPAWN_LOG: &str = "nspawn.log";
/// Per-sandbox spaced-host log file, under the sandbox run dir.
pub const SPACED_HOST_LOG: &str = "spaced-host.log";

#[cfg(test)]
mod tests;

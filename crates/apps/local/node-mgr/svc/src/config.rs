//! Runtime configuration for the local (non-Firecracker) node-manager.

use std::path::PathBuf;

use pplx_space_common_svc::core::host::linux::paths::NODE_MANAGER_SOCKET;

/// Fallback seed rootfs image used when `PrepareSandbox` does not carry a usable
/// `template_dir`. Baked into the runtime's image at build time.
pub const DEFAULT_SEED_ROOTFS: &str = "/space/templates/seed/rootfs.ext4";

/// Effective configuration resolved from CLI args + environment.
#[derive(Clone, Debug)]
pub struct Config {
    /// node-manager gRPC Unix socket to bind.
    pub socket: PathBuf,
    /// Seed rootfs image used when no template build dir is available.
    pub seed_rootfs: PathBuf,
}

impl Config {
    /// Build the config from the socket override and the environment.
    pub fn resolve(socket: Option<PathBuf>) -> Self {
        Self {
            socket: socket.unwrap_or_else(|| PathBuf::from(NODE_MANAGER_SOCKET)),
            seed_rootfs: PathBuf::from(DEFAULT_SEED_ROOTFS),
        }
    }
}

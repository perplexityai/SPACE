//! SPACE Linux guest contracts.

/// SPACE guest Linux network contract.
pub mod network {
    pub const PRIMARY_INTERFACE: &str = "eth0";
    pub use pplx_space_net_common::guest_link::{
        GATEWAY_IPV4_ADDR, GATEWAY_IPV4_CIDR, IPV4_ADDR, IPV4_NETMASK,
    };
}

/// SPACE guest Linux filesystem paths.
pub mod paths {
    use std::path::PathBuf;

    pub const SPACED_GUEST_SOCKET_DIR: &str = "/run";

    pub const SPACED_GUEST_SOCKET_FILE: &str = "spaced.sock";
    pub const SPACED_BRIDGE_GUEST_SOCKET_FILE: &str = "spaced-bridge-guest.sock";
    pub const SPACED_BRIDGE_HOST_SOCKET_FILE: &str = "spaced-bridge-host.sock";

    /// Returns the default guest-side UDS path for a socket file.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::guest::linux::paths::default_guest_uds_path;
    /// assert!(default_guest_uds_path("spaced.sock").ends_with("spaced.sock"));
    /// ```
    pub fn default_guest_uds_path(socket_file: &str) -> PathBuf {
        default_uds_path(SPACED_GUEST_SOCKET_DIR, socket_file)
    }

    /// Returns a platform-appropriate default UDS path.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::guest::linux::paths::default_uds_path;
    /// assert!(default_uds_path("/run", "svc.sock").ends_with("svc.sock"));
    /// ```
    pub fn default_uds_path(linux_socket_dir: &str, socket_file: &str) -> PathBuf {
        if cfg!(target_os = "linux") {
            PathBuf::from(linux_socket_dir)
        } else {
            std::env::temp_dir()
        }
        .join(socket_file)
    }
}

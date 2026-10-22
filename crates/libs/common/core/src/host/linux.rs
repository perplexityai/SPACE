//! SPACE Linux node host contracts.

pub mod gateway;

/// Signal the process pinned by a pidfd, including Linux real-time signals.
/// The kernel rejects descriptors that are not pidfds. The protocol accepts
/// numeric signals beyond nix's named variants; rustix also requires an unsafe
/// constructor for these values, with additional libc-reserved-signal constraints.
#[cfg(target_os = "linux")]
pub fn signal_pidfd(
    pidfd: impl std::os::fd::AsFd,
    signal: crate::types::ProcessSignal,
) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: the descriptor stays borrowed, ProcessSignal validates the
    // signal number, and a null siginfo is permitted with flags = 0.
    let result = unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            pidfd.as_fd().as_raw_fd(),
            signal.as_i32(),
            std::ptr::null::<libc::siginfo_t>(),
            0u32,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// File-descriptor limit applied to Firecracker and sandbox sidecar processes.
pub const SANDBOX_PROCESS_NOFILE_LIMIT: u64 = 4096;

/// SPACE node Linux filesystem paths, defined in `pplx-space-host-paths`.
pub use pplx_space_host_paths::host::paths;

/// SPACE node Linux users and groups.
///
/// The canonical source is `infra/space/identity/node-identities.sh`. This
/// module is generated at build time so Rust services and CLIs use the same
/// identity values as shell, Docker, Packer, and Helm deployment paths.
pub mod identities {
    include!(concat!(env!("OUT_DIR"), "/linux_identities.rs"));
}

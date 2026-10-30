//! Runtime control owned by node-manager. Lifecycle services do not receive a
//! VMM's host-path-capable control socket or interpret provider-specific states.
use pplx_space_common_core::types::RuntimeKind;
use std::{future::Future, path::PathBuf, pin::Pin};

#[cfg(feature = "host-runtime")]
mod cloud_hypervisor;

pub use crate::vm_state::VmState;

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("runtime {0} has no control adapter")]
    UnsupportedRuntime(RuntimeKind),
    #[error("{operation} is not supported by runtime {runtime}")]
    UnsupportedOperation {
        runtime: RuntimeKind,
        operation: &'static str,
    },
    #[error("runtime control requires a pinned process identity")]
    MissingPeer,
    #[error(transparent)]
    Firecracker(#[from] crate::fc_api::FcApiError),
    #[cfg(feature = "host-runtime")]
    #[error(transparent)]
    CloudHypervisor(#[from] cloud_hypervisor::ApiError),
}

impl ControlError {
    /// A mutation may have executed when its deadline expires. Callers must
    /// reconcile or terminate the fenced execution, never retry it blindly.
    pub fn is_timeout(&self) -> bool {
        #[cfg(feature = "host-runtime")]
        if matches!(
            self,
            Self::CloudHypervisor(cloud_hypervisor::ApiError::Timeout)
        ) {
            return true;
        }
        false
    }
}

/// Bound memory copies using a conservative 64 MiB/s budget plus setup time.
/// This is an operation deadline, not permission to replay after expiration.
#[cfg(feature = "host-runtime")]
fn memory_state_timeout(memory_bytes: u64) -> std::time::Duration {
    std::time::Duration::from_secs(
        memory_bytes
            .div_ceil(64 * 1024 * 1024)
            .saturating_add(60)
            .clamp(120, 900),
    )
}

pub type ControlFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ControlError>> + Send + 'a>>;

/// Fixed runtime-owned workspaces. This is not a caller-supplied host path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateWorkspace {
    Plain,
    Encrypted,
}

/// Control is independent of process launch: an in-process runtime can implement
/// this interface without pretending to expose an HTTP socket or jailer.
pub trait RuntimeControl: Send + Sync {
    fn state(&self) -> ControlFuture<'_, VmState>;
    /// Change resident execution state once; callers reconcile ambiguous results.
    fn set_paused(&self, paused: bool) -> ControlFuture<'_, ()>;
    /// Capture/restore the runtime-owned state workspace. These mutations are
    /// never replayed after transport uncertainty.
    fn capture_state(&self, workspace: StateWorkspace, memory_bytes: u64) -> ControlFuture<'_, ()>;
    fn restore_state(&self, workspace: StateWorkspace, memory_bytes: u64) -> ControlFuture<'_, ()>;
    /// Configure and boot a newly launched idle runtime; never replay after uncertainty.
    fn boot<'a>(&'a self, configuration: &'a crate::boot::StandaloneBoot) -> ControlFuture<'a, ()>;
    fn request_guest_shutdown(&self) -> ControlFuture<'_, ()>;
}

/// Construct only in the process authorized to own this runtime's control plane.
/// The socket is a locally derived adapter locator, never a tenant input.
#[cfg(feature = "host-runtime")]
pub fn connect(
    runtime: RuntimeKind,
    socket: PathBuf,
    peer: Option<crate::launch::ProcessObserver>,
) -> Result<Box<dyn RuntimeControl>, ControlError> {
    match runtime {
        RuntimeKind::Firecracker => Ok(Box::new(crate::fc_api::FcApiClient::new(socket))),
        RuntimeKind::CloudHypervisor => Ok(Box::new(cloud_hypervisor::Client::new(
            socket,
            peer.ok_or(ControlError::MissingPeer)?,
        ))),
        RuntimeKind::Gvisor => Err(ControlError::UnsupportedRuntime(runtime)),
    }
}

impl RuntimeControl for crate::fc_api::FcApiClient {
    fn capture_state(
        &self,
        _workspace: StateWorkspace,
        _memory_bytes: u64,
    ) -> ControlFuture<'_, ()> {
        Box::pin(async {
            Err(ControlError::UnsupportedOperation {
                runtime: RuntimeKind::Firecracker,
                operation: "state workspace capture",
            })
        })
    }
    fn restore_state(
        &self,
        _workspace: StateWorkspace,
        _memory_bytes: u64,
    ) -> ControlFuture<'_, ()> {
        Box::pin(async {
            Err(ControlError::UnsupportedOperation {
                runtime: RuntimeKind::Firecracker,
                operation: "state workspace restore",
            })
        })
    }
    fn set_paused(&self, paused: bool) -> ControlFuture<'_, ()> {
        Box::pin(async move {
            if paused {
                self.pause().await?;
            } else {
                self.resume().await?;
            }
            Ok(())
        })
    }
    fn state(&self) -> ControlFuture<'_, VmState> {
        Box::pin(async move {
            use crate::fc_api::FirecrackerVmState;
            Ok(match self.get_vm_state().await? {
                FirecrackerVmState::NotStarted => VmState::NotStarted,
                FirecrackerVmState::Running => VmState::Running,
                FirecrackerVmState::Paused => VmState::Paused,
            })
        })
    }
    fn boot<'a>(
        &'a self,
        _configuration: &'a crate::boot::StandaloneBoot,
    ) -> ControlFuture<'a, ()> {
        Box::pin(async {
            Err(ControlError::UnsupportedOperation {
                runtime: RuntimeKind::Firecracker,
                operation: "API cold boot",
            })
        })
    }
    fn request_guest_shutdown(&self) -> ControlFuture<'_, ()> {
        Box::pin(async {
            Err(ControlError::UnsupportedOperation {
                runtime: RuntimeKind::Firecracker,
                operation: "guest shutdown",
            })
        })
    }
}

/// Linux standalone adapters keep control endpoints in their own isolation root.
/// The shared control trait does not require a socket-backed implementation.
pub fn socket_path(
    runtime: RuntimeKind,
    sandbox_id: pplx_space_common_core::types::SandboxId,
) -> Result<PathBuf, ControlError> {
    let filename = match runtime {
        RuntimeKind::Firecracker => "firecracker.sock",
        RuntimeKind::CloudHypervisor => "control.sock",
        RuntimeKind::Gvisor => return Err(ControlError::UnsupportedRuntime(runtime)),
    };
    Ok(isolation_root(runtime, &sandbox_id.to_string())?.join(filename))
}

/// Filesystem layout for the installed standalone VM adapters. Other runtime
/// implementations need not expose a chroot or a Unix guest transport.
pub fn isolation_root(runtime: RuntimeKind, sandbox_id: &str) -> Result<PathBuf, ControlError> {
    match runtime {
        RuntimeKind::Firecracker => Ok(pplx_space_common_core::types::node_fs::jail_root(
            sandbox_id,
        )),
        RuntimeKind::CloudHypervisor => Ok(std::path::Path::new(
            pplx_space_common_core::host::linux::paths::FIRECRACKER_JAIL_DIR,
        )
        .join(runtime.as_str())
        .join(sandbox_id)
        .join("root")),
        RuntimeKind::Gvisor => Err(ControlError::UnsupportedRuntime(runtime)),
    }
}

pub fn guest_socket_path(runtime: RuntimeKind, sandbox_id: &str) -> Result<PathBuf, ControlError> {
    Ok(isolation_root(runtime, sandbox_id)?.join("v.sock"))
}

/// Expected process and in-namespace listener for a standalone guest transport.
/// These checks supplement the lease, cgroup, process start time and socket inode
/// proofs; a caller cannot select a different executable or endpoint.
pub fn guest_endpoint_identity(
    runtime: RuntimeKind,
) -> Result<(&'static str, &'static std::path::Path), ControlError> {
    let executable = match runtime {
        RuntimeKind::Firecracker => "firecracker",
        RuntimeKind::CloudHypervisor => "cloud-hypervisor",
        RuntimeKind::Gvisor => return Err(ControlError::UnsupportedRuntime(runtime)),
    };
    Ok((executable, std::path::Path::new("/v.sock")))
}

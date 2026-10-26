//! Runtime control owned by node-manager. Lifecycle services do not receive a
//! VMM's host-path-capable control socket or interpret provider-specific states.
use pplx_space_common_core::types::RuntimeKind;
use std::{future::Future, path::PathBuf, pin::Pin};

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
    #[error(transparent)]
    Firecracker(#[from] crate::fc_api::FcApiError),
    #[error(transparent)]
    CloudHypervisor(#[from] cloud_hypervisor::ApiError),
}

pub type ControlFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ControlError>> + Send + 'a>>;

/// Control is independent of process launch: an in-process runtime can implement
/// this interface without pretending to expose an HTTP socket or jailer.
pub trait RuntimeControl: Send + Sync {
    fn state(&self) -> ControlFuture<'_, VmState>;
    fn pause(&self) -> ControlFuture<'_, ()>;
    fn resume(&self) -> ControlFuture<'_, ()>;
}

/// Construct only in the process authorized to own this runtime's control plane.
/// The socket is a locally derived adapter locator, never a tenant input.
pub fn connect(
    runtime: RuntimeKind,
    socket: PathBuf,
) -> Result<Box<dyn RuntimeControl>, ControlError> {
    match runtime {
        RuntimeKind::Firecracker => Ok(Box::new(crate::fc_api::FcApiClient::new(socket))),
        RuntimeKind::CloudHypervisor => Ok(Box::new(cloud_hypervisor::Client::new(socket))),
        RuntimeKind::Gvisor => Err(ControlError::UnsupportedRuntime(runtime)),
    }
}

impl RuntimeControl for crate::fc_api::FcApiClient {
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
    fn pause(&self) -> ControlFuture<'_, ()> {
        Box::pin(async move { Ok(crate::fc_api::FcApiClient::pause(self).await?) })
    }
    fn resume(&self) -> ControlFuture<'_, ()> {
        Box::pin(async move { Ok(crate::fc_api::FcApiClient::resume(self).await?) })
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
    Ok(
        std::path::Path::new(pplx_space_common_core::host::linux::paths::FIRECRACKER_JAIL_DIR)
            .join(runtime.as_str())
            .join(sandbox_id.to_string())
            .join("root")
            .join(filename),
    )
}

//! Firecracker host-adapter input. Runtime dispatch rejects unknown adapters
//! before acquiring execution ownership or creating a process.
//!
//! Generic launches always carry the sandbox identity at the top level. Protected
//! launches repeat that same identity in `verified_runtime`; disagreement is a
//! refusal.

use crate::proto::LaunchRuntimeRequest;
use pplx_space_execution_common::proto::RuntimeKind;

#[derive(Debug, thiserror::Error)]
pub enum LaunchContractError {
    #[error("runtime adapter {0} does not support this launch contract")]
    UnsupportedRuntime(i32),
    #[error("Firecracker launch requires its containment executable")]
    MissingLauncher,
    #[error("verified launch identity differs from the request identity")]
    IdentityMismatch,
}

impl TryFrom<LaunchRuntimeRequest> for FirecrackerLaunch {
    type Error = LaunchContractError;

    fn try_from(request: LaunchRuntimeRequest) -> Result<Self, Self::Error> {
        if request.runtime != RuntimeKind::Firecracker as i32 {
            return Err(LaunchContractError::UnsupportedRuntime(request.runtime));
        }
        if request
            .verified_runtime
            .as_ref()
            .is_some_and(|input| input.sandbox_id != request.sandbox_id)
        {
            return Err(LaunchContractError::IdentityMismatch);
        }
        let launcher = request
            .launcher_binary
            .filter(|path| !path.is_empty())
            .ok_or(LaunchContractError::MissingLauncher)?;
        Ok(Self {
            sandbox_id: if request.verified_runtime.is_some() {
                String::new()
            } else {
                request.sandbox_id
            },
            execution_id: request.execution_id,
            jailer_bin: launcher,
            firecracker_bin: request.runtime_binary,
            chroot_base_dir: request.workspace_root,
            ns_path: request.network_namespace,
            uid: request.uid,
            gid: request.gid,
            fc_config_path: request.configuration_path,
            fc_log_path: request.log_path,
            fc_console_path: request.console_path,
            fc_metrics_path: request.metrics_path,
            cgroup_limits: request.resource_limits,
            verified_runtime: request.verified_runtime,
        })
    }
}

impl From<FirecrackerLaunch> for LaunchRuntimeRequest {
    fn from(request: FirecrackerLaunch) -> Self {
        Self {
            sandbox_id: request.verified_runtime.as_ref().map_or_else(
                || request.sandbox_id.clone(),
                |input| input.sandbox_id.clone(),
            ),
            execution_id: request.execution_id,
            runtime: RuntimeKind::Firecracker as i32,
            runtime_binary: request.firecracker_bin,
            launcher_binary: Some(request.jailer_bin),
            workspace_root: request.chroot_base_dir,
            network_namespace: request.ns_path,
            uid: request.uid,
            gid: request.gid,
            configuration_path: request.fc_config_path,
            log_path: request.fc_log_path,
            console_path: request.fc_console_path,
            metrics_path: request.fc_metrics_path,
            resource_limits: request.cgroup_limits,
            verified_runtime: request.verified_runtime,
        }
    }
}

#[cfg(test)]
mod tests;

/// Validated by the FC adapter before process ownership is acquired. This is
/// host implementation data, not a second wire contract.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FirecrackerLaunch {
    pub sandbox_id: String,
    pub execution_id: String,
    pub jailer_bin: String,
    pub firecracker_bin: String,
    pub chroot_base_dir: String,
    pub ns_path: String,
    pub uid: u32,
    pub gid: u32,
    pub fc_config_path: String,
    pub fc_log_path: String,
    pub fc_console_path: String,
    pub fc_metrics_path: String,
    pub cgroup_limits: Option<crate::proto::CgroupLimits>,
    pub verified_runtime: Option<crate::proto::VerifiedRuntimeLaunch>,
}

//! Firecracker host-adapter input. Runtime dispatch rejects unknown adapters
//! before acquiring execution ownership or creating a process.
//!
//! Generic launches always carry the sandbox identity at the top level. Protected
//! launches repeat that same identity in `verified_runtime`; disagreement is a
//! refusal.

use crate::proto::{ArtifactIntegrityRequirement, LaunchRuntimeRequest};
use pplx_space_common_core::types::{DigestParseError, EnvironmentReference};
use pplx_space_execution_common::proto::RuntimeKind;

#[derive(Debug, thiserror::Error)]
pub enum LaunchContractError {
    #[error("unsupported artifact integrity requirement {0}")]
    UnsupportedIntegrityRequirement(i32),
    #[error("invalid execution environment reference: {0}")]
    InvalidEnvironmentReference(#[from] DigestParseError),
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
        let environment_reference = request
            .verified_runtime
            .map(|input| {
                if input.integrity_requirement
                    != ArtifactIntegrityRequirement::AuthenticatedImmutableSources as i32
                {
                    return Err(LaunchContractError::UnsupportedIntegrityRequirement(
                        input.integrity_requirement,
                    ));
                }
                input
                    .environment_reference
                    .parse()
                    .map_err(LaunchContractError::from)
            })
            .transpose()?;
        Ok(Self {
            sandbox_id: request.sandbox_id,
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
            environment_reference,
        })
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
    pub environment_reference: Option<EnvironmentReference>,
}

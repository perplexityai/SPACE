//! Firecracker host-adapter input. Runtime dispatch rejects unknown adapters
//! before acquiring execution ownership or creating a process.
//!
//! Generic launches always carry the sandbox identity at the top level. Protected
//! launches repeat that same identity in `verified_runtime`; disagreement is a
//! refusal.

use crate::proto::{ArtifactIntegrityRequirement, LaunchRuntimeRequest};
use pplx_space_common_core::types::{
    DigestParseError, EnvironmentReference, NodePathError, NodeSpacePath,
    SandboxActivationAttemptId, SandboxId, SandboxIdParseError,
};
use pplx_space_execution_common::proto::RuntimeKind;

#[derive(Debug, thiserror::Error)]
pub enum LaunchContractError {
    #[error("invalid sandbox identity: {0}")]
    Sandbox(#[from] SandboxIdParseError),
    #[error("runtime execution identity is missing")]
    MissingExecution,
    #[error("invalid execution identity: {0}")]
    Execution(#[source] <SandboxActivationAttemptId as std::str::FromStr>::Err),
    #[error("invalid {field}: {source}")]
    Path {
        field: &'static str,
        #[source]
        source: NodePathError,
    },
    #[error("{0} contains a NUL byte")]
    NulPath(&'static str),
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
        if request.execution_id.is_empty() {
            return Err(LaunchContractError::MissingExecution);
        }
        Ok(Self {
            sandbox_id: request.sandbox_id.parse()?,
            execution_id: request
                .execution_id
                .parse()
                .map_err(LaunchContractError::Execution)?,
            jailer_bin: LaunchPath::parse("jailer_bin", launcher)?,
            firecracker_bin: LaunchPath::parse("firecracker_bin", request.runtime_binary)?,
            chroot_base_dir: LaunchPath::parse("chroot_base_dir", request.workspace_root)?,
            ns_path: LaunchPath::parse("ns_path", request.network_namespace)?,
            uid: request.uid,
            gid: request.gid,
            fc_config_path: LaunchPath::optional("fc_config_path", request.configuration_path)?,
            fc_log_path: LaunchPath::optional("fc_log_path", request.log_path)?,
            fc_console_path: LaunchPath::optional("fc_console_path", request.console_path)?,
            fc_metrics_path: LaunchPath::optional("fc_metrics_path", request.metrics_path)?,
            cgroup_limits: request.resource_limits,
            environment_reference,
        })
    }
}

#[cfg(test)]
mod tests;

/// Validated by the FC adapter before process ownership is acquired. This is
/// host implementation data, not a second wire contract.
#[derive(Clone, Debug, PartialEq)]
pub struct FirecrackerLaunch {
    pub sandbox_id: SandboxId,
    pub execution_id: SandboxActivationAttemptId,
    pub jailer_bin: LaunchPath,
    pub firecracker_bin: LaunchPath,
    pub chroot_base_dir: LaunchPath,
    pub ns_path: LaunchPath,
    pub uid: u32,
    pub gid: u32,
    pub fc_config_path: Option<LaunchPath>,
    pub fc_log_path: Option<LaunchPath>,
    pub fc_console_path: Option<LaunchPath>,
    pub fc_metrics_path: Option<LaunchPath>,
    pub cgroup_limits: Option<crate::proto::CgroupLimits>,
    pub environment_reference: Option<EnvironmentReference>,
}

/// UTF-8 absolute path with no parent traversal or NUL bytes. This proves path
/// syntax, not inode trust: protected launches still authenticate opened files.
#[derive(Clone, Debug, PartialEq)]
pub struct LaunchPath(String);

impl LaunchPath {
    fn parse(field: &'static str, value: String) -> Result<Self, LaunchContractError> {
        NodeSpacePath::parse(value.as_str())
            .map_err(|source| LaunchContractError::Path { field, source })?;
        if value.contains('\0') {
            return Err(LaunchContractError::NulPath(field));
        }
        Ok(Self(value))
    }

    fn optional(field: &'static str, value: String) -> Result<Option<Self>, LaunchContractError> {
        if value.is_empty() {
            Ok(None)
        } else {
            Self::parse(field, value).map(Some)
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn as_path(&self) -> &std::path::Path {
        std::path::Path::new(&self.0)
    }
}

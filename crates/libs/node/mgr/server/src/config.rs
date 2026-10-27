//! Immutable settings resolved before the daemon becomes active.
//!
//! Parsers stay with their domain owner; this is the single startup composition
//! point. Loop-device sizing retains its separate live-pool policy. The service
//! is handed its memory-admission sources, runtime-supervisor client and
//! node-status dirty sink from here rather than constructing them, so tests
//! can substitute their own.

use std::path::PathBuf;

use pplx_space_common_core::host::{disk_admission::CacheDiskAdmissionConfig, linux::paths};

#[derive(Debug)]
pub(crate) struct ServiceSettings {
    pub disk_admission: CacheDiskAdmissionConfig,
    pub lifecycle_limiter: crate::lifecycle_limiter::LifecycleLimiter,
    pub runtime_paths: RuntimePaths,
    pub guest_console_logs: bool,
    /// Startup settings, the upper bound any cluster policy narrows.
    pub execution_policy: pplx_space_node_runtime_core::selection::ExecutionPolicy,
    pub execution_policy_record: PathBuf,
    pub memory_admission_sources: crate::memory_admission::MemoryAdmissionSources,
    pub runtime_supervisor: crate::runtime_supervisor_client::RuntimeSupervisorLazyClient,
    pub node_status_dirty: std::sync::Arc<dyn crate::node_state_sync::NodeStatusDirtySink>,
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimePaths {
    pub vendor: PathBuf,
    pub assets: PathBuf,
    pub kernels: PathBuf,
    pub store: ArtifactStorePaths,
}

/// The node's artifact store, which node-manager alone publishes into, and
/// where publications come from.
#[derive(Clone, Debug)]
pub(crate) struct ArtifactStorePaths {
    pub releases: PathBuf,
    pub kernels: PathBuf,
    /// The runtime manager's staging directory, read and never written.
    pub staging: PathBuf,
    /// Root-owned catalog files adding pins to the compiled ones.
    pub catalog: PathBuf,
}

impl Default for ArtifactStorePaths {
    fn default() -> Self {
        Self {
            releases: paths::NODE_FIRECRACKER_DIR.into(),
            kernels: paths::NODE_KERNELS_DIR.into(),
            staging: paths::RUNTIME_MANAGER_STAGING_DIR.into(),
            catalog: paths::RUNTIME_MANAGER_CATALOG_DIR.into(),
        }
    }
}

impl ServiceSettings {
    pub fn from_env() -> Result<Self, SelectionConfigurationError> {
        let settings = Self {
            execution_policy: execution_policy_from_env()?,
            execution_policy_record: paths::NODE_MANAGER_EXECUTION_POLICY_FILE.into(),
            disk_admission: CacheDiskAdmissionConfig::from_env(),
            lifecycle_limiter: crate::lifecycle_limiter::LifecycleLimiter::from_env(),
            runtime_paths: RuntimePaths {
                vendor: path(
                    "SPACE_FIRECRACKER_VENDOR_DIR",
                    paths::FIRECRACKER_VENDOR_DIR,
                ),
                assets: path("SPACE_FIRECRACKER_ASSET_DIR", paths::FIRECRACKER_ASSET_DIR),
                kernels: path(
                    "SPACE_FIRECRACKER_KERNEL_ASSET_DIR",
                    paths::FIRECRACKER_KERNEL_ASSET_DIR,
                ),
                store: ArtifactStorePaths::default(),
            },
            guest_console_logs: std::env::var("SPACE_ENABLE_GUEST_CONSOLE_LOGS").is_ok_and(
                |value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                },
            ),
            memory_admission_sources: crate::memory_admission::MemoryAdmissionSources::default(),
            runtime_supervisor: crate::runtime_supervisor_client::RuntimeSupervisorLazyClient::new(
            ),
            node_status_dirty: std::sync::Arc::new(
                crate::node_state_sync::NodeCtlNodeStatusDirtySink,
            ),
        };
        // Explicit non-secret fields, not a Debug dump of arbitrary config.
        tracing::info!(
            disk_admission = ?settings.disk_admission,
            guest_console_logs = settings.guest_console_logs,
            allowed_environments = ?settings.execution_policy.allowed_references().collect::<Vec<_>>(),
            default_environment = ?settings.execution_policy.default_reference(),
            legacy_unbound_allowed = settings.execution_policy.legacy_unbound_allowed(),
            "resolved startup-only node-manager host settings"
        );
        Ok(settings)
    }
}

fn path(name: &str, default: &str) -> PathBuf {
    let (path, source) = match std::env::var_os(name) {
        Some(value) => (PathBuf::from(value), "env"),
        None => (PathBuf::from(default), "default"),
    };
    tracing::info!(setting = name, source, path = %path.display(), "resolved startup-only runtime path");
    path
}

#[cfg(test)]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum SelectionConfigurationError {
    #[error("invalid execution selection setting: {0}")]
    Environment(#[from] std::env::VarError),
    #[error("invalid execution selection: {0}")]
    Policy(#[from] pplx_space_node_runtime_core::selection::PolicyError),
    #[error("cannot load execution catalog: {0}")]
    Catalog(#[from] pplx_space_node_runtime_core::catalog::CatalogError),
    #[error("unsupported host architecture: {0}")]
    Architecture(#[from] pplx_space_common_core::types::ArchParseError),
}

fn execution_policy_from_env()
-> Result<pplx_space_node_runtime_core::selection::ExecutionPolicy, SelectionConfigurationError> {
    use pplx_space_node_runtime_core::{catalog::CompiledEnvironment, selection::*};
    let read = |name| match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    };
    let allowed = read(ALLOWED_ENVIRONMENTS)?;
    let default = read(DEFAULT_ENVIRONMENT)?;
    let architecture = std::env::consts::ARCH.parse()?;
    execution_policy_with_catalog(
        allowed.as_deref(),
        default.as_deref(),
        architecture,
        CompiledEnvironment::for_architecture(architecture),
    )
}

fn execution_policy_with_catalog(
    allowed: Option<&str>,
    default: Option<&str>,
    architecture: pplx_space_common_core::types::Arch,
    catalog: Result<
        Vec<pplx_space_node_runtime_core::catalog::CompiledEnvironment>,
        pplx_space_node_runtime_core::catalog::CatalogError,
    >,
) -> Result<pplx_space_node_runtime_core::selection::ExecutionPolicy, SelectionConfigurationError> {
    use pplx_space_node_runtime_core::{
        catalog::{CatalogError, InitialEnvironment},
        selection::ExecutionPolicy,
    };
    match catalog {
        Ok(catalog) => Ok(ExecutionPolicy::parse(allowed, default, &catalog)?),
        Err(error) => {
            let initial = InitialEnvironment::for_architecture(architecture)?;
            let fallback = [initial
                .compiled_environment()
                .map_err(CatalogError::EnvironmentDefinition)?];
            // Parse against the independently loaded initial binding. This accepts
            // explicit spellings of the default but never drops requested candidates.
            tracing::warn!(%error, "candidate catalog unavailable; validating policy against initial binding");
            let policy = ExecutionPolicy::parse(allowed, default, &fallback)?;
            Ok(policy)
        }
    }
}

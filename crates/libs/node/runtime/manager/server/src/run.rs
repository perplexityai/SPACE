use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_common_core::host::linux::identities::group_names;
use pplx_space_common_svc::types::{ServiceName, UnixSocketPath};
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::{ScanLimits, ScanRoots};
use pplx_space_node_runtime_manager_client::proto::runtime_manager_server::RuntimeManagerServer;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

#[cfg(feature = "s3")]
use pplx_space_common_core::types::StorageUri;

use pplx_space_node_runtime_core::runtime_inventory::RuntimeInstallationRoot;

use crate::installs::{
    DirectoryCatalog, ExecutorParts, InstallLimits, Installer, KernelSource, NodeManagerPublisher,
    OpenInstallerError, ReleaseContext, ReleaseSource,
};
use crate::service::RuntimeManagerService;

/// The service name the runtime manager reports to telemetry and peer logs.
pub static SERVICE_NAME: LazyLock<ServiceName> = LazyLock::new(|| {
    ServiceName::parse("space-runtime-manager").expect("runtime manager service name is valid")
});

/// Connecting requires this group; each RPC then authorizes the peer itself.
const SOCKET_GROUP: &str = group_names::SPACE_RTMGRCLIENTS;

/// Bounds for one listing. A node carries a handful of releases and kernels,
/// so the entry limit only stops a pathological root; the digest limit sits
/// well above any guest kernel image; the deadline bounds a cold scan that
/// hashes every kernel once.
pub const DEFAULT_SCAN_LIMITS: ScanLimits = ScanLimits {
    max_entries_per_root: 256,
    max_digest_bytes: 512 * 1024 * 1024,
    deadline: Duration::from_secs(10),
};

/// How long a listing waits for a running one before reporting busy.
pub const DEFAULT_LIST_ADMISSION_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounds for one kernel install. The byte limit sits well above any guest
/// kernel; the idle timeout frees the only mutation slot from a stalled
/// download.
pub const DEFAULT_INSTALL_LIMITS: InstallLimits = InstallLimits {
    max_bytes: 512 * 1024 * 1024,
    download_idle_timeout: Duration::from_secs(60),
    max_attempts: 3,
};

/// Where published kernels come from.
#[derive(Clone)]
pub enum KernelSourceConfig {
    /// The built-in S3 source of Firecracker kernels, read anonymously from
    /// a root laid out as [`crate::FIRECRACKER_CI_KERNEL_SOURCE`] is.
    #[cfg(feature = "s3")]
    S3(StorageUri),
    /// A source the deployment supplies.
    Custom(Arc<dyn KernelSource>),
}

impl std::fmt::Debug for KernelSourceConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(root) => formatter.debug_tuple("S3").field(root).finish(),
            Self::Custom(source) => formatter
                .debug_tuple("Custom")
                .field(source.root())
                .finish(),
        }
    }
}

impl KernelSourceConfig {
    fn into_source(self) -> Arc<dyn KernelSource> {
        match self {
            #[cfg(feature = "s3")]
            Self::S3(root) => Arc::new(crate::installs::S3KernelSource::new(root)),
            Self::Custom(source) => source,
        }
    }
}

/// Where runtime releases come from.
#[derive(Clone)]
pub enum ReleaseSourceConfig {
    /// Firecracker's GitHub releases, under a download base laid out as
    /// [`crate::FIRECRACKER_RELEASE_SOURCE`] is.
    #[cfg(feature = "github")]
    GitHub(crate::ReleaseUrl),
    /// A source the deployment supplies.
    Custom(Arc<dyn ReleaseSource>),
}

impl std::fmt::Debug for ReleaseSourceConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "github")]
            Self::GitHub(base) => formatter.debug_tuple("GitHub").field(base).finish(),
            Self::Custom(source) => formatter
                .debug_tuple("Custom")
                .field(source.root())
                .finish(),
        }
    }
}

impl ReleaseSourceConfig {
    fn into_source(self) -> Result<Arc<dyn ReleaseSource>, RunRuntimeManagerError> {
        match self {
            #[cfg(feature = "github")]
            Self::GitHub(base) => Ok(Arc::new(
                crate::installs::GitHubReleaseSource::new(base)
                    .map_err(RunRuntimeManagerError::ReleaseSource)?,
            )),
            Self::Custom(source) => Ok(source),
        }
    }
}

/// Which runtime's releases the service installs, where they live in the
/// node store, and where they come from.
#[derive(Clone, Debug)]
pub struct RuntimeManagerReleaseConfig {
    /// The managed release root, read here and published into by node-manager.
    /// Diagnostic listings include it; execution never scans it.
    pub root: PathBuf,
    pub source: ReleaseSourceConfig,
}

/// Where and within what bounds the service installs. Absent, installs and
/// uninstalls are refused. The runtime manager downloads into its own
/// staging and node-manager publishes into the node's root-owned store, so
/// nothing here is a directory the runtime manager writes but staging.
#[derive(Clone, Debug)]
pub struct RuntimeManagerInstallConfig {
    /// The node store's kernel root (`<version>/vmlinux.bin`): read here,
    /// published into by node-manager. Listings always scan it.
    pub kernels_root: PathBuf,
    /// The runtime manager's private staging, which node-manager publishes
    /// from. Provisioning creates it.
    pub staging_dir: PathBuf,
    pub limits: InstallLimits,
}

/// Everything [`run`] needs from the process that hosts it.
#[derive(Clone, Debug)]
pub struct RuntimeManagerRunConfig {
    pub socket_path: UnixSocketPath,
    pub roots: ScanRoots,
    /// The runtime manager's state directory, beside its socket.
    /// Provisioning creates it; installs require it to be protected.
    pub state_dir: PathBuf,
    pub limits: ScanLimits,
    pub list_admission_timeout: Duration,
    /// Where published kernels are listed and downloaded from.
    pub kernel_source: Option<KernelSourceConfig>,
    /// Read-only catalog access remains available without an installer.
    pub catalog_dir: PathBuf,
    pub releases: Option<RuntimeManagerReleaseConfig>,
    /// Installs, or none when the service runs list-only.
    pub install: Option<RuntimeManagerInstallConfig>,
}

#[derive(Debug, thiserror::Error)]
pub enum RunRuntimeManagerError {
    #[error("failed to bind the runtime-manager socket {path}: {source}")]
    BindSocket {
        path: PathBuf,
        #[source]
        source: pplx_space_common_svc::bootstrap::BindUnixSocketError,
    },
    #[error("runtime-manager gRPC server failed: {0}")]
    Serve(#[source] tonic::transport::Error),
    #[error("failed to enable kernel installs: {0}")]
    Installer(#[source] OpenInstallerError),
    #[error("kernel installs need a kernel source")]
    NoKernelSource,
    #[error(
        "the runtime-manager state directory {} is not protected ({protection}); installs need it private",
        path.display()
    )]
    UnprotectedState { path: PathBuf, protection: String },
    #[error("runtime-manager startup task failed")]
    Startup(#[source] tokio::task::JoinError),
    #[cfg(feature = "github")]
    #[error("failed to enable release installs: {0}")]
    ReleaseSource(#[source] crate::GitHubReleaseSourceError),
}

/// Serve the runtime manager until `shutdown_token` is cancelled.
pub async fn run(
    config: RuntimeManagerRunConfig,
    shutdown_token: CancellationToken,
) -> Result<(), RunRuntimeManagerError> {
    let listener = pplx_space_common_svc::bind_unix_socket(
        config.socket_path.as_path(),
        pplx_space_common_svc::UnixSocketOptions::new(0o770)
            .with_group(pplx_space_common_svc::SocketGroup::Required(SOCKET_GROUP)),
    )
    .map_err(|source| RunRuntimeManagerError::BindSocket {
        path: config.socket_path.as_path().to_path_buf(),
        source,
    })?;
    let (_, health_service) = pplx_space_common_svc::grpc_health_service();
    let kernel_source = config.kernel_source.map(KernelSourceConfig::into_source);
    let mut roots = config.roots;
    let pins = Arc::new(DirectoryCatalog(config.catalog_dir));
    let releases = match config.releases {
        Some(releases) => {
            let source = releases.source.into_source()?;
            roots.managed_releases = Some(RuntimeInstallationRoot::new(
                source.runtime(),
                releases.root.clone(),
            ));
            Some(ReleaseContext {
                root: releases.root,
                source,
            })
        }
        None => None,
    };
    let installer = match config.install {
        Some(install) => {
            let source = kernel_source
                .clone()
                .ok_or(RunRuntimeManagerError::NoKernelSource)?;
            let audited = config.state_dir.clone();
            let report = tokio::task::spawn_blocking(move || {
                crate::protection::audit(
                    &audited,
                    crate::protection::PathRole::StateDirectory,
                    &crate::protection::AuditIdentity::of_this_process(),
                )
            })
            .await
            .map_err(RunRuntimeManagerError::Startup)?;
            if report.protection != crate::protection::Protection::Protected {
                return Err(RunRuntimeManagerError::UnprotectedState {
                    path: config.state_dir.clone(),
                    protection: format!("{:?}", report.protection),
                });
            }
            roots.managed_kernels = Some(install.kernels_root.clone());
            let host_arch = crate::installs::host_arch().ok_or(
                RunRuntimeManagerError::Installer(OpenInstallerError::UnsupportedArchitecture),
            )?;
            // A previous prototype image may have granted this account write
            // access to execution artifacts. Downloads must not run with that
            // authority. Leave execution alone and serve diagnostics only.
            let execution_roots: Vec<_> = roots
                .runtime_releases
                .iter()
                .map(|root| root.path.clone())
                .chain([
                    roots.vendor_kernels.clone(),
                    roots.cache_kernels.clone(),
                    roots.cache_legacy_assets.clone(),
                ])
                .collect();
            let unsafe_roots = tokio::task::spawn_blocking(move || {
                let identity = crate::protection::AuditIdentity::of_this_process();
                execution_roots
                    .into_iter()
                    .map(|path| {
                        crate::protection::audit(
                            &path,
                            crate::protection::PathRole::InstallRoot,
                            &identity,
                        )
                    })
                    .filter(|report| {
                        matches!(
                            report.protection,
                            crate::protection::Protection::Unprotected(_)
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .map_err(RunRuntimeManagerError::Startup)?;
            if !unsafe_roots.is_empty() {
                tracing::error!(
                    ?unsafe_roots,
                    "artifact management disabled: execution paths are writable or cannot be audited; repair provisioning then restart runtime-manager"
                );
                None
            } else {
                let installer = Installer::open(
                    ExecutorParts {
                        kernels_root: install.kernels_root,
                        staging: install.staging_dir,
                        limits: install.limits,
                        host_arch,
                        source,
                        releases: releases.clone(),
                        publisher: NodeManagerPublisher::connect_lazily(),
                        pins: pins.clone(),
                    },
                    &config.state_dir,
                )
                .await
                .map_err(RunRuntimeManagerError::Installer)?;
                Some(installer)
            }
        }
        None => None,
    };
    let mut service = RuntimeManagerService::new(
        roots,
        config.state_dir,
        config.limits,
        config.list_admission_timeout,
    )
    .with_catalog(pins, releases);
    if let Some(source) = kernel_source {
        service = service.with_kernel_source(source);
    }
    let pruner = installer
        .clone()
        .map(|installer| tokio::spawn(installer.prune_finished_jobs(shutdown_token.child_token())));
    if let Some(installer) = installer.clone() {
        service = service.with_installer(installer);
    }

    tracing::info!(
        socket_path = %config.socket_path.as_path().display(),
        kernel_installs = installer.is_some(),
        release_installs = installer
            .as_ref()
            .is_some_and(|installer| installer.releases_root().is_some()),
        "space-rtmgr listening"
    );
    let shutdown = async move {
        shutdown_token.cancelled().await;
        tracing::warn!("space-rtmgr shutting down");
    };
    let served = pplx_space_common_svc::grpc_server_builder()
        .layer(pplx_space_common_svc::grpc_trace_layer())
        .add_service(health_service)
        .add_service(RuntimeManagerServer::new(service))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
        .map_err(RunRuntimeManagerError::Serve);
    if let Some(installer) = installer {
        installer.shutdown().await;
    }
    if let Some(pruner) = pruner
        && let Err(error) = pruner.await
    {
        tracing::warn!(%error, "kernel job pruner failed");
    }
    served
}

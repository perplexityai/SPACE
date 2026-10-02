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

use crate::installs::{
    InstallLimits, Installer, KernelSource, OpenInstallerError, SeedError, SeedReport,
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

/// Where and within what bounds the service installs kernels. Absent,
/// installs and uninstalls are refused.
#[derive(Clone, Debug)]
pub struct RuntimeManagerInstallConfig {
    /// Versioned kernel root (`<version>/vmlinux.bin`) to install into. Must
    /// exist, on the state directory's filesystem. Listings always scan it.
    pub kernels_root: PathBuf,
    pub limits: InstallLimits,
    /// The node image's manifest of kernels it pre-installed in
    /// `kernels_root`, recorded as space-managed at startup. A missing file
    /// seeds nothing.
    pub seed_manifest: Option<PathBuf>,
}

/// Everything [`run`] needs from the process that hosts it.
#[derive(Clone, Debug)]
pub struct RuntimeManagerRunConfig {
    pub socket_path: UnixSocketPath,
    pub roots: ScanRoots,
    /// The runtime manager's persistent state directory. Provisioning
    /// creates it; this service only checks its protection for now.
    pub state_dir: PathBuf,
    pub limits: ScanLimits,
    pub list_admission_timeout: Duration,
    /// Where available kernels are listed from and installs download from.
    /// Where published kernels are listed and downloaded from.
    pub kernel_source: Option<KernelSourceConfig>,
    /// Installs stay disabled until a deployment hands the runtime manager
    /// an install root's writes.
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
}

/// A seed import that fails leaves the pre-installed kernels external; it is
/// reported, and the service runs on.
fn log_seed(manifest: &std::path::Path, imported: Result<SeedReport, SeedError>) {
    match imported {
        Ok(report) => {
            for refusal in &report.refused {
                tracing::warn!(error = %crate::installs::describe(refusal), "pre-installed kernel left external");
            }
            if !report.seeded.is_empty()
                || !report.recorded.is_empty()
                || !report.refused.is_empty()
            {
                tracing::info!(
                    manifest = %manifest.display(),
                    seeded = ?report.seeded.iter().map(ToString::to_string).collect::<Vec<_>>(),
                    already_recorded = report.recorded.len(),
                    refused = report.refused.len(),
                    "imported pre-installed kernels"
                );
            }
        }
        Err(error) => tracing::warn!(
            error = %crate::installs::describe(&error),
            "pre-installed kernels not imported; they stay external"
        ),
    }
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
    let installer = match config.install {
        Some(install) => {
            let source = kernel_source
                .clone()
                .ok_or(RunRuntimeManagerError::NoKernelSource)?;
            roots.managed_kernels = Some(install.kernels_root.clone());
            let installer = Installer::open(
                install.kernels_root,
                &config.state_dir,
                install.limits,
                source,
            )
            .await
            .map_err(RunRuntimeManagerError::Installer)?;
            if let Some(manifest) = install.seed_manifest {
                log_seed(&manifest, installer.import_seed(manifest.clone()).await);
            }
            Some(installer)
        }
        None => None,
    };
    let mut service = RuntimeManagerService::new(
        roots,
        config.state_dir,
        config.limits,
        config.list_admission_timeout,
    );
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

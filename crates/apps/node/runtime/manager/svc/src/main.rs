use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::{RuntimeKind, StorageUri};
use pplx_space_common_svc::types::{UnixSocketPath, UnixSocketPathParseError};
use pplx_space_node_runtime_core::runtime_inventory::RuntimeInstallationRoot;
use pplx_space_node_runtime_core::runtime_inventory::diagnostic::{ScanLimits, ScanRoots};
use pplx_space_node_runtime_manager::{
    DEFAULT_INSTALL_LIMITS, DEFAULT_LIST_ADMISSION_TIMEOUT, DEFAULT_SCAN_LIMITS,
    FIRECRACKER_CI_KERNEL_SOURCE, FIRECRACKER_RELEASE_SOURCE, InstallLimits, KernelSourceConfig,
    ReleaseSourceConfig, ReleaseUrl, RuntimeManagerInstallConfig, RuntimeManagerReleaseConfig,
    RuntimeManagerRunConfig, SERVICE_NAME,
};

#[derive(Parser)]
#[command(
    name = "space-rtmgr-svc",
    about = "SPACE node runtime and kernel manager",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Unix socket path to listen on.
    #[arg(long, default_value = paths::RUNTIME_MANAGER_SOCKET)]
    socket: PathBuf,

    /// A runtime's release directories, each `<version>/` holding its
    /// binaries, as `<runtime>=<dir>`. Repeat for each runtime.
    #[arg(
        long = "runtime-releases",
        value_parser = parse_release_root,
        default_values_t = [ReleaseRoot::firecracker()]
    )]
    runtime_releases: Vec<ReleaseRoot>,

    /// Versioned vendor kernels (`<version>/vmlinux.bin`).
    #[arg(long, default_value = paths::FIRECRACKER_KERNEL_VENDOR_DIR)]
    vendor_kernels_dir: PathBuf,

    /// Versioned cache kernels the execution inventory boots from.
    #[arg(long, default_value = paths::FIRECRACKER_KERNEL_ASSET_DIR)]
    cache_kernels_dir: PathBuf,

    /// Legacy flat cache layout (`default/kernel/vmlinux.bin`).
    #[arg(long, default_value = paths::FIRECRACKER_ASSET_DIR)]
    cache_legacy_assets_dir: PathBuf,

    /// State directory, beside the socket: the SQLite store.
    #[arg(long, default_value = paths::RUNTIME_MANAGER_STATE_DIR)]
    state_dir: PathBuf,

    /// Compatibility links to report. Repeat for each link.
    #[arg(long = "runtime-alias", default_values = [paths::FIRECRACKER_BIN, paths::JAILER_BIN])]
    runtime_aliases: Vec<PathBuf>,

    /// Directory entries read from one root before the listing reports it incomplete.
    #[arg(long, default_value_t = DEFAULT_SCAN_LIMITS.max_entries_per_root)]
    max_entries_per_root: usize,

    /// Largest kernel image hashed; larger images are listed without a digest.
    #[arg(long, default_value_t = DEFAULT_SCAN_LIMITS.max_digest_bytes)]
    max_digest_bytes: u64,

    /// Wall-clock budget for one listing, in seconds.
    #[arg(long, default_value_t = DEFAULT_SCAN_LIMITS.deadline.as_secs())]
    scan_deadline_secs: u64,

    /// How long a listing waits for a running one before reporting busy, in seconds.
    #[arg(long, default_value_t = DEFAULT_LIST_ADMISSION_TIMEOUT.as_secs())]
    list_admission_timeout_secs: u64,

    /// Only list and report: refuse every install and uninstall. For hosts
    /// whose node store has no node-manager to publish into it.
    #[arg(long)]
    list_only: bool,

    /// The node store's kernel root (`<version>/vmlinux.bin`). Read here;
    /// node-manager publishes into it.
    #[arg(long, default_value = paths::NODE_KERNELS_DIR)]
    node_kernels_dir: PathBuf,

    /// The node store's Firecracker release root
    /// (`v<version>/bin/{firecracker,jailer}`). Execution never reads this
    /// root. Read here; node-manager publishes into it.
    #[arg(long, default_value = paths::NODE_FIRECRACKER_DIR)]
    node_releases_dir: PathBuf,

    /// Private staging that downloads unpack into and node-manager publishes
    /// from. Provisioning creates it.
    #[arg(long, default_value = paths::RUNTIME_MANAGER_STAGING_DIR)]
    staging_dir: PathBuf,

    /// Read-only auxiliary boot source: <kind>-<version>--<digest-prefix>/image.
    #[arg(long)]
    boot_artifacts_source: Option<PathBuf>,

    /// Root-owned catalog files adding release and kernel pins to the
    /// compiled ones.
    #[arg(long, default_value = paths::RUNTIME_MANAGER_CATALOG_DIR)]
    catalog_dir: PathBuf,

    /// Where Firecracker release archives are downloaded from: an
    /// `https://` base laid out as Firecracker's GitHub release downloads,
    /// `<base>/v<version>/firecracker-v<version>-<arch>.tgz`.
    #[arg(long, default_value = FIRECRACKER_RELEASE_SOURCE, value_parser = ReleaseSourceParser)]
    release_source: ReleaseUrl,

    /// Where Cloud Hypervisor static executables are downloaded from.
    #[arg(long, default_value = pplx_space_node_runtime_manager::CLOUD_HYPERVISOR_RELEASE_SOURCE, value_parser = ReleaseSourceParser)]
    cloud_hypervisor_release_source: ReleaseUrl,

    /// Largest kernel or release archive one install downloads, in bytes.
    #[arg(long, default_value_t = DEFAULT_INSTALL_LIMITS.max_bytes)]
    max_install_bytes: u64,

    /// How long a download may stall before it fails, in seconds.
    #[arg(long, default_value_t = DEFAULT_INSTALL_LIMITS.download_idle_timeout.as_secs())]
    download_idle_timeout_secs: u64,

    /// Attempts per install or uninstall before a transfer failure is final.
    #[arg(long, default_value_t = DEFAULT_INSTALL_LIMITS.max_attempts)]
    install_max_attempts: i32,

    /// Where published kernels are listed and downloaded from: an `s3://`
    /// URI laid out as Firecracker's CI bucket, its key prefix the directory
    /// above the `v<line>` release-line directories. Read anonymously.
    #[arg(long, default_value = FIRECRACKER_CI_KERNEL_SOURCE)]
    kernel_source: StorageUri,
}

/// `--runtime-releases`: a runtime and its release root.
#[derive(Clone, Debug)]
struct ReleaseRoot(RuntimeInstallationRoot);

impl ReleaseRoot {
    fn firecracker() -> Self {
        Self(RuntimeInstallationRoot::new(
            RuntimeKind::Firecracker,
            paths::FIRECRACKER_VENDOR_DIR,
        ))
    }
}

impl std::fmt::Display for ReleaseRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}={}", self.0.kind, self.0.path.display())
    }
}

fn parse_release_root(value: &str) -> Result<ReleaseRoot, String> {
    let (runtime, path) = value
        .split_once('=')
        .filter(|(_, path)| !path.is_empty())
        .ok_or_else(|| {
            format!("'{value}' is not <runtime>=<dir>, such as fc=/space/vendor/firecracker")
        })?;
    let runtime = RuntimeKind::parse(runtime).map_err(|error| error.to_string())?;
    Ok(ReleaseRoot(RuntimeInstallationRoot::new(runtime, path)))
}

impl TryFrom<Cli> for RuntimeManagerRunConfig {
    type Error = UnixSocketPathParseError;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let install = (!cli.list_only).then(|| RuntimeManagerInstallConfig {
            kernels_root: cli.node_kernels_dir.clone(),
            staging_dir: cli.staging_dir,
            boot_artifacts_source: cli.boot_artifacts_source,
            limits: InstallLimits {
                max_bytes: cli.max_install_bytes,
                download_idle_timeout: Duration::from_secs(cli.download_idle_timeout_secs),
                max_attempts: cli.install_max_attempts,
            },
        });
        Ok(Self {
            socket_path: UnixSocketPath::parse(cli.socket)?,
            roots: ScanRoots {
                runtime_releases: cli
                    .runtime_releases
                    .into_iter()
                    .map(|root| root.0)
                    .collect(),
                vendor_kernels: cli.vendor_kernels_dir,
                managed_kernels: Some(cli.node_kernels_dir),
                managed_releases: Vec::new(),
                cache_kernels: cli.cache_kernels_dir,
                cache_legacy_assets: cli.cache_legacy_assets_dir,
                runtime_aliases: cli.runtime_aliases,
            },
            state_dir: cli.state_dir,
            limits: ScanLimits {
                max_entries_per_root: cli.max_entries_per_root,
                max_digest_bytes: cli.max_digest_bytes,
                deadline: Duration::from_secs(cli.scan_deadline_secs),
            },
            list_admission_timeout: Duration::from_secs(cli.list_admission_timeout_secs),
            kernel_sources: vec![
                KernelSourceConfig::S3(cli.kernel_source),
                KernelSourceConfig::CloudHypervisor,
            ],
            catalog_dir: cli.catalog_dir,
            releases: vec![
                RuntimeManagerReleaseConfig {
                    root: cli.node_releases_dir,
                    source: ReleaseSourceConfig::GitHub(cli.release_source),
                },
                RuntimeManagerReleaseConfig {
                    root: paths::NODE_CLOUD_HYPERVISOR_DIR.into(),
                    source: ReleaseSourceConfig::CloudHypervisorGitHub(
                        cli.cloud_hypervisor_release_source,
                    ),
                },
            ],
            install,
        })
    }
}

/// Clap's inferred parser repeats invalid inputs in diagnostics. Release-source
/// URLs must be refused without echoing credentials or signed query parameters.
#[derive(Clone)]
struct ReleaseSourceParser;

impl clap::builder::TypedValueParser for ReleaseSourceParser {
    type Value = ReleaseUrl;

    fn parse_ref(
        &self,
        command: &clap::Command,
        argument: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let invalid = || {
            let option = argument.map_or("release source", |arg| arg.get_id().as_str());
            clap::Error::raw(
                clap::error::ErrorKind::ValueValidation,
                format!(
                    "{option}: {}",
                    pplx_space_node_runtime_manager::ReleaseUrlParseError
                ),
            )
            .with_cmd(command)
        };
        value
            .to_str()
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())
    }
}

fn main() -> anyhow::Result<()> {
    let config = RuntimeManagerRunConfig::try_from(Cli::parse())?;
    pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure runtime-manager Tokio runtime")?
        .block_on(async {
            let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME.as_str())?;
            let shutdown_token = pplx_space_common_svc::shutdown_token();
            let result = pplx_space_node_runtime_manager::run(config, shutdown_token).await;
            pplx_space_common_svc::shutdown_telemetry(telemetry).await;
            Ok(result?)
        })
}

#[cfg(test)]
mod tests;

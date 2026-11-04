use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use pplx_space_common_svc::DEFAULT_SERVICE_MUTEX_TIMEOUT;
use pplx_space_sbx_mgr::{SERVICE_NAME, SOCKET_PATH, SbxMgrRunConfig};

#[derive(Parser)]
#[command(
    name = "space-sbx-manager",
    about = "SPACE sandbox manager daemon",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Production sandbox-manager Unix socket. Bound only after the service
    /// mutex is acquired.
    #[arg(long, default_value = SOCKET_PATH)]
    socket: PathBuf,

    /// Sandbox-scoped host-control Unix socket. Bound only after the service
    /// mutex is acquired.
    #[arg(
        long,
        default_value = pplx_space_common_core::host::linux::paths::SANDBOX_HOST_CONTROL_SOCKET
    )]
    host_control_socket: PathBuf,

    /// Instance-local health Unix socket. When supplied, it is bound from
    /// process startup so external readiness checks can pass while this
    /// instance is still passive waiting on the service mutex. Must be on a
    /// per-process mount so a coexisting successor does not unlink the
    /// predecessor's socket file when binding.
    #[arg(long)]
    health_socket: Option<PathBuf>,

    /// Maximum time the passive instance waits for the predecessor to
    /// release the service mutex. On timeout the process exits non-zero so
    /// the supervisor restarts it and a stuck predecessor surfaces as a
    /// visible failure instead of an indefinitely-passive process.
    #[arg(long, default_value_t = DEFAULT_SERVICE_MUTEX_TIMEOUT.as_secs())]
    service_mutex_timeout_secs: u64,
}

impl From<Cli> for SbxMgrRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            socket_path: cli.socket,
            host_control_socket_path: cli.host_control_socket,
            health_socket_path: cli.health_socket,
            service_mutex_timeout: Duration::from_secs(cli.service_mutex_timeout_secs),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure sandbox-manager Tokio runtime")?
        .block_on(async {
            let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
            let shutdown_token = pplx_space_common_svc::shutdown_token();
            let result = pplx_space_sbx_mgr::run(cli.into(), shutdown_token).await;
            pplx_space_common_svc::shutdown_telemetry(telemetry).await;
            Ok(result?)
        })
}

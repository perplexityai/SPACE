use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use pplx_space_common_svc::DEFAULT_SERVICE_MUTEX_TIMEOUT;
use pplx_space_node_ctl_server::{
    DEFAULT_NODE_CTL_CONFIG_PATH, DEFAULT_NODE_CTL_SOCKET_GROUP, NodeCtlRunConfig, SERVICE_NAME,
};

#[derive(Parser)]
#[command(
    name = "space-nodectl-svc",
    about = "SPACE node control reconciler",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Node-ctl TOML config path.
    #[arg(long, default_value = DEFAULT_NODE_CTL_CONFIG_PATH)]
    config: PathBuf,

    /// Production node-ctl Unix socket. Bound only after the service mutex
    /// beside it, `service.mutex` in the same directory, is acquired.
    #[arg(long, default_value = pplx_space_common_core::host::linux::paths::NODE_CTL_SOCKET)]
    socket: PathBuf,

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

    /// The group the production socket is given, so a test can name a group
    /// it belongs to. Test builds only.
    #[cfg(feature = "test-hooks")]
    #[arg(long, default_value = DEFAULT_NODE_CTL_SOCKET_GROUP)]
    socket_group: String,

    #[arg(long, default_value = pplx_space_common_core::host::linux::paths::NODE_CTL_DB)]
    db: PathBuf,

    #[arg(long)]
    poll_interval_secs: Option<u64>,

    /// Optional SPACE node role used when node_id is a substrate-local name.
    #[arg(long)]
    role: Option<String>,
}

impl From<Cli> for NodeCtlRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            config_path: cli.config,
            socket_path: cli.socket,
            health_socket_path: cli.health_socket,
            service_mutex_timeout: Duration::from_secs(cli.service_mutex_timeout_secs),
            // Parsed once per process, and the socket options take a
            // `&'static str`.
            #[cfg(feature = "test-hooks")]
            socket_group: cli.socket_group.leak(),
            #[cfg(not(feature = "test-hooks"))]
            socket_group: DEFAULT_NODE_CTL_SOCKET_GROUP,
            db_path: cli.db,
            poll_interval_secs: cli.poll_interval_secs,
            role: cli.role,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure node-ctl Tokio runtime")?
        .block_on(async {
            let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
            let shutdown_token = pplx_space_common_svc::shutdown_token();
            let result = pplx_space_node_ctl_server::run(cli.into(), shutdown_token).await;
            pplx_space_common_svc::shutdown_telemetry(telemetry).await;
            Ok(result?)
        })
}

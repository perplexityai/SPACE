use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use pplx_space_node_api_server::{DEFAULT_SHUTDOWN_GRACE_SECS, NodeApiRunConfig, SERVICE_NAME};

#[derive(Parser)]
#[command(
    name = "space-node-api-svc",
    about = "SPACE node API gateway",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// TCP address to listen on
    #[arg(long, default_value = "0.0.0.0:9800")]
    listen: String,

    /// Node role: determines which API surface is exposed.
    ///
    /// The cluster control plane is served by the separate `space-internal-api-svc`
    /// binary; this binary handles per-node roles only.
    #[arg(long, default_value = "worker")]
    role: String,

    /// Instance-local health Unix socket for Kubernetes probes.
    #[arg(long)]
    health_socket: Option<PathBuf>,

    /// Maximum time to wait for in-flight gRPC requests after SIGTERM.
    #[arg(long, default_value_t = DEFAULT_SHUTDOWN_GRACE_SECS)]
    shutdown_grace_secs: u64,
}

impl From<Cli> for NodeApiRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            listen: cli.listen,
            role: cli.role,
            health_socket: cli.health_socket,
            shutdown_grace: Duration::from_secs(cli.shutdown_grace_secs),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure node-api Tokio runtime")?
        .block_on(async {
            // `init_service` installs the process-global aws-lc-rs rustls crypto
            // provider used by the kube TokenReview client; no separate install here.
            let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
            let shutdown_token = pplx_space_common_svc::shutdown_token();
            let result = pplx_space_node_api_server::run(cli.into(), shutdown_token).await;
            pplx_space_common_svc::shutdown_telemetry(telemetry).await;
            Ok(result?)
        })
}

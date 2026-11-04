use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use pplx_space_build_mgr_server::{
    BuildMgrRunConfig, DEFAULT_DRAIN_TIMEOUT_SECONDS, DEFAULT_LISTEN, SERVICE_NAME,
};
use pplx_space_common_svc::core::host::linux::paths::BUILD_MANAGER_DB;

#[derive(Parser)]
#[command(
    name = "space-buildmgr-svc",
    about = "SPACE build manager daemon",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    #[arg(long, default_value = DEFAULT_LISTEN)]
    listen: SocketAddr,

    #[arg(long, default_value_t = 2)]
    max_concurrent_builds: usize,

    #[arg(long, default_value = BUILD_MANAGER_DB)]
    db: String,

    #[arg(long, default_value_t = DEFAULT_DRAIN_TIMEOUT_SECONDS)]
    drain_timeout_seconds: u64,
}

impl From<Cli> for BuildMgrRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            listen: cli.listen,
            max_concurrent_builds: cli.max_concurrent_builds,
            db_path: cli.db,
            drain_timeout: Duration::from_secs(cli.drain_timeout_seconds),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure build-manager Tokio runtime")?
        .block_on(async {
            let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
            let shutdown_token = pplx_space_common_svc::shutdown_token();
            let result = pplx_space_build_mgr_server::run(cli.into(), shutdown_token).await;
            pplx_space_common_svc::shutdown_telemetry(telemetry).await;
            Ok(result?)
        })
}

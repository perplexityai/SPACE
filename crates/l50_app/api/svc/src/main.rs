use std::time::Duration;

use clap::{Parser, ValueEnum};
use pplx_space_api::{ApiRunConfig, DEFAULT_SHUTDOWN_GRACE_SECS};

// jemalloc gives the API service better fragmentation behavior than glibc
// malloc under the bursty allocation pattern of an HTTP service. Static musl
// builds use the target allocator; tikv-jemallocator's prefixed symbols do not
// link reliably through the cross-musl toolchain.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(
    name = "space-api-svc",
    about = "SPACE API server",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Listen address
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: String,

    /// Maximum time to wait for in-flight HTTP requests after SIGTERM.
    #[arg(long, default_value_t = DEFAULT_SHUTDOWN_GRACE_SECS)]
    shutdown_grace_secs: u64,

    /// API surface to serve from this process.
    #[arg(long, value_enum, default_value_t = ApiSurface::Public)]
    surface: ApiSurface,

    /// PostgreSQL connection string for the `space_user` and `space_creds`
    /// pools.
    #[arg(long, env = "POSTGRES_CONNECTION_STRING", hide_env_values = true)]
    postgres_connection_string: String,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ApiSurface {
    /// Dedicated public API host plus host-gated E2B compatibility API.
    Public,
    /// Single-host topology with `/api`, `/admin/api`, and `/e2b/api` roots.
    Shared,
    /// Dedicated admin API only.
    Admin,
}

impl From<ApiSurface> for pplx_space_api::ApiSurface {
    fn from(surface: ApiSurface) -> Self {
        match surface {
            ApiSurface::Public => Self::Public,
            ApiSurface::Shared => Self::Shared,
            ApiSurface::Admin => Self::Admin,
        }
    }
}

impl From<Cli> for ApiRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            listen: cli.listen,
            shutdown_grace: Duration::from_secs(cli.shutdown_grace_secs),
            surface: cli.surface.into(),
            database_url: cli.postgres_connection_string,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ApiRunConfig::from(Cli::parse());
    let telemetry = pplx_space_common_svc::init_service(config.service_name())?;
    let shutdown_token = pplx_space_common_svc::shutdown_token();

    let result = pplx_space_api::run(config, shutdown_token).await;
    pplx_space_common_svc::shutdown_telemetry(telemetry).await;
    Ok(result?)
}

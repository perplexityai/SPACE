use clap::Parser;
use pplx_space_cluster_controller_server::{
    ClusterControllerConfig, DEFAULT_LEASE_NAMESPACE, SERVICE_NAME,
};

#[derive(Parser)]
#[command(
    name = "space-cluster-controller",
    about = "SPACE cluster controller: leader-elected reconciler set",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// PostgreSQL connection string for the control-plane database.
    #[arg(long, env = "POSTGRES_CONNECTION_STRING", hide_env_values = true)]
    postgres_connection_string: String,

    /// Namespace of the leadership `Lease`; the downward-API pod namespace.
    #[arg(long, env = "POD_NAMESPACE", default_value = DEFAULT_LEASE_NAMESPACE)]
    pod_namespace: String,
}

impl From<Cli> for ClusterControllerConfig {
    fn from(cli: Cli) -> Self {
        let lease_namespace = match cli.pod_namespace.trim() {
            "" => DEFAULT_LEASE_NAMESPACE.to_string(),
            namespace => namespace.to_string(),
        };
        Self {
            database_url: cli.postgres_connection_string,
            lease_namespace,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
    let shutdown_token = pplx_space_common_svc::shutdown_token();

    let result = pplx_space_cluster_controller_server::run(cli.into(), shutdown_token).await;
    pplx_space_common_svc::shutdown_telemetry(telemetry).await;
    Ok(result?)
}

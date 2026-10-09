use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use pplx_space_common_svc::DEFAULT_SERVICE_MUTEX_TIMEOUT;
use pplx_space_common_svc::core::host::linux::paths::CREDENTIAL_MANAGER_SOCKET;
use pplx_space_credmgr::{CredmgrRole, CredmgrRunConfig, SERVICE_NAME};

#[derive(Parser)]
#[command(
    name = "space-credmgr-svc",
    about = "SPACE credential manager — KEK envelope operations and credential resolution",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),
    /// Operating role. `authority` (default) owns the configured key backends
    /// and Postgres state and serves database-backed RPCs directly. `agent`
    /// is the node-local proxy: it holds no keys and no database, and
    /// forwards every RPC to the upstream authority configured via
    /// SPACE_CREDMGR_ENDPOINT.
    #[arg(long, value_enum, default_value_t = Role::Authority)]
    role: Role,
    /// Unix socket path for the gRPC service
    #[arg(long, default_value = CREDENTIAL_MANAGER_SOCKET)]
    socket: PathBuf,
    /// Restricted volume-manager credential socket (ExchangeToken and the
    /// sandbox artifact-DEK RPCs).
    #[arg(long, conflicts_with = "no_uds")]
    volume_socket: Option<PathBuf>,
    /// TCP listen address for the control-plane deployment
    /// (e.g. 0.0.0.0:9810). Without TLS flags this requires
    /// --allow-plaintext.
    #[arg(long)]
    tcp_listen: Option<SocketAddr>,
    /// Server certificate PEM. Terminates and encrypts the connection and
    /// lets clients verify the credmgr server identity. Requires --tls-key.
    /// Client identity is established by the projected ServiceAccount token
    /// (TokenReview), not a client certificate.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// Server private key PEM.
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// Permit a plaintext TCP listener. Non-cloud profiles only
    /// (local-dev compose, direct systemd deploys); the EKS chart never
    /// sets this.
    #[arg(long)]
    allow_plaintext: bool,
    /// Skip the node-local Unix socket and serve TCP only. For
    /// control-plane deployments, whose containers lack the node
    /// identities the socket group requires.
    #[arg(long, requires = "tcp_listen")]
    no_uds: bool,
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

/// Which side of the control-plane split this process runs as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
enum Role {
    /// Owns the configured key backends and Postgres state; serves RPCs directly.
    #[default]
    Authority,
    /// Node-local proxy holding no keys or database; forwards to the
    /// upstream authority.
    Agent,
}

impl From<Role> for CredmgrRole {
    fn from(role: Role) -> Self {
        match role {
            Role::Authority => Self::Authority,
            Role::Agent => Self::Agent,
        }
    }
}

impl From<Cli> for CredmgrRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            role: cli.role.into(),
            socket_path: cli.socket,
            volume_socket_path: cli.volume_socket,
            tcp_listen: cli.tcp_listen,
            tls_cert: cli.tls_cert,
            tls_key: cli.tls_key,
            allow_plaintext: cli.allow_plaintext,
            no_uds: cli.no_uds,
            health_socket_path: cli.health_socket,
            service_mutex_timeout: Duration::from_secs(cli.service_mutex_timeout_secs),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure credential-manager Tokio runtime")?
        .block_on(async {
            let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
            let shutdown_token = pplx_space_common_svc::shutdown_token();
            let result = pplx_space_credmgr::run(cli.into(), shutdown_token).await;
            pplx_space_common_svc::shutdown_telemetry(telemetry).await;
            Ok(result?)
        })
}

use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use pplx_space_spaced_host_server::{SERVICE_NAME, SpacedHostBootType, SpacedHostRunConfig};

#[derive(Parser, Debug)]
#[command(name = "space-spaced-host-svc", version, disable_version_flag = true)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    #[arg(long)]
    socket: Option<PathBuf>,
    #[arg(long)]
    host_control_socket: Option<PathBuf>,
    #[arg(long)]
    vsock_uds: Option<PathBuf>,
    #[arg(long, requires = "vsock_uds")]
    firecracker_pid: Option<u32>,
    #[arg(long, requires = "vsock_uds")]
    firecracker_uid: Option<u32>,
    /// Reach the guest's bridge over this UDS instead of a Firecracker vsock.
    /// Ignored when `--vsock-uds` is set.
    #[arg(long)]
    bridge_guest_uds: Option<PathBuf>,
    /// Listen for the guest's host-bound bridge on this UDS instead of a
    /// Firecracker vsock. Ignored when `--vsock-uds` is set.
    #[arg(long)]
    bridge_host_uds: Option<PathBuf>,
    #[arg(long)]
    sandbox_id: Option<String>,
    #[arg(long, value_enum, default_value_t = BootType::Fresh)]
    boot_type: BootType,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum BootType {
    Fresh,
    Snapshot,
    #[value(name = "base-snapshot")]
    BaseSnapshot,
    Restore,
    Resume,
}

impl From<BootType> for SpacedHostBootType {
    fn from(boot_type: BootType) -> Self {
        match boot_type {
            BootType::Fresh => Self::Fresh,
            BootType::Snapshot => Self::Snapshot,
            BootType::BaseSnapshot => Self::BaseSnapshot,
            BootType::Restore => Self::Restore,
            BootType::Resume => Self::Resume,
        }
    }
}

impl From<Cli> for SpacedHostRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            socket: cli.socket,
            host_control_socket: cli.host_control_socket,
            vsock_uds: cli.vsock_uds,
            firecracker_pid: cli.firecracker_pid,
            firecracker_uid: cli.firecracker_uid,
            bridge_guest_uds: cli.bridge_guest_uds,
            bridge_host_uds: cli.bridge_host_uds,
            sandbox_id: cli.sandbox_id,
            boot_type: cli.boot_type.into(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = SpacedHostRunConfig::from(Cli::parse());
    let telemetry =
        pplx_space_common_svc::init_service_with_log_context(SERVICE_NAME, config.log_context())?;
    let shutdown = pplx_space_common_svc::shutdown_token();
    let result = pplx_space_spaced_host_server::run(config, shutdown).await;
    pplx_space_common_svc::shutdown_telemetry(telemetry).await;
    Ok(result?)
}

use std::os::fd::RawFd;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use pplx_space_common_core::host::linux::paths::RUNTIME_SUPERVISOR_SOCKET;
use pplx_space_node_runtime_supervisor::{
    DEFAULT_JAILER_START_TIMEOUT, RuntimeSupervisorRunConfig, SIDECAR_LAUNCHER_FD_ARG_NAME,
};

#[derive(Parser)]
#[command(
    name = "space-runtime-supervisor",
    about = "SPACE host runtime supervisor",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Unix socket path to listen on.
    #[arg(long, default_value = RUNTIME_SUPERVISOR_SOCKET)]
    socket: PathBuf,

    /// Milliseconds one Firecracker jailer run may take before it is killed and
    /// the launch fails. Zero would fail every launch, so it is refused.
    #[arg(
        long,
        env = "SPACE_RUNTIME_SUPERVISOR_JAILER_START_TIMEOUT_MS",
        default_value_t = DEFAULT_JAILER_START_TIMEOUT.as_millis() as u64,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    jailer_start_timeout_ms: u64,

    /// Run the internal single-threaded sidecar launcher protocol.
    #[arg(long = SIDECAR_LAUNCHER_FD_ARG_NAME, hide = true)]
    sidecar_launcher_fd: Option<RawFd>,
}

impl From<Cli> for RuntimeSupervisorRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            socket_path: cli.socket,
            sidecar_launcher_fd: cli.sidecar_launcher_fd,
            jailer_start_timeout: Duration::from_millis(cli.jailer_start_timeout_ms),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // The library owns the Tokio runtime, telemetry, and the shutdown token for
    // this service: launcher mode has to run in a single-threaded process with
    // none of them, so the mode choice cannot be made out here.
    Ok(pplx_space_node_runtime_supervisor::run(cli.into())?)
}

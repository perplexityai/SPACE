use clap::{ArgAction, Parser};
#[cfg(unix)]
use pplx_space_spaced_guest_server::SpacedGuestRunConfig;

#[derive(Parser)]
#[command(
    name = "spaced-svc",
    about = "SPACE guest daemon",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(
        short = 'v',
        long = "version",
        action = ArgAction::Version,
        required = false
    )]
    _version: (),

    /// Hold the calling unit's cgroup open and do nothing else, for
    /// `space-workloads.service`: systemd removes an empty cgroup, and this
    /// binary is in every guest, where `sleep` may not be.
    #[cfg(unix)]
    #[arg(long = "hold-cgroup", hide = true)]
    hold_cgroup: bool,

    /// Run the Windows service in the foreground for diagnostics.
    #[cfg(windows)]
    #[arg(long)]
    console: bool,
}

#[cfg(unix)]
impl From<Cli> for SpacedGuestRunConfig {
    fn from(_cli: Cli) -> Self {
        // The guest daemon takes no flags: the microVM's environment is its
        // configuration, and the library reads it.
        Self::default()
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    #[cfg(unix)]
    if cli.hold_cgroup {
        // No runtime, telemetry or cgroup setup: SIGTERM from the unit's stop
        // ends the process with the default disposition.
        loop {
            std::thread::park();
        }
    }
    #[cfg(unix)]
    {
        Ok(pplx_space_spaced_guest_server::run(cli.into())?)
    }
    #[cfg(windows)]
    {
        if cli.console {
            tracing_subscriber::fmt()
                .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
                .init();
        }
        Ok(pplx_space_spaced_guest_windows::run(cli.console)?)
    }
}

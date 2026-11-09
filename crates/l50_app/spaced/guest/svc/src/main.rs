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

    /// Select the committed or single-trial release before systemd starts us.
    #[cfg(target_os = "linux")]
    #[arg(long, hide = true, conflicts_with = "hold_cgroup")]
    select: bool,

    /// Initialize an offline, prepopulated slot A during image assembly.
    #[cfg(target_os = "linux")]
    #[arg(long, hide = true, conflicts_with_all = ["hold_cgroup", "select"])]
    initialize_update: Option<std::path::PathBuf>,

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
    #[cfg(target_os = "linux")]
    if let Some(root) = &cli.initialize_update {
        return Ok(pplx_space_spaced_guest_update::initialize_at(root)?);
    }
    #[cfg(target_os = "linux")]
    if cli.select {
        return Ok(pplx_space_spaced_guest_update::select_installed()?);
    }
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
        } else {
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
                .with_writer(|| pplx_space_spaced_guest_windows::EventLogWriter)
                .init();
        }
        Ok(pplx_space_spaced_guest_windows::run(cli.console)?)
    }
}

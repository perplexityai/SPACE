use clap::{ArgAction, Parser};
#[cfg(unix)]
use pplx_space_spaced_guest_server::platform::current::SpacedGuestRunConfig;

#[derive(Parser)]
#[command(
    name = "spaced-svc",
    about = "SPACE guest daemon",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Operator-only immutable Wasmtime/Python tool root inside a contained agent.
    #[cfg(all(unix, feature = "experimental-wasm"))]
    #[arg(long, requires_all = ["wasmtime_workspace", "wasmtime_bridge_socket", "wasmtime_host_bridge_socket"])]
    wasmtime_root: Option<std::path::PathBuf>,

    #[cfg(all(unix, feature = "experimental-wasm"))]
    #[arg(long, requires = "wasmtime_root")]
    wasmtime_workspace: Option<std::path::PathBuf>,

    #[cfg(all(unix, feature = "experimental-wasm"))]
    #[arg(long, requires = "wasmtime_root")]
    wasmtime_bridge_socket: Option<std::path::PathBuf>,

    #[cfg(all(unix, feature = "experimental-wasm"))]
    #[arg(long, requires = "wasmtime_root")]
    wasmtime_host_bridge_socket: Option<std::path::PathBuf>,

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
    #[cfg(any(target_os = "linux", windows))]
    #[arg(long, hide = true)]
    #[cfg_attr(target_os = "linux", arg(conflicts_with_all = ["hold_cgroup", "select"]))]
    initialize_update: Option<std::path::PathBuf>,

    /// Settle a stopped or expired agent through SCM (fixed image recovery ABI).
    #[cfg(windows)]
    #[arg(long, hide = true, conflicts_with = "console")]
    recover_update: bool,

    /// Accepted for installed expiry tasks; recovery also retries stopped services.
    #[cfg(windows)]
    #[arg(long, hide = true, requires = "recover_update")]
    expired_only: bool,

    /// Run the Windows service in the foreground for diagnostics.
    #[cfg(windows)]
    #[arg(long)]
    console: bool,
}

#[cfg(all(unix, not(feature = "experimental-wasm")))]
impl From<Cli> for SpacedGuestRunConfig {
    fn from(_cli: Cli) -> Self {
        // The guest daemon takes no flags: the microVM's environment is its
        // configuration, and the library reads it.
        Self::default()
    }
}

#[cfg(all(unix, feature = "experimental-wasm"))]
impl TryFrom<Cli> for SpacedGuestRunConfig {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        use pplx_space_spaced_guest_server::platform::current::{
            HostConnectorType, SpacedGuestServiceConfig, WasiProcessProfile,
        };
        let Some(root) = cli.wasmtime_root else {
            return Ok(Self::default());
        };
        let workspace = cli
            .wasmtime_workspace
            .ok_or_else(|| anyhow::anyhow!("Wasmtime workspace is required"))?;
        let bridge = cli
            .wasmtime_bridge_socket
            .ok_or_else(|| anyhow::anyhow!("Wasmtime bridge socket is required"))?;
        let host_bridge = cli
            .wasmtime_host_bridge_socket
            .ok_or_else(|| anyhow::anyhow!("Wasmtime host bridge socket is required"))?;
        let profile = WasiProcessProfile::new(
            root.join("bin/wasmtime"),
            root.join("python/python.wasm"),
            workspace.clone(),
            root.join("python"),
            1024 * 1024 * 1024,
        )?;
        Ok(Self {
            service_config: Some(SpacedGuestServiceConfig {
                wasi_process_profile: Some(profile),
                default_working_dir: Some(workspace),
                local_socket_path: None,
                bridge_socket_path: Some(bridge),
                host_connector_type: HostConnectorType::Uds(host_bridge),
                ..Default::default()
            }),
        })
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    #[cfg(any(target_os = "linux", windows))]
    if let Some(root) = &cli.initialize_update {
        return Ok(pplx_space_spaced_guest_update::initialize_at(root)?);
    }
    #[cfg(target_os = "linux")]
    if cli.select {
        return Ok(pplx_space_spaced_guest_update::select_installed()?);
    }
    #[cfg(windows)]
    if cli.recover_update {
        return Ok(pplx_space_spaced_guest_server::platform::windows::recover(
            cli.expired_only,
        )?);
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
        Ok(pplx_space_spaced_guest_server::platform::current::run(
            cli.try_into()?,
        )?)
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
                .with_writer(|| pplx_space_spaced_guest_server::platform::windows::EventLogWriter)
                .init();
        }
        Ok(pplx_space_spaced_guest_server::platform::windows::run(
            cli.console,
        )?)
    }
}

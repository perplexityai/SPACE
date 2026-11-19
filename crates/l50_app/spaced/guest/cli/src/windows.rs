use anyhow::Context;
use clap::{ArgAction, Parser};
use windows_service::{
    service::{ServiceAccess, ServiceState, ServiceStatus},
    service_manager::{ServiceManager, ServiceManagerAccess},
};

#[cfg(test)]
mod tests;

#[derive(Parser)]
#[command(
    name = "spaced",
    about = "SPACE Windows guest diagnostics",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = ArgAction::Version)]
    _version: (),
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Query SCM state. This does not probe guest RPC responsiveness.
    Status,
}

pub(super) fn run() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::Status => {
            // Diagnostics must never acquire service start/stop privileges.
            let manager =
                ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
                    .context("cannot connect to the Windows service manager")?;
            let service = manager
                .open_service("spaced", ServiceAccess::QUERY_STATUS)
                .context("cannot open the spaced service")?;
            let status = service
                .query_status()
                .context("cannot query spaced status")?;
            print!("{}", status_output(&status));
        }
    }
    Ok(())
}

fn status_output(status: &ServiceStatus) -> String {
    // These strings are CLI output, independent of the dependency's Debug format.
    let state = match status.current_state {
        ServiceState::Stopped => "Stopped",
        ServiceState::StartPending => "StartPending",
        ServiceState::StopPending => "StopPending",
        ServiceState::Running => "Running",
        ServiceState::ContinuePending => "ContinuePending",
        ServiceState::PausePending => "PausePending",
        ServiceState::Paused => "Paused",
    };
    let mut output = format!("state={state}\n");
    if let Some(pid) = status.process_id {
        output.push_str(&format!("pid={pid}\n"));
    }
    output
}

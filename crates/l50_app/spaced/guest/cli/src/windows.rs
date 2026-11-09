use clap::{ArgAction, Parser};

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

pub(super) fn run() -> Result<(), pplx_space_spaced_guest_server::platform::windows::RunError> {
    match Cli::parse().command {
        Command::Status => {
            let status = pplx_space_spaced_guest_server::platform::windows::query_service_status()?;
            println!("state={:?}", status.current_state);
            if let Some(pid) = status.process_id {
                println!("pid={pid}");
            }
        }
    }
    Ok(())
}

use clap::{ArgAction, Parser};
use pplx_space_common_core::types::SimpleDuration;
use pplx_space_spaced_guest_client::proto::{
    RequestActiveForResponse, RequestShutdownResponse, SetActivityStateResponse,
    request_active_for_response::Outcome as ActiveForOutcome,
    request_shutdown_response::Outcome as ShutdownOutcome,
    set_activity_state_response::Outcome as ActivityStateOutcome,
};

#[derive(Parser)]
#[command(
    name = "spaced",
    about = "SPACE guest agent CLI",
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

    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Check that spaced-guest-svc is reachable.
    Ping,
    /// Tell the host that the guest is idle.
    NotifyIdle,
    /// Tell the host that the guest is active again.
    NotifyActive,
    /// Ask the host to keep the sandbox active for at least this long.
    ActiveFor {
        /// Keep the sandbox active for at least this long, e.g. 30s or 1m.
        #[arg(long = "for")]
        hold_for: SimpleDuration,
    },
    /// Ask spaced-guest-svc to request host-side shutdown.
    RequestShutdown {
        /// Operator-visible shutdown reason.
        #[arg(long)]
        reason: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    pplx_space_common_cli::init_logging("spaced");

    let cli = Cli::parse();

    let mut client = pplx_space_common_cli::unwrap_or_exit(
        pplx_space_spaced_guest_client::SpacedGuestClient::new().await,
        "cannot connect to spaced (is spaced running?)",
    );

    match cli.command {
        Commands::Ping => {
            pplx_space_common_cli::unwrap_or_exit(client.ping().await, "ping failed");
            println!("ok=true");
        }
        Commands::NotifyIdle => {
            let response = pplx_space_common_cli::unwrap_or_exit(
                client.notify_idle().await,
                "notify-idle failed",
            );
            print_activity_state_response(response);
        }
        Commands::NotifyActive => {
            let response = pplx_space_common_cli::unwrap_or_exit(
                client.notify_active().await,
                "notify-active failed",
            );
            print_activity_state_response(response);
        }
        Commands::ActiveFor { hold_for } => {
            let response = pplx_space_common_cli::unwrap_or_exit(
                client.request_active_for(hold_for.as_duration()).await,
                "active-for failed",
            );
            print_active_for_response(response);
        }
        Commands::RequestShutdown { reason } => {
            let response = pplx_space_common_cli::unwrap_or_exit(
                client.request_shutdown_with_reason(reason).await,
                "request-shutdown failed",
            );
            print_shutdown_response(response);
        }
    }
}

fn print_activity_state_response(response: SetActivityStateResponse) {
    match response.outcome {
        Some(ActivityStateOutcome::Accepted(_)) => println!("decision=accepted"),
        Some(ActivityStateOutcome::Denied(denied)) => {
            println!("decision=denied");
            println!("reason={}", denied.reason);
        }
        None => println!("ok=true"),
    }
}

fn print_active_for_response(response: RequestActiveForResponse) {
    match response.outcome {
        Some(ActiveForOutcome::Accepted(accepted)) => {
            println!("decision=accepted");
            if let Some(granted_for) = accepted.granted_for {
                println!(
                    "granted_for={}",
                    format_duration(granted_for.seconds, granted_for.nanos)
                );
            }
        }
        Some(ActiveForOutcome::Denied(denied)) => {
            println!("decision=denied");
            println!("reason={}", denied.reason);
        }
        None => println!("ok=true"),
    }
}

fn print_shutdown_response(response: RequestShutdownResponse) {
    match response.outcome {
        Some(ShutdownOutcome::Accepted(_)) => println!("decision=accepted"),
        Some(ShutdownOutcome::Denied(denied)) => {
            println!("decision=denied");
            println!("reason={}", denied.reason);
        }
        None => println!("ok=true"),
    }
}

fn format_duration(seconds: i64, nanos: i32) -> String {
    let duration = jiff::SignedDuration::new(seconds, nanos);
    format!("{duration:#}")
}

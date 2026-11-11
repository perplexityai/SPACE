//! Net-gateway TCP benchmark and smoke harness.
//!
//! Three roles, one binary, so a single build is used on both sides of a
//! before/after comparison and only the linked gateway library differs:
//!
//! * `serve-upstream` — the destination, in the upstream namespace.
//! * `serve-gateway`  — the real TCP and UDP proxy loops, in the gateway
//!   namespace, on `IP_TRANSPARENT` sockets that TPROXY steers traffic to.
//! * `bench` / `smoke` — the guest, in the client namespace, driving the
//!   registered workloads and sampling the gateway's CPU and memory.
//!
//! `scripts/netgtw/bench/topology.sh` builds the namespaces the three roles
//! expect; `scripts/netgtw/bench/run.sh` sequences them.

mod client_hello;
mod gateway;
mod proc;
mod report;
mod runner;
mod upstream;
mod wire;
mod workload;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    name = "netgtw-bench",
    about = "SPACE net-gateway TCP bench and smoke harness"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List the registered workloads.
    List,
    /// Run the bench upstream: the destination workloads dial.
    ServeUpstream {
        /// Speaks the bench header protocol.
        #[arg(long, default_value_t = 9000)]
        port: u16,
        /// Echoes from the first byte, for the SNI workloads.
        #[arg(long, default_value_t = 9001)]
        raw_echo_port: u16,
    },
    /// Run the gateway under test.
    ServeGateway {
        #[arg(long, default_value = "00000000-0000-4000-8000-000000000000")]
        sandbox_id: String,
        #[arg(long, default_value_t = pplx_space_net_gateway::TCP_PROXY_PORT)]
        tcp_port: u16,
        #[arg(long, default_value_t = pplx_space_net_gateway::UDP_PROXY_PORT)]
        udp_port: u16,
        #[arg(long, value_enum, default_value_t = gateway::PolicyKind::AllowAll)]
        policy: gateway::PolicyKind,
        /// Written once both proxies are serving; carries this process's pid.
        #[arg(long)]
        ready_file: Option<PathBuf>,
    },
    /// Run every selected workload once and report pass or fail.
    Smoke(Box<DriveArgs>),
    /// Run every selected workload `--iterations` times and record the samples.
    Bench(Box<DriveArgs>),
    /// Render a recorded result file, or compare two of them.
    Report {
        /// The run to render, or the baseline when `--candidate` is given.
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long)]
        candidate: Option<PathBuf>,
    },
}

#[derive(Parser)]
struct DriveArgs {
    /// Upstream address speaking the bench header protocol.
    #[arg(long)]
    upstream: SocketAddr,
    /// Upstream address that echoes from the first byte.
    #[arg(long)]
    raw_echo: SocketAddr,
    /// A destination the platform guardrails hard-deny.
    #[arg(long, default_value = "169.254.169.254:80")]
    denied: SocketAddr,
    /// The gateway process to sample. Omit to skip CPU and memory sampling.
    #[arg(long)]
    gateway_pid: Option<u32>,
    /// Restrict to these workloads. Defaults to all of them.
    #[arg(long, value_delimiter = ',')]
    workloads: Vec<String>,
    /// Measured iterations per workload. At least one: a run of zero would
    /// report a table of dashes and exit successfully.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..))]
    iterations: u32,
    /// Discarded iterations run first, to settle allocators and page caches.
    /// Keep at one or more whenever `connection-limit` is in the set.
    #[arg(long, default_value_t = 1)]
    warmup: u32,
    /// Scales every size and count in the registry, as a percentage.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..))]
    scale: u32,
    /// Per-iteration deadline.
    #[arg(long, default_value_t = 180, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_secs: u64,
    /// Names this side of a comparison. Conventionally the gateway's git rev.
    #[arg(long, default_value = "unlabelled")]
    label: String,
    /// Where to write the raw samples.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_unset| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match cli.command {
        Command::List => {
            for entry in workload::REGISTRY {
                println!("{:<22} {}", entry.name, entry.summary);
            }
            Ok(())
        }
        Command::ServeUpstream {
            port,
            raw_echo_port,
        } => {
            let shutdown = CancellationToken::new();
            let listening = upstream::serve(port, raw_echo_port, shutdown.clone());
            tokio::select! {
                result = listening => result.map_err(Into::into),
                _signal = tokio::signal::ctrl_c() => {
                    shutdown.cancel();
                    Ok(())
                }
            }
        }
        Command::ServeGateway {
            sandbox_id,
            tcp_port,
            udp_port,
            policy,
            ready_file,
        } => {
            gateway::serve(gateway::GatewayArgs {
                sandbox_id,
                tcp_port,
                udp_port,
                policy,
                ready_file,
            })
            .await
        }
        Command::Smoke(args) => runner::drive(&args, runner::Mode::Smoke).await,
        Command::Bench(args) => runner::drive(&args, runner::Mode::Bench).await,
        Command::Report {
            baseline,
            candidate,
        } => {
            let baseline = runner::load(&baseline)?;
            match candidate {
                None => print!("{}", report::render(&baseline)),
                Some(candidate) => {
                    let candidate = runner::load(&candidate)?;
                    print!("{}", report::compare(&baseline, &candidate));
                }
            }
            Ok(())
        }
    }
}

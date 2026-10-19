use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use clap::Parser;
use pplx_space_node_mgr::{DEFAULT_NODE_CONFIG_PATH, NodeMgrRunConfig, SERVICE_NAME, SOCKET_PATH};

#[derive(Parser)]
#[command(
    name = "space-node-manager",
    about = "SPACE network manager daemon",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    /// Production node-manager Unix socket. Bound only after the service
    /// mutex is acquired.
    #[arg(long, default_value = SOCKET_PATH)]
    socket: PathBuf,

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
    // Cover predecessor termination grace plus surge readiness/scheduling slack.
    #[arg(long, default_value_t = 180_u64)]
    service_mutex_timeout_secs: u64,

    /// Node-manager TOML config path
    #[arg(long, default_value = DEFAULT_NODE_CONFIG_PATH)]
    config: PathBuf,

    /// Maximum number of network slots, which is the node's sandbox concurrency cap
    #[arg(long)]
    network_pool_max_slots: Option<u32>,

    /// Desired number of ready network slots maintained in the background
    #[arg(long)]
    network_pool_target: Option<u32>,

    /// Ready slots to keep after bursts before pruning surplus idle slots
    #[arg(long)]
    network_pool_retain_ready: Option<u32>,

    /// Minimum ready slots before the node should be treated as schedulable
    #[arg(long)]
    network_pool_min_ready: Option<u32>,

    /// Optional fixed pod network namespace path to prime every ready slot in.
    /// Do not use this for Kubernetes per-pod CNI namespaces.
    #[arg(long)]
    pod_netns_path: Option<String>,

    /// Service-port ingress isolation policy: auto, shared_host, or exclusive_host.
    #[arg(long)]
    service_port_ingress_isolation: Option<String>,

    /// Firecracker host cgroup memory.max in MiB for launched sandboxes.
    #[arg(long)]
    sandbox_cgroup_memory_mib: Option<u64>,

    /// Firecracker cgroup cpu.max vCPU count for launched sandboxes.
    #[arg(long)]
    sandbox_cgroup_vcpu_count: Option<u32>,

    /// Node memory kept available in addition to a new sandbox cgroup demand, in MiB.
    #[arg(long)]
    memory_admission_reserve_mib: Option<u64>,
}

impl From<Cli> for NodeMgrRunConfig {
    fn from(cli: Cli) -> Self {
        Self {
            socket_path: cli.socket,
            health_socket_path: cli.health_socket,
            service_mutex_timeout: Duration::from_secs(cli.service_mutex_timeout_secs),
            config_path: cli.config,
            network_pool_max_slots: cli.network_pool_max_slots,
            network_pool_target: cli.network_pool_target,
            network_pool_retain_ready: cli.network_pool_retain_ready,
            network_pool_min_ready: cli.network_pool_min_ready,
            pod_netns_path: cli.pod_netns_path,
            service_port_ingress_isolation: cli.service_port_ingress_isolation,
            sandbox_cgroup_memory_mib: cli.sandbox_cgroup_memory_mib,
            sandbox_cgroup_vcpu_count: cli.sandbox_cgroup_vcpu_count,
            memory_admission_reserve_mib: cli.memory_admission_reserve_mib,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // All runtime, telemetry and library threads must inherit this mask.
    #[cfg(target_os = "linux")]
    let signals = pplx_space_node_mgr::process_signals::ProcessShutdownSignals::install()
        .context("failed to route node-manager shutdown signals")?;
    let runtime = pplx_space_common_svc::tokio_runtime::build_multi_thread_runtime()
        .context("failed to configure node-manager Tokio runtime")?;
    runtime.block_on(async {
        let telemetry = pplx_space_common_svc::init_service(SERVICE_NAME)?;
        #[cfg(target_os = "linux")]
        let shutdown_token = signals.shutdown_token();
        #[cfg(not(target_os = "linux"))]
        let shutdown_token = pplx_space_common_svc::shutdown_token();
        let result = pplx_space_node_mgr::run(cli.into(), shutdown_token).await;
        pplx_space_common_svc::shutdown_telemetry(telemetry).await;
        Ok(result?)
    })
}

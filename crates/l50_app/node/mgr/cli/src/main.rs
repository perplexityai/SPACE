use clap::Parser;
use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};
use pplx_space_common_core::ids::sandbox_label_or_id;
use pplx_space_common_core::types::{OneOrMany, SandboxId, SandboxSelector};
use pplx_space_execution_common::environment::RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION as DEFINITION_SCHEMA;
use pplx_space_node_api_client::proto::GetControllerStateRequest;
use pplx_space_node_mgr_client::proto::{
    AllocateRequest, AllowPortRequest, CleanupSandboxRequest, DenyPortRequest, ExposePortRequest,
    InspectSandboxRequest, ListNetworkLeasesRequest, ListOrphanCandidatesRequest,
    ListOrphanCandidatesResponse, NetworkLease, NetworkPoolStatus, NetworkPoolStatusRequest,
    NetworkProtocol, NetworkSlotHealth, NetworkSlotState, ReleaseRequest, RepairNetworkPoolRequest,
    SetNetworkPoolConfigRequest, SetNetworkPoolDrainRequest, UnexposePortRequest,
};

const FACET_SANDBOX_DESIRED_STATE: &str = "sandbox-desired-state";

mod guest_channel;
#[cfg(test)]
mod tests;

/// Administrative commands capture one observation and submit that exact
/// identity. A concurrent replacement rejects the command; never fetch-and-retry.
async fn operator_execution_fence(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox_id: &str,
) -> Result<Option<pplx_space_node_mgr_client::proto::ExecutionFence>, tonic::Status> {
    let sandbox = sandbox_id
        .parse::<SandboxId>()
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    Ok(
        pplx_space_node_mgr_client::execution_fence::inspect_execution(client, sandbox)
            .await?
            .map(Into::into),
    )
}

fn unbound_execution_fence() -> pplx_space_node_mgr_client::proto::ExecutionFence {
    pplx_space_node_mgr_client::execution_fence::ExecutionFence::new(
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        None,
    )
    .into()
}

async fn collect_orphans(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
) -> Result<ListOrphanCandidatesResponse, tonic::Status> {
    let started = std::time::Instant::now();
    let mut token = String::new();
    let mut result = ListOrphanCandidatesResponse::default();
    let mut count = 0;
    // Bound even a changing or faulty peer. Keep partial results visible, but
    // never present a truncated walk as proof that no candidates exist.
    for _ in 0..4096 {
        let remaining = std::time::Duration::from_secs(60).saturating_sub(started.elapsed());
        let response = tokio::time::timeout(
            remaining.min(std::time::Duration::from_secs(10)),
            client.list_orphan_candidates(ListOrphanCandidatesRequest {
                page_token: token.clone(),
            }),
        )
        .await
        .map_err(|_| tonic::Status::deadline_exceeded("orphan inventory timed out"))?;
        let mut page = match response {
            Err(status) if status.code() == tonic::Code::ResourceExhausted => {
                let remaining =
                    std::time::Duration::from_secs(60).saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    return Err(status);
                }
                tokio::time::sleep(remaining.min(std::time::Duration::from_millis(250))).await;
                continue;
            }
            result => result?.into_inner(),
        };
        result.scan_started_at_unix_ms = page.scan_started_at_unix_ms;
        result.observed_at_unix_ms = page.observed_at_unix_ms;
        result.warnings.extend(
            page.warnings
                .drain(..)
                .take(8_usize.saturating_sub(result.warnings.len())),
        );
        for (target, items) in [
            (&mut result.orphaned_namespaces, page.orphaned_namespaces),
            (&mut result.orphaned_cgroups, page.orphaned_cgroups),
            (&mut result.orphaned_run_dirs, page.orphaned_run_dirs),
            (&mut result.orphaned_cache_dirs, page.orphaned_cache_dirs),
            (&mut result.orphaned_nft_rules, page.orphaned_nft_rules),
            (&mut result.orphaned_processes, page.orphaned_processes),
        ] {
            count += items.len();
            target.extend(items);
        }
        if count > 4096 {
            result
                .warnings
                .push("CLI result budget exceeded; showing partial inventory".into());
            return Ok(result);
        }
        if page.next_page_token.is_empty() {
            result.complete = page.complete;
            return Ok(result);
        }
        if page.next_page_token == token {
            return Err(tonic::Status::data_loss("orphan cursor made no progress"));
        }
        token = page.next_page_token;
    }
    result
        .warnings
        .push("CLI page budget exceeded; showing partial inventory".into());
    Ok(result)
}

#[derive(Parser)]
#[command(
    name = "space-nodemgr",
    about = "SPACE node manager CLI (debug tool)",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Connect to an execution-fenced guest TCP port or serial console (operator only).
    GuestChannel(guest_channel::Args),
    /// Resolve and verify an execution environment on this node; JSON output.
    Environment {
        #[arg(long)]
        reference: Option<pplx_space_common_core::types::EnvironmentReference>,
    },
    /// Show allocated network slots and health
    Status {
        /// Filter by sandbox ID(s)
        sandbox_ids: Vec<SandboxSelector>,
    },
    /// Inspect ALL resources for a sandbox (processes, network, cgroup, filesystem)
    Inspect {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
    },
    /// Read-only scan for resources without a node-manager lease; not deletion authority
    Orphans,
    /// Comprehensive cleanup of one sandbox (kill, network, cgroup, files, slot). Requires sudo.
    Cleanup {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Force cleanup even if processes are running
        #[arg(long)]
        force: bool,
    },
    /// Allocate a network slot (low-level, for debugging)
    Allocate {
        /// Sandbox ID (auto-generated if omitted)
        sandbox_id: Option<SandboxId>,
        /// Effective Firecracker host cgroup memory.max needed for the new VM, in MiB.
        #[arg(long)]
        sandbox_cgroup_memory_mib: std::num::NonZeroU64,
    },
    /// Release a network slot (low-level, for debugging)
    Release {
        /// Sandbox ID
        sandbox_id: SandboxSelector,
        #[arg(long)]
        force: bool,
    },
    /// Enable pool drain mode: reject new leases and stop background filling
    Drain,
    /// Disable pool drain mode and resume background filling
    Undrain,
    /// Nudge pool repair and optionally rebuild idle quarantined slots
    RepairPool {
        /// Rebuild quarantined slots that do not have an active sandbox lease
        #[arg(long)]
        rebuild_quarantined: bool,
    },
    /// Show or update live network-pool watermarks
    PoolConfig {
        /// Expand max slot rows. Shrinking max slots requires restart/manual cleanup.
        #[arg(long)]
        max_slots: Option<u32>,
        /// Desired number of ready slots maintained by the filler
        #[arg(long)]
        target: Option<u32>,
        /// Ready slots to retain after bursts before pruning surplus idle slots
        #[arg(long)]
        retain_ready: Option<u32>,
        /// Minimum ready slots before the node is treated as schedulable
        #[arg(long)]
        min_ready: Option<u32>,
    },
    /// Expose a host port for inbound traffic (install DNAT rule: host_port → guest_port)
    Expose {
        /// Sandbox ID
        sandbox_id: SandboxSelector,
        /// Host port to expose (1-65535)
        port: u16,
        /// Guest port (default: same as host port)
        #[arg(long)]
        guest_port: Option<u16>,
        /// Protocol: tcp or udp
        #[arg(long, default_value = "tcp")]
        protocol: String,
    },
    /// Unexpose a host port or all host ports
    Unexpose {
        /// Sandbox ID
        sandbox_id: SandboxSelector,
        /// Host port to unexpose
        port: Option<u16>,
        /// Remove all exposed host ports
        #[arg(long, conflicts_with = "port")]
        all: bool,
        /// Protocol: tcp or udp
        #[arg(long, default_value = "tcp")]
        protocol: String,
    },
    /// Allow inbound traffic to a guest port (open in sandbox firewall)
    Allow {
        /// Sandbox ID
        sandbox_id: SandboxSelector,
        /// Guest port to allow (1-65535)
        port: u16,
        /// Protocol: tcp or udp
        #[arg(long, default_value = "tcp")]
        protocol: String,
    },
    /// Deny inbound traffic to a guest port or all guest ports
    Deny {
        /// Sandbox ID
        sandbox_id: SandboxSelector,
        /// Guest port to deny
        port: Option<u16>,
        /// Deny all guest ports
        #[arg(long, conflicts_with = "port")]
        all: bool,
        /// Protocol: tcp or udp
        #[arg(long, default_value = "tcp")]
        protocol: String,
    },
}

fn parse_protocol(protocol: &str) -> Result<NetworkProtocol, String> {
    match protocol {
        "tcp" => Ok(NetworkProtocol::Tcp),
        "udp" => Ok(NetworkProtocol::Udp),
        other => Err(format!("ERROR: protocol must be tcp or udp, got {other}")),
    }
}

fn pool_status_name(status: i32) -> &'static str {
    match NetworkPoolStatus::try_from(status).unwrap_or(NetworkPoolStatus::Unspecified) {
        NetworkPoolStatus::Healthy => "healthy",
        NetworkPoolStatus::Degraded => "degraded",
        NetworkPoolStatus::Empty => "empty",
        NetworkPoolStatus::Draining => "draining",
        NetworkPoolStatus::Warming => "warming",
        NetworkPoolStatus::Unspecified => "unknown",
    }
}

fn slot_state_name(state: i32) -> &'static str {
    match NetworkSlotState::try_from(state).unwrap_or(NetworkSlotState::Unspecified) {
        NetworkSlotState::Missing => "missing",
        NetworkSlotState::Creating => "creating",
        NetworkSlotState::Ready => "ready",
        NetworkSlotState::Leasing => "leasing",
        NetworkSlotState::Leased => "leased",
        NetworkSlotState::Dirty => "dirty",
        NetworkSlotState::Resetting => "resetting",
        NetworkSlotState::Quarantined => "quarantined",
        NetworkSlotState::Destroying => "destroying",
        NetworkSlotState::Unspecified => "unknown",
    }
}

fn slot_health_name(health: i32) -> &'static str {
    match NetworkSlotHealth::try_from(health).unwrap_or(NetworkSlotHealth::Unspecified) {
        NetworkSlotHealth::Unknown => "unknown",
        NetworkSlotHealth::Healthy => "healthy",
        NetworkSlotHealth::Degraded => "degraded",
        NetworkSlotHealth::Invalid => "invalid",
        NetworkSlotHealth::Unspecified => "unknown",
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    pplx_space_common_cli::init_logging("space-nodemgr");
    match run(Cli::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let mut client = pplx_space_node_mgr_client::connect()
        .await
        .map_err(|error| {
            format!(
                "ERROR: cannot connect to node-manager (is space-nodemgr-svc running?): {error}"
            )
        })?;

    match cli.command {
        Commands::GuestChannel(args) => guest_channel::run(&mut client, args)
            .await
            .map_err(|error| error.to_string())?,
        Commands::Environment { reference } => {
            let response = client
                .resolve_execution_environment(
                    pplx_space_node_mgr_client::proto::ResolveExecutionEnvironmentRequest {
                        definition_schema_version: DEFINITION_SCHEMA,
                        compatible: None,
                        reference: reference.map(|value| value.to_string()),
                    },
                )
                .await
                .map_err(|error| error.to_string())?
                .into_inner();
            let definition =
                pplx_space_execution_common::environment::EnvironmentDefinition::try_from(
                    response.definition.as_ref().ok_or("missing definition")?,
                )
                .map_err(|error| error.to_string())?;
            let execution = pplx_space_common_core::types::ExecutionMetadata::try_from(
                response.execution.as_ref().ok_or("missing execution")?,
            )
            .map_err(|error| error.to_string())?;
            if reference.is_some_and(|reference| reference != definition.reference())
                || execution.environment() != Some(definition.reference())
            {
                return Err("node did not acknowledge the requested environment".into());
            }
            let execution = definition
                .bind_execution(execution)
                .map_err(|error| error.to_string())?;
            let document: serde_json::Value = serde_json::from_str(
                &definition
                    .to_document()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            println!(
                "{}",
                serde_json::json!({"definition": document, "execution": execution})
            );
        }
        Commands::Status { sandbox_ids } => {
            let pool = client
                .get_network_pool_status(NetworkPoolStatusRequest {})
                .await
                .map_err(|e| format!("ERROR: {e}"))?
                .into_inner();
            let leases = client
                .list_network_leases(ListNetworkLeasesRequest {})
                .await
                .map_err(|e| format!("ERROR: {e}"))?
                .into_inner()
                .leases;
            let leases = filter_leases(leases, &sandbox_ids)?;

            println!(
                "Pool: status={} ready={} leased={} dirty={} resetting={} creating={} quarantined={} degraded={} target={} retain_ready={} min_ready={} max={}",
                pool_status_name(pool.network_pool_status),
                pool.network_slots_ready,
                pool.network_slots_leased,
                pool.network_slots_dirty,
                pool.network_slots_resetting,
                pool.network_slots_creating,
                pool.network_slots_quarantined,
                pool.network_slots_degraded,
                pool.network_pool_target,
                pool.network_pool_retained_ready,
                pool.network_pool_min_ready,
                pool.network_pool_max_slots,
            );

            if leases.is_empty() {
                println!("No network leases.");
            } else {
                let mut table = Table::new();
                table.load_style(UTF8_FULL_CONDENSED);
                table.set_header(vec![
                    "Slot",
                    "Sandbox",
                    "State",
                    "Health",
                    "Namespace",
                    "sveth",
                    "vpeer",
                    "PID",
                    "UID",
                    "Pod NS",
                    "Err",
                ]);

                for lease in &leases {
                    table.add_row(vec![
                        lease.slot.to_string(),
                        sandbox_label_or_id(&lease.sandbox_id),
                        slot_state_name(lease.slot_state).to_string(),
                        slot_health_name(lease.slot_health).to_string(),
                        lease.namespace.clone(),
                        lease.sveth.clone(),
                        lease.vpeer.clone(),
                        lease
                            .pid
                            .map(|pid| pid.to_string())
                            .unwrap_or_else(|| "-".into()),
                        lease
                            .sandbox_uid
                            .map(|uid| uid.to_string())
                            .unwrap_or_else(|| "-".into()),
                        lease.pod_netns_path.as_deref().unwrap_or("-").to_string(),
                        lease.last_error.as_deref().unwrap_or("-").to_string(),
                    ]);
                }

                println!("{table}");
            }
        }

        Commands::Inspect { sandbox_id } => {
            let sandbox_id = resolve_sandbox_selector(&mut client, &sandbox_id).await?;
            let resp = client
                .inspect_sandbox(InspectSandboxRequest {
                    sandbox_id: sandbox_id.clone(),
                    uuid8: String::new(),
                    uid: None,
                    slot: None,
                    template: None,
                    execution_only: false,
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?
                .into_inner();

            let yn = |b: bool| if b { "exists" } else { "MISSING" };
            let alive = |b: bool| if b { "alive" } else { "dead" };

            println!("Sandbox: {}", resp.sandbox_id);
            if let Some(fence) = &resp.execution_fence {
                println!("Execution: {}", fence.execution_id);
            } else {
                println!("Execution: unbound");
            }
            println!(
                "Slot:    {}",
                resp.slot
                    .map(|slot| slot.to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
            println!(
                "UID:     {}",
                resp.uid
                    .map(|uid| uid.to_string())
                    .unwrap_or_else(|| "-".to_string())
            );
            println!("Template:{}", resp.template.as_deref().unwrap_or("-"));
            println!();

            println!("PROCESSES:");
            if let Some(runtime_pid) = resp.runtime_pid {
                println!(
                    "  Runtime PID:      {}    {}",
                    runtime_pid,
                    alive(resp.runtime_alive)
                );
            } else {
                println!("  Runtime PID:      -");
            }
            if let Some(spaced_host_pid) = resp.spaced_host_pid {
                println!(
                    "  spaced-host PID:  {}    {}",
                    spaced_host_pid,
                    alive(resp.spaced_host_alive)
                );
            } else {
                println!("  spaced-host PID:  -");
            }
            println!();

            println!("NETWORK:");
            println!("  Namespace:        {}", yn(resp.namespace_exists));
            println!("  Host veth:        {}", yn(resp.sveth_exists));
            println!("  NS veth:          {}", yn(resp.vpeer_exists));
            println!("  TAP:              {}", yn(resp.tap_exists));
            println!("  Host route:       {}", yn(resp.host_route_exists));
            println!("  nft FORWARD:      {}", yn(resp.nft_forward_exists));
            println!("  nft NS SNAT:      {}", yn(resp.nft_ns_snat_exists));
            println!("  nft NS DNAT:      {}", yn(resp.nft_ns_dnat_exists));
            println!(
                "  NS ip_forward:    {}",
                if resp.ns_ip_forward { "1" } else { "-" }
            );
            println!(
                "  NS rp_filter:     {}",
                if resp.ns_rp_filter_off { "0 (ok)" } else { "-" }
            );
            println!();

            println!("CGROUP:");
            println!("  Path:             {}", yn(resp.cgroup_exists));
            if resp.cgroup_exists {
                println!("  cpu.max:          {}", resp.cgroup_cpu_max);
                println!("  memory.max:       {}", resp.cgroup_memory_max);
                println!("  memory.current:   {}", resp.cgroup_memory_current);
                println!("  pids.max:         {}", resp.cgroup_pids_max);
                println!("  pids.current:     {}", resp.cgroup_pids_current);
            }
            println!();

            println!("FILESYSTEM:");
            println!("  Cache dir:        {}", yn(resp.cache_dir_exists));
            println!("  Run dir:          {}", yn(resp.run_dir_exists));
            println!("  Isolation root:   {}", yn(resp.isolation_root_exists));
            println!("  vm-config.json:   {}", yn(resp.vm_config_exists));
            println!("  Runtime log:      {}", yn(resp.runtime_log_exists));
            println!("  vsock.sock:       {}", yn(resp.vsock_sock_exists));
            println!("  spaced-host.sock: {}", yn(resp.spaced_sock_exists));
            println!("  rootfs.ext4:      {}", yn(resp.rootfs_exists));
            println!("  Runtime PID link: {}", yn(resp.runtime_pid_link_exists));
            println!(
                "  spaced-host.pid:  {}",
                yn(resp.spaced_host_pid_symlink_exists)
            );
            println!("  Runtime PID file: {}", yn(resp.runtime_pidfile_exists));
            println!();

            println!("STATE:");
            println!(
                "  Network slot:     {}",
                if resp.slot_allocated {
                    "allocated"
                } else {
                    "not allocated"
                }
            );
        }

        Commands::Orphans => {
            let resp = collect_orphans(&mut client)
                .await
                .map_err(|e| format!("ERROR: {e}"))?;

            for warning in &resp.warnings {
                eprintln!("INCOMPLETE: {warning}");
            }
            if !resp.complete {
                eprintln!("Orphan scan is incomplete; absence is unknown.");
            }

            let mut found = false;

            if !resp.orphaned_namespaces.is_empty() {
                found = true;
                println!("Orphaned namespaces:");
                for ns in &resp.orphaned_namespaces {
                    println!("  {ns}");
                }
            }
            if !resp.orphaned_cgroups.is_empty() {
                found = true;
                println!("Orphaned cgroups:");
                for cg in &resp.orphaned_cgroups {
                    println!("  {cg}");
                }
            }
            if !resp.orphaned_run_dirs.is_empty() {
                found = true;
                println!("Orphaned run directories:");
                for d in &resp.orphaned_run_dirs {
                    println!("  {d}");
                }
            }
            if !resp.orphaned_cache_dirs.is_empty() {
                found = true;
                println!("Orphaned cache directories:");
                for d in &resp.orphaned_cache_dirs {
                    println!("  {d}");
                }
            }
            if !resp.orphaned_nft_rules.is_empty() {
                found = true;
                println!("Orphaned nft FORWARD rules (interface doesn't exist):");
                for r in &resp.orphaned_nft_rules {
                    println!("  {r}");
                }
            }

            if !resp.orphaned_processes.is_empty() {
                found = true;
                println!("Orphaned sandbox processes (PID:sandbox_id):");
                for p in &resp.orphaned_processes {
                    println!("  {p}");
                }
            }

            if !found && resp.complete {
                println!("No orphaned resources found.");
            }
            if found {
                println!(
                    "Candidates are not deletion authority. Inspect lifecycle ownership before explicit cleanup."
                );
            }
        }

        Commands::Cleanup { sandbox_id, force } => {
            let sandbox_id = resolve_sandbox_selector(&mut client, &sandbox_id).await?;
            let execution_fence = operator_execution_fence(&mut client, &sandbox_id)
                .await
                .map_err(|error| error.to_string())?;
            let resp = client
                .cleanup_sandbox(CleanupSandboxRequest {
                    execution_fence: Some(execution_fence.unwrap_or_else(unbound_execution_fence)),
                    sandbox_id: sandbox_id.clone(),
                    force,
                    keep_filesystem: false,
                    return_after_slot_release: false,
                    immediate_kill: false,
                })
                .await
                .map_err(|error| cleanup_error("cleanup", error))?
                .into_inner();

            if resp.actions.is_empty() {
                println!(
                    "Nothing to clean up for {}",
                    sandbox_label_or_id(&sandbox_id)
                );
            } else {
                for action in &resp.actions {
                    println!("  {action}");
                }
                println!("Cleanup complete.");
            }
        }

        Commands::Allocate {
            sandbox_id,
            sandbox_cgroup_memory_mib,
        } => {
            let sandbox_id = sandbox_id.unwrap_or_else(SandboxId::new).to_string();
            let execution_fence = operator_execution_fence(&mut client, &sandbox_id)
                .await
                .map_err(|error| error.to_string())?;
            let resp = client
                .allocate(AllocateRequest {
                    execution_fence: Some(execution_fence.unwrap_or_else(unbound_execution_fence)),
                    sandbox_id: sandbox_id.clone(),
                    netns_path: None,
                    sandbox_cgroup_memory_mib: sandbox_cgroup_memory_mib.get(),
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?
                .into_inner();

            println!(
                "Allocated slot {} for {}",
                resp.slot,
                sandbox_label_or_id(&sandbox_id)
            );
            println!("  Namespace: {}", resp.namespace);
            println!("  Guest IP:  {}", resp.guest_ip);
            println!("  UID:       {}", resp.sandbox_uid);
            println!("  Generation: {}", resp.slot_generation);
        }

        Commands::Release { sandbox_id, force } => {
            let sandbox_id = resolve_sandbox_selector(&mut client, &sandbox_id).await?;
            let execution_fence = operator_execution_fence(&mut client, &sandbox_id)
                .await
                .map_err(|error| error.to_string())?
                .ok_or("sandbox has no execution binding to release")?;
            client
                .release(ReleaseRequest {
                    execution_fence: Some(execution_fence),
                    sandbox_id: sandbox_id.clone(),
                    force,
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?;

            println!("Released {}", sandbox_label_or_id(&sandbox_id));
        }

        Commands::Drain => {
            let resp = client
                .set_network_pool_drain(SetNetworkPoolDrainRequest { draining: true })
                .await
                .map_err(|e| format!("ERROR: {e}"))?
                .into_inner();
            println!("Network pool draining: {}", resp.draining);
        }

        Commands::Undrain => {
            let resp = client
                .set_network_pool_drain(SetNetworkPoolDrainRequest { draining: false })
                .await
                .map_err(|e| format!("ERROR: {e}"))?
                .into_inner();
            println!("Network pool draining: {}", resp.draining);
        }

        Commands::RepairPool {
            rebuild_quarantined,
        } => {
            let resp = client
                .repair_network_pool(RepairNetworkPoolRequest {
                    rebuild_quarantined,
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?
                .into_inner();
            println!(
                "Network pool repair requested: reset_enqueued={} topup_requested={}",
                resp.reset_enqueued, resp.topup_requested
            );
        }

        Commands::PoolConfig {
            max_slots,
            target,
            retain_ready,
            min_ready,
        } => {
            if max_slots.is_none()
                && target.is_none()
                && retain_ready.is_none()
                && min_ready.is_none()
            {
                let resp = client
                    .get_network_pool_status(NetworkPoolStatusRequest {})
                    .await
                    .map_err(|e| format!("ERROR: {e}"))?
                    .into_inner();
                println!(
                    "Network pool config: max_slots={} target_ready_slots={} retained_ready_slots={} min_ready_slots={} pod_netns_path={}",
                    resp.network_pool_max_slots,
                    resp.network_pool_target,
                    resp.network_pool_retained_ready,
                    resp.network_pool_min_ready,
                    resp.network_pool_pod_netns_path.as_deref().unwrap_or("-"),
                );
            } else {
                let resp = client
                    .set_network_pool_config(SetNetworkPoolConfigRequest {
                        max_slots,
                        target_ready_slots: target,
                        retained_ready_slots: retain_ready,
                        min_ready_slots: min_ready,
                    })
                    .await
                    .map_err(|e| format!("ERROR: {e}"))?
                    .into_inner();
                let Some(config) = resp.config else {
                    return Err("ERROR: node-manager returned an empty config response".into());
                };
                println!(
                    "Network pool config: max_slots={} target_ready_slots={} retained_ready_slots={} min_ready_slots={} pod_netns_path={}",
                    config.max_slots,
                    config.target_ready_slots,
                    config.retained_ready_slots,
                    config.min_ready_slots,
                    config.pod_netns_path.as_deref().unwrap_or("-"),
                );
            }
        }

        Commands::Expose {
            sandbox_id,
            port,
            guest_port,
            protocol,
        } => {
            let sandbox_id = resolve_sandbox_selector(&mut client, &sandbox_id).await?;
            let gp = guest_port.unwrap_or(port);
            warn_if_inbound_reconciliation_active(FACET_SANDBOX_DESIRED_STATE).await;
            client
                .expose_port(ExposePortRequest {
                    sandbox_id: sandbox_id.clone(),
                    port: port as u32,
                    protocol: parse_protocol(&protocol)?.into(),
                    guest_port: guest_port.map(|guest_port| guest_port as u32),
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?;

            let sid = sandbox_label_or_id(&sandbox_id);
            println!("Exposed host:{port} → guest:{gp} for {sid}");
        }

        Commands::Unexpose {
            sandbox_id,
            port,
            all,
            protocol,
        } => {
            let sandbox_id = resolve_sandbox_selector(&mut client, &sandbox_id).await?;
            let port = resolve_optional_port(port, all, "unexpose")?;
            warn_if_inbound_reconciliation_active(FACET_SANDBOX_DESIRED_STATE).await;
            client
                .unexpose_port(UnexposePortRequest {
                    sandbox_id: sandbox_id.clone(),
                    port: port.map(|port| port as u32),
                    protocol: parse_protocol(&protocol)?.into(),
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?;

            let sid = sandbox_label_or_id(&sandbox_id);
            if let Some(port) = port {
                println!("Unexposed host:{port} for {sid}");
            } else {
                println!("Unexposed all ports for {sid}");
            }
        }

        Commands::Allow {
            sandbox_id,
            port,
            protocol,
        } => {
            let sandbox_id = resolve_sandbox_selector(&mut client, &sandbox_id).await?;
            warn_if_inbound_reconciliation_active(FACET_SANDBOX_DESIRED_STATE).await;
            client
                .allow_port(AllowPortRequest {
                    sandbox_id: sandbox_id.clone(),
                    port: port as u32,
                    protocol: parse_protocol(&protocol)?.into(),
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?;

            println!(
                "Allowed guest:{port} for {}",
                sandbox_label_or_id(&sandbox_id)
            );
        }

        Commands::Deny {
            sandbox_id,
            port,
            all,
            protocol,
        } => {
            let sandbox_id = resolve_sandbox_selector(&mut client, &sandbox_id).await?;
            let port = resolve_optional_port(port, all, "deny")?;
            warn_if_inbound_reconciliation_active(FACET_SANDBOX_DESIRED_STATE).await;
            client
                .deny_port(DenyPortRequest {
                    sandbox_id: sandbox_id.clone(),
                    port: port.map(|port| port as u32),
                    protocol: parse_protocol(&protocol)?.into(),
                })
                .await
                .map_err(|e| format!("ERROR: {e}"))?;

            let sid = sandbox_label_or_id(&sandbox_id);
            if let Some(port) = port {
                println!("Denied guest:{port} for {sid}");
            } else {
                println!("Denied all guest ports for {sid}");
            }
        }
    }
    Ok(())
}

fn cleanup_error(command: &str, error: tonic::Status) -> String {
    let mut message = format!("ERROR: {error}");
    if error.code() == tonic::Code::PermissionDenied {
        message.push_str(&format!("\nhint: `{command}` calls node-manager CleanupSandbox, which is restricted to root/admin or the sandbox-manager service account.\nhint: run `sudo space-nodemgr {command}` from the deploy account."));
    }
    message
}

async fn warn_if_inbound_reconciliation_active(facet: &str) {
    let Ok(mut node_ctl) = pplx_space_node_api_client::connect_node_ctl().await else {
        return;
    };
    let Ok(response) = node_ctl
        .get_controller_state(GetControllerStateRequest {
            facet: Some(facet.to_string()),
            include_queue: false,
        })
        .await
    else {
        return;
    };
    let state = response.into_inner();
    if !state.cluster_sync_configured {
        return;
    }
    let active = state
        .facets
        .iter()
        .any(|row| row.facet == facet && row.supports_inbound && row.inbound_pause.is_none());
    if active {
        eprintln!(
            "warning: this setting will be overridden by nodectl reconciliation for facet '{facet}'"
        );
    }
}

fn filter_leases(
    leases: Vec<NetworkLease>,
    selectors: &[SandboxSelector],
) -> Result<Vec<NetworkLease>, String> {
    let mut selected = Vec::new();
    for lease in leases
        .into_iter()
        .filter(|lease| !lease.sandbox_id.is_empty())
    {
        if selectors.is_empty() || {
            let id = parse_lease_sandbox_id(&lease)?;
            selectors.iter().any(|selector| selector.matches_id(id))
        } {
            selected.push(lease);
        }
    }
    Ok(selected)
}

fn parse_lease_sandbox_id(lease: &NetworkLease) -> Result<SandboxId, String> {
    lease.sandbox_id.parse().map_err(|error| {
        format!(
            "ERROR: node-manager returned invalid sandbox id '{}': {error}",
            lease.sandbox_id
        )
    })
}

fn exact_sandbox_id_from_selector(selector: &SandboxSelector) -> Option<String> {
    selector
        .as_str()
        .parse::<SandboxId>()
        .ok()
        .map(|id| id.to_string())
}

async fn resolve_sandbox_selector(
    client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    selector: &SandboxSelector,
) -> Result<String, String> {
    let leases = client
        .list_network_leases(ListNetworkLeasesRequest {})
        .await
        .map_err(|e| format!("ERROR: {e}"))?
        .into_inner()
        .leases;
    resolve_lease_selector(leases, selector)
}

fn resolve_lease_selector(
    leases: Vec<NetworkLease>,
    selector: &SandboxSelector,
) -> Result<String, String> {
    // A retired execution's slot may still be cleaning when its sandbox
    // starts again. Select the sandbox identity, not an individual lease.
    let sandboxes = leases
        .iter()
        .map(parse_lease_sandbox_id)
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    match selector.find(sandboxes, |id| *id) {
        Some(OneOrMany::One(id)) => Ok(id.to_string()),
        Some(OneOrMany::Many(matches)) => {
            let mut message = format!("Ambiguous sandbox selector '{selector}', matches:");
            for id in matches {
                message.push_str(&format!("\n  {}", sandbox_label_or_id(&id.to_string())));
            }
            Err(message)
        }
        None => exact_sandbox_id_from_selector(selector)
            .ok_or_else(|| format!("No sandbox matching '{selector}'")),
    }
}

fn resolve_optional_port(
    port: Option<u16>,
    all: bool,
    command: &str,
) -> Result<Option<u16>, String> {
    match (port, all) {
        (Some(0), _) => Err(format!(
            "ERROR: {command} port must be 1-65535; use --all for all ports"
        )),
        (Some(port), false) => Ok(Some(port)),
        (None, true) => Ok(None),
        (None, false) => Err(format!("ERROR: {command} requires a port or --all")),
        (Some(_), true) => unreachable!("clap conflicts_with prevents port with --all"),
    }
}

use super::*;
use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};
use std::collections::HashMap;

pub(super) async fn handle(command: Commands, client: &mut SandboxClient) {
    match command {
        Commands::List => handle_list(client).await,
        Commands::Stats { sandbox_id } => handle_stats(client, sandbox_id).await,
        Commands::Health { sandbox_id } => handle_health(client, sandbox_id).await,
        Commands::DiskUsage { sandbox_id } => handle_disk_usage(client, sandbox_id).await,
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}

async fn handle_health(client: &mut SandboxClient, sandbox_id: SandboxSelector) {
    let sandbox = resolve_sandbox_state(client, &sandbox_id).await;
    let response = client
        .get_sandbox_health(GetSandboxHealthRequest {
            sandbox_id: sandbox.sandbox_id,
        })
        .await
        .unwrap_or_else(|error| exit_error(error))
        .into_inner();
    let status = pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::try_from(response.status)
        .unwrap_or(pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Unspecified);
    println!("Sandbox: {}", response.sandbox_id);
    println!("Health:  {}", status.as_str_name());
    if let Some(evaluated_at) = response.evaluated_at.as_ref() {
        println!("Updated: {}", format_relative_timestamp(evaluated_at));
    }
    println!();

    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(["COMPONENT", "WHEN", "OUTCOME", "LATENCY", "DETAIL"]);
    for component in response.components {
        let component_name =
            pplx_space_sbx_mgr_client::proto::SandboxHealthComponent::try_from(component.component)
                .unwrap_or(pplx_space_sbx_mgr_client::proto::SandboxHealthComponent::Unspecified)
                .as_str_name();
        for observation in component.observations {
            let outcome = pplx_space_sbx_mgr_client::proto::SandboxHealthProbeOutcome::try_from(
                observation.outcome,
            )
            .unwrap_or(pplx_space_sbx_mgr_client::proto::SandboxHealthProbeOutcome::Unspecified);
            table.add_row([
                component_name.to_string(),
                observation
                    .observed_at
                    .as_ref()
                    .map_or_else(|| "-".to_string(), format_relative_timestamp),
                outcome.as_str_name().to_string(),
                format!("{}us", observation.latency_micros),
                observation.detail,
            ]);
        }
    }
    println!("{table}");
}

async fn handle_list(client: &mut SandboxClient) {
    let template_hashes = template_hashes(client).await;
    let listed = pplx_space_sbx_mgr_client::list_all_sandboxes(client)
        .await
        .unwrap_or_else(|error| exit_error(error));

    if listed.is_empty() {
        println!("No sandboxes found.");
        return;
    }
    let listed_ids = listed
        .iter()
        .map(|sandbox| sandbox.sandbox_id.clone())
        .collect::<Vec<_>>();
    let listed_by_id = listed
        .iter()
        .map(|sandbox| (sandbox.sandbox_id.clone(), sandbox))
        .collect::<std::collections::HashMap<_, _>>();
    let state = client
        .state(StateRequest {
            sandbox_ids: listed_ids.clone(),
        })
        .await
        .unwrap_or_else(|error| exit_error(error))
        .into_inner();
    let mut sandboxes = state.sandboxes;
    for missing_id in state.missing_ids {
        if let Some(listed) = listed_by_id.get(&missing_id) {
            sandboxes.push(SandboxInfo {
                sandbox_id: listed.sandbox_id.clone(),
                template: listed.template.clone(),
                lifecycle: listed.lifecycle,
                owner: listed.owner.clone(),
                image_hash: listed.image_hash.clone(),
                created_at: listed.created_at.clone(),
                runtime: listed.runtime.clone(),
                pool_template: listed.pool_template.clone(),
                ..Default::default()
            });
        }
    }
    let disk_usage = client
        .get_disk_usage(GetDiskUsageRequest {
            sandbox_ids: listed_ids,
        })
        .await
        .ok()
        .map(|response| response.into_inner().usage_bytes)
        .unwrap_or_default();

    println!(
        "{}",
        sandbox_list_table(&template_hashes, &sandboxes, &disk_usage)
    );
}

async fn template_hashes(client: &mut SandboxClient) -> HashMap<String, String> {
    client
        .list_templates(ListTemplatesRequest::default())
        .await
        .ok()
        .map(|r| {
            r.into_inner()
                .templates
                .into_iter()
                .map(|t| (t.id, t.sha256))
                .collect()
        })
        .unwrap_or_default()
}

fn sandbox_list_table(
    template_hashes: &HashMap<String, String>,
    sandboxes: &[SandboxInfo],
    disk_usage: &HashMap<String, u64>,
) -> Table {
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec![
        "ID (short)",
        "BACKEND",
        "STATE",
        "HP",
        "AGE",
        "TEMPLATE",
        "AUTO",
        "NEXT AUTO",
        "TMPL_HASH",
        "PID",
        "DISK (CENSUS)",
        "SNAPS",
        "UID",
        "SLOT",
        "NAMESPACE",
        "UPTIME",
        "USER",
    ]);

    for sandbox in sandboxes {
        table.add_row(sandbox_list_row(template_hashes, disk_usage, sandbox));
    }

    table
}

fn sandbox_list_row(
    template_hashes: &HashMap<String, String>,
    disk_usage: &HashMap<String, u64>,
    sandbox: &SandboxInfo,
) -> Vec<String> {
    let auto_policy = sandbox
        .auto_lifecycle
        .as_ref()
        .map(format_policy_from_proto);
    let next_auto_event = sandbox
        .auto_lifecycle_status
        .as_ref()
        .and_then(|status| status.next_auto_event_at.as_ref())
        .map_or_else(|| "-".into(), format_relative_timestamp);

    vec![
        sandbox_label(&sandbox.sandbox_id),
        sandbox.runtime.clone().unwrap_or_else(|| "fc".into()),
        sandbox
            .observed_lifecycle()
            .map(|state| state.label().to_string())
            .unwrap_or_else(|_| "unknown".into()),
        sandbox_health(sandbox),
        sandbox
            .created_at
            .as_deref()
            .map_or_else(|| "-".into(), format_age),
        sandbox.template.clone().unwrap_or_else(|| "-".into()),
        auto_policy.unwrap_or_else(|| "-".into()),
        next_auto_event,
        template_hash_display(sandbox, template_hashes),
        optional_u32(sandbox.pid),
        disk_usage
            .get(&sandbox.sandbox_id)
            .map_or_else(|| "-".into(), |bytes| format_bytes(*bytes)),
        snapshots_label(sandbox.snapshots_enabled),
        optional_u32(sandbox.uid),
        optional_u32(sandbox.slot),
        sandbox.namespace.clone().unwrap_or_else(|| "-".into()),
        sandbox.uptime.clone().unwrap_or_else(|| "-".into()),
        sandbox.owner.clone().unwrap_or_else(|| "-".into()),
    ]
}

fn template_hash_display(
    sandbox: &SandboxInfo,
    template_hashes: &HashMap<String, String>,
) -> String {
    let Some(image_hash) = sandbox.image_hash.as_ref() else {
        return "-".into();
    };
    let hash_display = image_hash
        .strip_prefix("sha256:")
        .or_else(|| image_hash.strip_prefix("blake3:"))
        .unwrap_or(image_hash);
    let short = hash_display.chars().take(12).collect::<String>();
    let matches_current = template_hashes
        .get(sandbox.template.as_deref().unwrap_or(""))
        .is_some_and(|h| h == image_hash);
    if matches_current {
        short
    } else {
        format!("{short}*")
    }
}

fn sandbox_health(sandbox: &SandboxInfo) -> String {
    if !sandbox.observed_lifecycle().is_ok_and(|state| {
        matches!(
            state,
            pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Started
                | pplx_space_sbx_mgr_client::proto::SandboxLifecycleState::Paused
        )
    }) {
        return "-".into();
    }
    match pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::try_from(sandbox.health_status)
        .unwrap_or(pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Unspecified)
    {
        pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Healthy => "\u{2705}".to_string(),
        pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Degraded => {
            "\u{26a0}\u{fe0f}".to_string()
        }
        pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Initializing
        | pplx_space_sbx_mgr_client::proto::SandboxHealthStatus::Unspecified => {
            "\u{23f3}".to_string()
        }
    }
}

fn optional_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "-".into(), |value| value.to_string())
}

fn snapshots_label(enabled: bool) -> String {
    if enabled { "on".into() } else { "-".into() }
}

async fn handle_stats(client: &mut SandboxClient, sandbox_id: SandboxSelector) {
    let sandbox = resolve_sandbox_state(client, &sandbox_id).await;
    let full_id = sandbox.sandbox_id.clone();
    let short_id = sandbox_label(&full_id);
    println!(
        "Sandbox: {short_id}  ({})",
        sandbox
            .observed_lifecycle()
            .map(|state| state.label())
            .unwrap_or("unknown")
    );
    println!();

    match client
        .stats(StatsRequest {
            sandbox_id: full_id.clone(),
        })
        .await
    {
        Ok(resp) => print_stats(resp.into_inner(), sandbox.netgtw_alive),
        Err(e) => {
            eprintln!("ERROR: cannot get stats: {e}");
        }
    }
}

fn print_stats(stats: pplx_space_sbx_mgr_client::proto::StatsResponse, netgtw_alive: bool) {
    let network_scope = if stats.net_gateway_reachable {
        "NETWORK (net-gateway via sbx-manager)"
    } else if netgtw_alive {
        "NETWORK (persisted; live net-gateway stats unavailable)"
    } else {
        "NETWORK (persisted; net-gateway not running)"
    };
    println!("{network_scope}");
    println!("{}", network_stats_table(&stats));
    println!();
    println!("CGROUP");
    println!("{}", cgroup_stats_table(&stats));
}

fn network_stats_table(stats: &pplx_space_sbx_mgr_client::proto::StatsResponse) -> Table {
    let mut table = metric_table();
    if stats.net_gateway_reachable {
        table.add_row(vec![
            "TCP active".to_string(),
            stats.tcp_active_connections.to_string(),
        ]);
        table.add_row(vec![
            "TCP total".to_string(),
            stats.tcp_total_connections.to_string(),
        ]);
        table.add_row(vec![
            "TCP denied".to_string(),
            stats.tcp_denied_connections.to_string(),
        ]);
        table.add_row(vec![
            "UDP active".to_string(),
            stats.udp_active_mappings.to_string(),
        ]);
    }
    table.add_row(vec![
        "Network TX".to_string(),
        format_bytes(stats.network_tx_bytes),
    ]);
    table.add_row(vec![
        "Network RX".to_string(),
        format_bytes(stats.network_rx_bytes),
    ]);
    if stats.net_gateway_reachable {
        table.add_row(vec![
            "Bytes forwarded".to_string(),
            format_bytes(stats.bytes_forwarded),
        ]);
    }
    table
}

fn cgroup_stats_table(stats: &pplx_space_sbx_mgr_client::proto::StatsResponse) -> Table {
    let mut table = metric_table();
    table.add_row(vec!["CPU max".to_string(), stats.cpu_max.clone()]);
    table.add_row(vec![
        "CPU usage".to_string(),
        format!("{} usec", stats.cpu_usage_usec),
    ]);
    table.add_row(vec![
        "Memory current".to_string(),
        format_bytes(stats.memory_current),
    ]);
    table.add_row(vec![
        "Memory max".to_string(),
        format_bytes(stats.memory_max),
    ]);
    table.add_row(vec![
        "PIDs current".to_string(),
        stats.pids_current.to_string(),
    ]);
    table.add_row(vec!["PIDs max".to_string(), stats.pids_max.to_string()]);
    table
}

fn metric_table() -> Table {
    let mut table = Table::new();
    table.load_style(UTF8_FULL_CONDENSED);
    table.set_header(vec!["Metric", "Value"]);
    table
}

async fn handle_disk_usage(client: &mut SandboxClient, sandbox_id: SandboxSelector) {
    let sandbox = resolve_sandbox_list_element(client, &sandbox_id).await;
    let response = client
        .get_disk_usage(GetDiskUsageRequest {
            sandbox_ids: vec![sandbox.sandbox_id.clone()],
        })
        .await
        .unwrap_or_else(|error| exit_error(error))
        .into_inner();
    match response.usage_bytes.get(&sandbox.sandbox_id) {
        Some(bytes) => {
            println!(
                "{} {} (last node disk census)",
                sandbox_label(&sandbox.sandbox_id),
                format_bytes(*bytes)
            )
        }
        None if response
            .missing_ids
            .iter()
            .any(|missing| missing.id == sandbox.sandbox_id) =>
        {
            exit_error(format!(
                "instance path missing for {} at the last node disk census",
                sandbox_label(&sandbox.sandbox_id)
            ));
        }
        None => {
            exit_error(format!(
                "disk usage not measured for {} in the last node disk census",
                sandbox_label(&sandbox.sandbox_id)
            ));
        }
    }
}

#[cfg(test)]
mod tests;

use super::*;

pub(super) async fn warn_if_inbound_reconciliation_active(facet: &str) {
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

pub(super) async fn connect_template_manager() -> Result<
    pplx_space_template_mgr_client::TemplateManagerGrpcClient,
    pplx_space_template_mgr_client::ConnectError,
> {
    pplx_space_template_mgr_client::connect().await
}

pub(super) fn parse_sandbox_id_or_exit(raw: &str) -> SandboxId {
    unwrap_or_exit(
        raw.parse::<SandboxId>(),
        "invalid sandbox id returned by server",
    )
}

pub(super) fn sandbox_matches_selector(raw_id: &str, selector: &SandboxSelector) -> bool {
    selector.matches_id(parse_sandbox_id_or_exit(raw_id))
}

pub(super) async fn resolve_sandbox_list_element(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_id: &SandboxSelector,
) -> SandboxListElement {
    let sandboxes = pplx_space_sbx_mgr_client::list_all_sandboxes(client)
        .await
        .unwrap_or_else(|error| exit_error(error));

    let matches: Vec<_> = sandboxes
        .into_iter()
        .filter(|sandbox| sandbox_matches_selector(&sandbox.sandbox_id, sandbox_id))
        .collect();
    let sandbox = match matches.as_slice() {
        [] => {
            exit_error(format!("no sandbox found matching '{sandbox_id}'"));
        }
        [sandbox] => sandbox,
        _ => {
            let labels = matches
                .iter()
                .map(|sandbox| format!("  {}", sandbox_label(&sandbox.sandbox_id)))
                .collect::<Vec<_>>()
                .join("\n");
            exit_error(format!(
                "ambiguous sandbox id '{sandbox_id}', matches:\n{labels}"
            ));
        }
    };

    sandbox.clone()
}

pub(super) async fn resolve_sandbox_state(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_id: &SandboxSelector,
) -> SandboxInfo {
    let sandbox = resolve_sandbox_list_element(client, sandbox_id).await;
    let response = client
        .state(StateRequest {
            sandbox_ids: vec![sandbox.sandbox_id.clone()],
        })
        .await
        .unwrap_or_else(|error| exit_error(error))
        .into_inner();
    response.sandboxes.into_iter().next().unwrap_or_else(|| {
        exit_error(format!(
            "sandbox {} disappeared before state lookup",
            sandbox_label(&sandbox.sandbox_id)
        ));
    })
}

pub(super) async fn resolve_sandbox_template(
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
    sandbox_id: &SandboxSelector,
) -> String {
    let sandbox = resolve_sandbox_list_element(client, sandbox_id).await;
    sandbox.template.clone().unwrap_or_else(|| {
        exit_error(format!(
            "sandbox {} has no template to inherit defaults from",
            sandbox_label(&sandbox.sandbox_id)
        ));
    })
}

pub(super) async fn fetch_template_default_policy(template: &str) -> Option<AutoLifecyclePolicy> {
    let mut client = connect_template_manager().await.unwrap_or_else(|error| {
        exit_error(format!("cannot connect to template-manager: {error}"));
    });
    let response = client
        .list_templates(pplx_space_template_mgr_client::proto::ListTemplatesRequest::default())
        .await
        .unwrap_or_else(|error| exit_error(error))
        .into_inner();
    response
        .templates
        .into_iter()
        .find(|entry| entry.name == template)
        .and_then(|entry| entry.default_auto_lifecycle)
        .map(|policy| {
            AutoLifecyclePolicy::try_from(&policy).unwrap_or_else(|error| {
                exit_error(format!("invalid template default auto lifecycle: {error}"));
            })
        })
}

pub(super) fn format_policy_from_proto(
    policy: &pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy,
) -> String {
    match AutoLifecyclePolicy::try_from(policy) {
        Ok(policy) => format_policy(&policy),
        Err(_) => "<invalid>".to_string(),
    }
}

pub(super) fn format_policy(policy: &AutoLifecyclePolicy) -> String {
    if policy.is_disabled() {
        return "off".to_string();
    }

    let mut parts = Vec::new();
    if !policy.wake.on.is_empty() {
        let wake = policy
            .wake
            .on
            .iter()
            .map(|source| match source {
                AutoLifecycleWakeSource::Access => "access",
                AutoLifecycleWakeSource::Command => "command",
            })
            .collect::<Vec<_>>()
            .join("+");
        parts.push(format!("wake:{wake}"));
    }

    let access_chain = AutoLifecycleStageChain::from_condition_stages(
        policy
            .stages
            .iter()
            .filter(|stage| stage.condition_set == AutoLifecycleConditionSetId::AccessIdle),
    );
    let command_chain = AutoLifecycleStageChain::from_condition_stages(
        policy
            .stages
            .iter()
            .filter(|stage| stage.condition_set == AutoLifecycleConditionSetId::CommandIdle),
    );
    let guest_activity_chain = AutoLifecycleStageChain::from_condition_stages(
        policy
            .stages
            .iter()
            .filter(|stage| stage.condition_set == AutoLifecycleConditionSetId::GuestActivity),
    );
    let guest_activity_extension = policy
        .condition_sets
        .get(&AutoLifecycleConditionSetId::GuestActivity)
        .and_then(|condition| condition.max_guest_idle_extension);

    let non_empty_chains = [&access_chain, &command_chain, &guest_activity_chain]
        .into_iter()
        .filter(|chain| !chain.is_empty())
        .collect::<Vec<_>>();

    if let Some(shared_chain) = non_empty_chains.first()
        && non_empty_chains.iter().all(|chain| *chain == *shared_chain)
    {
        parts.push(format!("idle:{shared_chain}"));
        let mut idle_break_on = Vec::new();
        if !access_chain.is_empty() {
            idle_break_on.push("access");
        } else if !command_chain.is_empty() {
            idle_break_on.push("command");
        }
        if !guest_activity_chain.is_empty() {
            idle_break_on.push("guest-activity");
        }
        if !idle_break_on.is_empty() {
            parts.push(format!("break:{}", idle_break_on.join("+")));
        }
    } else {
        if !access_chain.is_empty() {
            parts.push(format!("access_idle:{access_chain}"));
        }
        if !command_chain.is_empty() {
            parts.push(format!("command_idle:{command_chain}"));
        }
        if !guest_activity_chain.is_empty() {
            parts.push(format!("guest_activity:{guest_activity_chain}"));
        }
    }

    if let Some(max_guest_idle_extension) = guest_activity_extension {
        parts.push(format!("guest-activity-max:{max_guest_idle_extension}"));
    }

    if parts.is_empty() {
        "off".to_string()
    } else {
        parts.join(" | ")
    }
}

pub(super) fn format_duration(value: u64) -> String {
    if value > 0 && value.is_multiple_of(86_400) {
        format!("{}d", value / 86_400)
    } else if value > 0 && value.is_multiple_of(3_600) {
        format!("{}h", value / 3_600)
    } else if value > 0 && value.is_multiple_of(60) {
        format!("{}m", value / 60)
    } else {
        format!("{value}s")
    }
}

pub(super) fn format_relative_timestamp(timestamp: &Timestamp) -> String {
    let seconds = timestamp.seconds;
    if seconds <= 0 {
        return "-".to_string();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let delta = seconds - now;
    if delta <= 0 {
        if delta == 0 {
            "now".to_string()
        } else {
            format!("{} ago", format_duration(delta.unsigned_abs()))
        }
    } else {
        let delta = u64::try_from(delta).expect("positive duration delta fits in u64");
        format!("in {}", format_duration(delta))
    }
}

pub(super) fn format_bytes(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1} GB", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1} MB", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1} KB", n as f64 / 1_000.0)
    } else {
        format!("{n} B")
    }
}

/// Read a PID from a symlink like space-spaced-host-svc.pid -> /proc/12345
pub(super) fn read_pid_symlink(path: &str) -> Option<u32> {
    std::fs::read_link(path).ok().and_then(|target| {
        target
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|s| s.parse::<u32>().ok())
    })
}

pub(super) struct ProcInfo {
    pub(super) state: char,
    pub(super) cpu_percent: f64,
    pub(super) rss_bytes: u64,
    pub(super) threads: u32,
}

pub(super) fn read_proc_info(pid: u32) -> ProcInfo {
    let default = ProcInfo {
        state: '?',
        cpu_percent: 0.0,
        rss_bytes: 0,
        threads: 0,
    };

    // Read /proc/{pid}/stat for state, utime, stime, num_threads
    let stat_path = format!("/proc/{pid}/stat");
    let Ok(stat) = std::fs::read_to_string(&stat_path) else {
        return default;
    };

    // Format: pid (comm) state ... — find closing ')' to skip comm which may contain spaces
    let Some(close_paren) = stat.rfind(')') else {
        return default;
    };
    let Some(stat_fields) = stat.get(close_paren + 2..) else {
        return default;
    };
    let fields: Vec<&str> = stat_fields.split_whitespace().collect();
    // fields[0] = state, fields[11] = utime, fields[12] = stime, fields[17] = num_threads,
    // fields[19] = process start time.
    if fields.len() < 20 {
        return default;
    }

    let state = fields
        .first()
        .and_then(|value| value.chars().next())
        .unwrap_or('?');
    let utime: u64 = fields
        .get(11)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let stime: u64 = fields
        .get(12)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let num_threads: u32 = fields
        .get(17)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);

    // RSS from /proc/{pid}/status (VmRSS in kB)
    let status_path = format!("/proc/{pid}/status");
    let rss_bytes = std::fs::read_to_string(&status_path)
        .ok()
        .and_then(|s| {
            s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| {
                l.split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse::<u64>().ok())
            })
        })
        .map_or(0, |kb| kb * 1024);

    // CPU% — approximate from /proc/{pid}/stat utime+stime vs /proc/uptime
    let cpu_percent = std::fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| {
            s.split_whitespace()
                .next()
                .and_then(|v| v.parse::<f64>().ok())
        })
        .map_or(0.0, |uptime_secs| {
            let ticks_per_sec = 100.0; // sysconf(_SC_CLK_TCK), almost always 100 on Linux
            let total_cpu_secs = (utime + stime) as f64 / ticks_per_sec;
            // Process start time is fields[19] in ticks since boot
            let start_time: u64 = fields
                .get(19)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let process_uptime = uptime_secs - (start_time as f64 / ticks_per_sec);
            if process_uptime > 0.0 {
                (total_cpu_secs / process_uptime) * 100.0
            } else {
                0.0
            }
        });

    ProcInfo {
        state,
        cpu_percent,
        rss_bytes,
        threads: num_threads,
    }
}

/// Parse a compact timestamp like "20260331T140000Z" into a human-readable string.
pub(super) fn format_snapshot_ts(ts: &str) -> String {
    // Try parsing as compact ISO: "20260331T140000Z" -> "2026-03-31T14:00:00Z"
    if ts.len() >= 16 {
        let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
            ts.get(0..4),
            ts.get(4..6),
            ts.get(6..8),
            ts.get(9..11),
            ts.get(11..13),
            ts.get(13..15),
        ) else {
            return ts.to_string();
        };
        let iso = format!("{year}-{month}-{day}T{hour}:{minute}:{second}Z");
        if let Ok(zdt) = iso.parse::<jiff::Timestamp>() {
            let now = jiff::Timestamp::now();
            if let Ok(span) = now.since(zdt) {
                let secs = span.get_seconds();
                let age = if secs < 60 {
                    format!("{secs}s ago")
                } else if secs < 3600 {
                    format!("{}m ago", secs / 60)
                } else if secs < 86400 {
                    format!("{}h ago", secs / 3600)
                } else {
                    format!("{}d ago", secs / 86400)
                };
                // e.g. "2026-03-31 14:00:00 (5m ago)"
                return format!("{year}-{month}-{day} {hour}:{minute}:{second} ({age})");
            }
        }
    }
    ts.to_string()
}

pub(super) fn format_age(created_at: &str) -> String {
    let Ok(ts) = created_at.parse::<u64>() else {
        return "-".into();
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must not be before UNIX_EPOCH")
        .as_secs();
    if ts > now {
        return "-".into();
    }
    let secs = now - ts;
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

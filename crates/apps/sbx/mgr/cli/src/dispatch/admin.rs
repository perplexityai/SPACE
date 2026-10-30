use super::*;

pub(super) async fn handle(
    command: Commands,
    client: &mut pplx_space_sbx_mgr_client::SandboxManagerGrpcClient,
) {
    match command {
        Commands::Templates => {
            let template_defaults: HashMap<String, AutoLifecyclePolicy> =
                match connect_template_manager().await {
                    Ok(mut template_client) => template_client
                        .list_templates(
                            pplx_space_template_mgr_client::proto::ListTemplatesRequest::default(),
                        )
                        .await
                        .ok()
                        .map(|response| {
                            response
                                .into_inner()
                                .templates
                                .into_iter()
                                .filter_map(|template| {
                                    template.default_auto_lifecycle.map(|policy| {
                                        (
                                            template.name,
                                            AutoLifecyclePolicy::try_from(&policy).unwrap_or_else(
                                                |_| AutoLifecyclePolicy::disabled(),
                                            ),
                                        )
                                    })
                                })
                                .collect()
                        })
                        .unwrap_or_default(),
                    Err(_) => HashMap::new(),
                };

            let resp = client
                .list_templates(ListTemplatesRequest::default())
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();

            if resp.templates.is_empty() {
                println!("No templates found.");
                return;
            }

            let mut table = Table::new();
            table.load_style(UTF8_FULL_CONDENSED);
            table.set_header(vec![
                "ID",
                "DESCRIPTION",
                "BASE",
                "BUILT AT",
                "SIZE",
                "SHA256 (1MB)",
                "AUTO DEFAULT",
            ]);

            for t in &resp.templates {
                let size = if t.size_bytes > 1_000_000_000 {
                    format!("{:.1} GB", t.size_bytes as f64 / 1_000_000_000.0)
                } else {
                    format!("{:.0} MB", t.size_bytes as f64 / 1_000_000.0)
                };
                let hash_prefix = t.sha256.chars().take(12).collect::<String>();
                let hash = if t.sha256.chars().count() > 12 {
                    format!("{hash_prefix}...")
                } else {
                    t.sha256.clone()
                };
                table.add_row(vec![
                    t.id.clone(),
                    t.description.clone(),
                    if t.base.is_empty() {
                        "-".into()
                    } else {
                        t.base.clone()
                    },
                    t.built_at.clone(),
                    size,
                    hash,
                    template_defaults
                        .get(&t.id)
                        .map_or_else(|| "-".into(), format_policy),
                ]);
            }

            println!("{table}");
        }

        Commands::TemplateSetAutoLifecycle {
            template,
            auto_lifecycle,
        } => {
            let Some(policy) = auto_lifecycle.to_policy().unwrap_or_else(|error| {
                exit_error(format!("invalid auto lifecycle policy: {error}"));
            }) else {
                exit_error("provide --auto-lifecycle-off or at least one lifecycle flag");
            };

            let mut template_client = connect_template_manager().await.unwrap_or_else(|error| {
                exit_error(format!("cannot connect to template-manager: {error}"));
            });
            template_client
                .update_template_settings(
                    pplx_space_template_mgr_client::proto::UpdateTemplateSettingsRequest {
                        template: template.clone(),
                        default_auto_lifecycle: Some(
                            pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy::from(&policy),
                        ),
                        network_policy: None,
                        network_policy_generation: 0,
                        network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
                        auto_lifecycle_authority: PolicyAuthority::Local.as_proto_i32(),
                    },
                )
                .await
                .unwrap_or_else(|error| exit_error(error));
            println!(
                "Updated template {template}: default_auto_lifecycle={}",
                format_policy(&policy)
            );
        }

        Commands::NetworkPolicy {
            sandbox_id,
            set,
            clear,
        } => {
            if set.is_some() || clear {
                warn_if_inbound_reconciliation_active(FACET_NETWORK_POLICY).await;
            }
            let short_id = sandbox_label(sandbox_id.as_str());
            let current_generation = client
                .get_network_policy(GetNetworkPolicyRequest {
                    sandbox_ids: vec![sandbox_id.to_string()],
                })
                .await
                .map_or(0, |response| {
                    response
                        .into_inner()
                        .policies
                        .into_iter()
                        .next()
                        .map_or(0, |policy| policy.effective_generation)
                });
            let next_generation = current_generation.saturating_add(1).max(1);
            let response = if clear {
                client
                    .set_network_policy(SetNetworkPolicyRequest {
                        sandbox_id: sandbox_id.to_string(),
                        policy: None,
                        inherit_template: true,
                        policy_generation: next_generation,
                        authority: PolicyAuthority::Local.as_proto_i32(),
                        sync_to_gateway: false,
                    })
                    .await
                    .unwrap_or_else(|error| exit_error(error))
                    .into_inner()
            } else if let Some(mode) = set {
                client
                    .set_network_policy(SetNetworkPolicyRequest {
                        sandbox_id: sandbox_id.to_string(),
                        policy: Some(mode.to_proto()),
                        inherit_template: false,
                        policy_generation: next_generation,
                        authority: PolicyAuthority::Local.as_proto_i32(),
                        sync_to_gateway: false,
                    })
                    .await
                    .unwrap_or_else(|error| exit_error(error))
                    .into_inner()
            } else {
                let response = client
                    .get_network_policy(GetNetworkPolicyRequest {
                        sandbox_ids: vec![sandbox_id.to_string()],
                    })
                    .await
                    .unwrap_or_else(|error| exit_error(error))
                    .into_inner();
                let policy = response.policies.into_iter().next().unwrap_or_else(|| {
                    exit_error("sbx-manager returned no network policy");
                });
                pplx_space_sbx_mgr_client::proto::SetNetworkPolicyResponse {
                    effective_policy: policy.effective_policy,
                    inherited: policy.inherited,
                    source_template: policy.source_template,
                    effective_generation: policy.effective_generation,
                    applied_generation: policy.applied_generation,
                    applied_source: policy.applied_source,
                    apply_decision:
                        pplx_space_net_common::proto::NetworkPolicyApplyDecision::Unspecified as i32,
                    authority: policy.authority,
                    ..Default::default()
                }
            };

            println!("sandbox: {short_id}");
            let source = if response.inherited {
                if response.source_template.is_empty() {
                    "template".to_string()
                } else {
                    format!("template({})", response.source_template)
                }
            } else {
                "own".to_string()
            };
            if let Some(policy) = response.effective_policy.as_ref() {
                let mode = network_policy_mode_from_proto(policy)
                    .unwrap_or_else(|error| exit_error(error));
                print_network_policy(mode, Some(&source));
            } else {
                exit_error("sbx-manager returned no network policy");
            }
        }

        Commands::NetworkState { sandbox_id } => {
            let sandbox = resolve_sandbox_list_element(client, &sandbox_id).await;
            let short_id = sandbox_label(&sandbox.sandbox_id);
            let mut gateway = pplx_space_net_gateway_client::connect(&sandbox.sandbox_id)
                .await
                .unwrap_or_else(|error| {
                    exit_error(format!(
                        "cannot connect to net-gateway for {short_id}: {error}"
                    ));
                });
            let response = gateway
                .get_network_state(pplx_space_net_gateway_client::proto::GetNetworkStateRequest {})
                .await
                .unwrap_or_else(|error| exit_error(error))
                .into_inner();
            println!("sandbox: {short_id}");
            let state =
                pplx_space_net_gateway_client::proto::NetworkState::try_from(response.state)
                    .unwrap_or(pplx_space_net_gateway_client::proto::NetworkState::Unspecified);
            println!("state: {state}");
            let interception_readiness =
                pplx_space_net_gateway_client::proto::InterceptionReadiness::try_from(
                    response.interception_readiness,
                )
                .unwrap_or(
                    pplx_space_net_gateway_client::proto::InterceptionReadiness::Unspecified,
                );
            println!("interception_readiness: {interception_readiness}");
            println!("installed_policy_ref: {}", response.installed_policy_ref);
            let last_error_kind =
                pplx_space_net_gateway_client::proto::NetworkPolicyErrorKind::try_from(
                    response.last_error_kind,
                )
                .unwrap_or(pplx_space_net_gateway_client::proto::NetworkPolicyErrorKind::None);
            if last_error_kind != pplx_space_net_gateway_client::proto::NetworkPolicyErrorKind::None
                || !response.last_error.is_empty()
            {
                println!("last_error_kind: {last_error_kind}");
                println!("last_error: {}", response.last_error);
            }
            println!(
                "last_hydration_latency_ms: {}",
                response.last_hydration_latency_ms
            );
            println!(
                "last_transition_unix_ms: {}",
                response.last_transition_unix_ms
            );
            println!(
                "pending_tcp_connections: {}",
                response.pending_tcp_connections
            );
            println!("pending_udp_datagrams: {}", response.pending_udp_datagrams);
            println!("pending_udp_bytes: {}", response.pending_udp_bytes);
            println!(
                "current_retry_backoff_ms: {}",
                response.current_retry_backoff_ms
            );
            println!(
                "consecutive_hydration_failures: {}",
                response.consecutive_hydration_failures
            );
            println!("debug_enabled: {}", response.debug_enabled);
            println!("build_git_sha: {}", response.build_git_sha);
        }

        Commands::NetworkDebug {
            sandbox_id,
            duration,
            disable,
        } => {
            let sandbox = resolve_sandbox_list_element(client, &sandbox_id).await;
            let short_id = sandbox_label(&sandbox.sandbox_id);
            let mut gateway = pplx_space_net_gateway_client::connect(&sandbox.sandbox_id)
                .await
                .unwrap_or_else(|error| {
                    exit_error(format!(
                        "cannot connect to net-gateway for {short_id}: {error}"
                    ));
                });
            let debug_until_unix_ms = if disable {
                0
            } else if let Some(window) = duration {
                debug_deadline_unix_ms(window.as_duration())
            } else {
                // clap enforces `--duration` unless `--disable` is present.
                unreachable!("--duration is required unless --disable")
            };
            let response = gateway
                .enable_debug(pplx_space_net_gateway_client::proto::EnableDebugRequest {
                    debug_until_unix_ms,
                })
                .await
                .unwrap_or_else(|error| exit_error(error))
                .into_inner();

            println!("sandbox: {short_id}");
            println!("debug_enabled: {}", response.enabled);
            if response.enabled {
                println!("debug_until_unix_ms: {}", response.debug_until_unix_ms);
            }
        }

        Commands::CycleNetGateway {
            sandbox_ids,
            stale,
            refresh_config,
            ids_only,
            execute,
            max,
            sleep,
            min_age,
        } => {
            // State requires explicit ids (empty is rejected server-side), so
            // enumerate the node's sandboxes via List first.
            let all_ids: Vec<String> = pplx_space_sbx_mgr_client::list_all_sandboxes(client)
                .await
                .unwrap_or_else(|error| exit_error(error))
                .into_iter()
                .map(|sandbox| sandbox.sandbox_id)
                .collect();
            // Complete the read before cycling so a late oversized state
            // reply cannot leave a partially cycled selection.
            let sandboxes = collect_cycle_state(client, &all_ids)
                .await
                .unwrap_or_else(|error| exit_error(error));

            // Fail loud on a broken clock: a silent 0 here would make every
            // sandbox read as "too young" and --min-age would skip them all.
            let now_epoch_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is before UNIX_EPOCH")
                .as_secs();
            let too_young = |sandbox: &SandboxInfo| -> bool {
                let Some(min_age) = min_age else {
                    return false;
                };
                // SandboxInfo.created_at carries the sandboxes-row value,
                // which sandbox-manager's db::now() writes as epoch seconds
                // (not RFC 3339 — that's SnapshotMeta.created_at, a different
                // struct). Unparseable values never count as "too young" so a
                // formatting surprise can't hide sandboxes.
                sandbox
                    .created_at
                    .as_deref()
                    .and_then(|created| created.parse::<u64>().ok())
                    .is_some_and(|created| {
                        now_epoch_secs.saturating_sub(created) < min_age.as_duration().as_secs()
                    })
            };

            // Explicit selectors resolving to sandboxes without a live
            // gateway (stopped/suspended, or gateway dead — the watchdog owns
            // that restart) are reported and skipped instead of being sent to
            // the server just to bounce off its failed_precondition check.
            let selection = if stale || refresh_config {
                CycleSelection {
                    selected: bulk_cycle_candidates(sandboxes, refresh_config),
                    ..Default::default()
                }
            } else {
                explicit_cycle_candidates(&sandboxes, &sandbox_ids)
            };
            for selector in &selection.missing {
                eprintln!("ERROR: no sandbox found matching '{selector}'");
            }
            let mut selected = selection.selected;
            let skipped_no_gateway = selection.no_gateway;
            let selection_failures = selection.missing.len() + skipped_no_gateway.len();
            let skipped_young = {
                let before = selected.len();
                selected.retain(|sandbox| !too_young(sandbox));
                before - selected.len()
            };
            // Oldest first: the longest-lived gateways carry the most skew.
            selected.sort_by(|a, b| a.created_at.cmp(&b.created_at));
            let total_selected = selected.len();
            selected.truncate(max);

            if !skipped_no_gateway.is_empty() {
                eprintln!(
                    "({} skipped, no live net-gateway: {})",
                    skipped_no_gateway.len(),
                    skipped_no_gateway.join(", ")
                );
            }
            if ids_only {
                for sandbox in &selected {
                    println!("{}", sandbox.sandbox_id);
                }
                if selection_failures > 0 {
                    std::process::exit(1);
                }
                return;
            }

            if selected.is_empty() {
                println!("No net-gateways selected.");
                if skipped_young > 0 {
                    println!("({skipped_young} skipped by --min-age)");
                }
                if selection_failures > 0 {
                    std::process::exit(1);
                }
                return;
            }

            println!(
                "{} net-gateway(s) selected{}:",
                selected.len(),
                if execute { "" } else { " (dry-run)" }
            );
            if refresh_config {
                println!(
                    "Configuration refresh: current and unknown SHAs are included; repeating this command selects them again."
                );
            }
            for sandbox in &selected {
                // Stale with no sha means the gateway answered a probe but its
                // binary predates sha reporting; absent sha without a stale
                // verdict means it has not been probed yet.
                let gateway_sha = sandbox.netgtw_git_sha.as_deref().unwrap_or(
                    if sandbox.netgtw_stale == Some(true) {
                        "pre-sha-reporting"
                    } else {
                        "unknown"
                    },
                );
                println!(
                    "  {}  template={}  status={}  gateway_sha={gateway_sha}",
                    sandbox.sandbox_id,
                    sandbox.template.as_deref().unwrap_or("-"),
                    sandbox
                        .observed_lifecycle()
                        .map(|state| state.label())
                        .unwrap_or("unknown"),
                );
            }
            if total_selected > selected.len() {
                let follow_up = if refresh_config {
                    "use explicit remaining sandbox IDs; rerunning selects the same batch"
                } else {
                    "rerun to continue"
                };
                println!(
                    "({} more matched beyond --max {max}; {follow_up})",
                    total_selected - selected.len()
                );
            }
            if skipped_young > 0 {
                println!("({skipped_young} skipped by --min-age)");
            }
            if !execute {
                println!("Dry-run: pass --execute to cycle these gateways.");
                if selection_failures > 0 {
                    std::process::exit(1);
                }
                return;
            }

            let mut failures = selection_failures;
            for (index, sandbox) in selected.iter().enumerate() {
                if index > 0 {
                    tokio::time::sleep(sleep.as_duration()).await;
                }
                let sandbox_id = &sandbox.sandbox_id;
                match client
                    .cycle_net_gateway(CycleNetGatewayRequest {
                        sandbox_id: sandbox.sandbox_id.clone(),
                    })
                    .await
                {
                    Ok(response) => {
                        let response = response.into_inner();
                        println!(
                            "cycled {sandbox_id}: pid {} -> {}",
                            response.old_pid, response.new_pid
                        );
                    }
                    Err(error) => {
                        failures += 1;
                        eprintln!("ERROR: cycle {sandbox_id} failed: {error}");
                    }
                }
            }
            if failures > 0 {
                exit_message(format!("{failures} gateway(s) failed or unavailable"));
            }
        }

        Commands::Cleanup => {
            let resp = client
                .cleanup(CleanupRequest {})
                .await
                .unwrap_or_else(|e| exit_error(e))
                .into_inner();

            if resp.cleaned_ids.is_empty() {
                println!("Nothing to clean up.");
            } else {
                for id in &resp.cleaned_ids {
                    println!("Cleaned up: {}", sandbox_label(id));
                }
                println!("Released {} entries.", resp.cleaned_ids.len());
            }
        }
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}

trait CycleStateReader {
    async fn read_cycle_state(
        &mut self,
        sandbox_ids: &[String],
    ) -> Result<Vec<SandboxInfo>, tonic::Status>;
}

impl CycleStateReader for pplx_space_sbx_mgr_client::SandboxManagerGrpcClient {
    async fn read_cycle_state(
        &mut self,
        sandbox_ids: &[String],
    ) -> Result<Vec<SandboxInfo>, tonic::Status> {
        Ok(self
            .state(StateRequest {
                sandbox_ids: sandbox_ids.to_vec(),
            })
            .await?
            .into_inner()
            .sandboxes)
    }
}

async fn collect_cycle_state<R: CycleStateReader>(
    reader: &mut R,
    all_ids: &[String],
) -> Result<Vec<SandboxInfo>, tonic::Status> {
    let mut pending = all_ids
        .chunks(pplx_space_common_core::inventory::READ_BATCH_ITEMS)
        .map(|batch| batch.to_vec())
        .collect::<std::collections::VecDeque<_>>();
    let mut sandboxes = Vec::with_capacity(all_ids.len());
    while let Some(mut batch) = pending.pop_front() {
        match reader.read_cycle_state(&batch).await {
            Ok(states) => sandboxes.extend(states),
            // Tonic reports its receive-size limit as OutOfRange. State does
            // not use that code for application errors, and these reads are
            // safe to retry with smaller requests.
            Err(error) if error.code() == tonic::Code::OutOfRange && batch.len() > 1 => {
                let right = batch.split_off(batch.len() / 2);
                pending.push_front(right);
                pending.push_front(batch);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(sandboxes)
}

fn bulk_cycle_candidates(sandboxes: Vec<SandboxInfo>, refresh_config: bool) -> Vec<SandboxInfo> {
    sandboxes
        .into_iter()
        .filter(|sandbox| {
            sandbox.netgtw_alive && (refresh_config || sandbox.netgtw_stale == Some(true))
        })
        .collect()
}

#[derive(Default)]
struct CycleSelection {
    selected: Vec<SandboxInfo>,
    missing: Vec<String>,
    no_gateway: Vec<String>,
}

// Saved cohorts can outlive their sandboxes. Keep unavailable members separate
// so execution can finish the live members before reporting an incomplete run.
fn explicit_cycle_candidates(
    sandboxes: &[SandboxInfo],
    selectors: &[SandboxSelector],
) -> CycleSelection {
    let mut selection = CycleSelection::default();
    selection.selected = selectors
        .iter()
        .filter_map(|selector| {
            let matches: Vec<&SandboxInfo> = sandboxes
                .iter()
                .filter(|sandbox| sandbox_matches_selector(&sandbox.sandbox_id, selector))
                .collect();
            match matches.as_slice() {
                [] => {
                    // Full UUID selectors are normalized without hyphens.
                    // Restore the file format so missing IDs can be copied
                    // from errors into an --ids-file retry cohort.
                    let id = selector
                        .as_str()
                        .parse::<SandboxId>()
                        .map_or_else(|_| selector.to_string(), |id| id.to_string());
                    selection.missing.push(id);
                    None
                }
                [sandbox] if !sandbox.netgtw_alive => {
                    selection.no_gateway.push(sandbox.sandbox_id.clone());
                    None
                }
                [sandbox] => Some((*sandbox).clone()),
                _ => {
                    let labels = matches
                        .iter()
                        .map(|sandbox| format!("  {}", sandbox.sandbox_id))
                        .collect::<Vec<_>>()
                        .join("\n");
                    exit_error(format!(
                        "ambiguous sandbox id '{selector}', matches:\n{labels}"
                    ));
                }
            }
        })
        .collect();
    selection
}

#[cfg(test)]
mod tests;

/// Absolute wall-clock deadline (`now + window`) in unix milliseconds, matching
/// the gateway's `debug_until_unix_ms` contract. The CLI and the per-sandbox
/// gateway share the node clock, so the absolute instant is unambiguous.
fn debug_deadline_unix_ms(window: std::time::Duration) -> u64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
    now_ms.saturating_add(window_ms.max(1))
}

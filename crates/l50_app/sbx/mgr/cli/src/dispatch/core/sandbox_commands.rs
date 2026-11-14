use super::*;

pub(super) async fn handle(command: Commands, client: &mut SandboxClient) {
    match command {
        Commands::Create {
            content_hash,
            environment,
            runtime,
            template,
            user,
            snapshots,
            autostart,
            boot_mode,
            blocking,
            env,
            rootfs_size_mb,
            auto_lifecycle,
        } => {
            handle_create(
                client,
                CreateCommandInput {
                    content_hash,
                    environment,
                    runtime,
                    template,
                    user,
                    snapshots,
                    autostart,
                    boot_mode,
                    blocking,
                    env,
                    rootfs_size_mb,
                    auto_lifecycle,
                },
            )
            .await
        }
        Commands::Start {
            sandbox_id,
            checkpoint,
            memory,
            from_warm,
            blocking,
            ready_timeout_secs,
        } => {
            handle_start(
                client,
                sandbox_id,
                checkpoint,
                memory,
                from_warm,
                blocking,
                ready_timeout_secs,
            )
            .await
        }
        Commands::Stop {
            id,
            all,
            template,
            blocking,
            immediate_kill,
        } => handle_stop(client, id, all, template, blocking, immediate_kill).await,
        Commands::Delete {
            id,
            all,
            template,
            blocking,
            discard_retained_filesystem,
            reason,
        } => match reason.filter(|_| discard_retained_filesystem) {
            Some(reason) => handle_discard_delete(client, id, blocking, reason).await,
            None => handle_delete(client, id, all, template, blocking).await,
        },
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}

struct CreateCommandInput {
    content_hash: Option<pplx_space_content_digest::ContentHash>,
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
    runtime: Option<String>,
    template: String,
    user: Option<String>,
    snapshots: bool,
    autostart: bool,
    boot_mode: String,
    blocking: bool,
    env: Vec<String>,
    rootfs_size_mb: Option<u32>,
    auto_lifecycle: AutoLifecycleArgs,
}

async fn handle_create(client: &mut SandboxClient, mut input: CreateCommandInput) {
    let boot_mode = parse_boot_mode(&input.boot_mode).unwrap_or_else(|error| {
        exit_error(format!("{error}\nValid values: cold, base-snapshot, pool"));
    });
    let owner = resolve_create_owner(input.user.take()).unwrap_or_else(|error| exit_error(error));
    let env = parse_env_vars(std::mem::take(&mut input.env));
    let auto_lifecycle = input.auto_lifecycle.to_policy().unwrap_or_else(|error| {
        exit_error(format!("invalid auto lifecycle policy: {error}"));
    });
    if input.snapshots {
        warn_if_inbound_reconciliation_active(FACET_SANDBOX_DESIRED_STATE).await;
    }
    let request = local_create_request(input, boot_mode, owner, env, auto_lifecycle);
    let resp = pplx_space_sbx_mgr_client::environment::create(client, request)
        .await
        .unwrap_or_else(|error| exit_error(error));

    for line in create_response_lines(&resp) {
        println!("{line}");
    }
}

fn local_create_request(
    input: CreateCommandInput,
    boot_mode: BootMode,
    owner: String,
    env: std::collections::BTreeMap<String, String>,
    auto_lifecycle: Option<AutoLifecyclePolicy>,
) -> CreateRequest {
    let mut request = CreateRequest {
        environment: None,
        template: input.template,
        owner,
        runtime: input.runtime.unwrap_or_default(),
        snapshots_enabled: input.snapshots,
        autostart: input.autostart,
        pool_template: None,
        volume_encryption: false,
        boot_mode: boot_mode.into(),
        blocking: input.blocking,
        sandbox_id: None,
        content_hash: input.content_hash.map(|value| value.to_string()),
        env: env.into_iter().collect(),
        auto_lifecycle: auto_lifecycle
            .as_ref()
            .map(pplx_space_sbx_mgr_client::proto::AutoLifecyclePolicy::from),
        rootfs_size_mb: input.rootfs_size_mb,
        network_policy: None,
        network_policy_generation: 0,
        network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_authority: if auto_lifecycle.is_some() {
            PolicyAuthority::Local.as_proto_i32()
        } else {
            PolicyAuthority::Cluster.as_proto_i32()
        },
        template_network_policy: None,
        template_network_policy_generation: 0,
        template_network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
        route_identity: None,
        disk_io_max_operations_per_sec: None,
        disk_io_max_bandwidth_mib_per_sec: None,
    };
    if let Some(reference) = input.environment {
        pplx_space_sbx_mgr_client::environment::bind(&mut request, reference);
    }
    request
}

fn parse_boot_mode(value: &str) -> Result<BootMode, String> {
    match value {
        "cold" => Ok(BootMode::Cold),
        "base-snapshot" => Ok(BootMode::BaseSnapshot),
        "pool" => Ok(BootMode::Pool),
        other => Err(format!("unknown boot mode: {other}")),
    }
}

fn resolve_create_owner(user: Option<String>) -> Result<String, String> {
    let owner = match user {
        Some(user) => parse_cli_local_sandbox_owner(&user),
        None => default_local_sandbox_owner(),
    }?;
    Ok(owner.to_string())
}

fn create_response_lines(resp: &pplx_space_sbx_mgr_client::proto::CreateResponse) -> Vec<String> {
    let mode_str = create_response_boot_mode_label(resp.boot_mode_used);
    let mut lines = vec![format!("Sandbox:   {}", sandbox_label(&resp.sandbox_id))];
    if let Some(pid) = resp.pid {
        lines.extend([
            format!("PID:       {pid}"),
            format!(
                "Guest IP:  {}",
                resp.guest_ip.as_deref().unwrap_or_default()
            ),
            format!(
                "Namespace: {}",
                resp.namespace.as_deref().unwrap_or_default()
            ),
            format!("Boot mode: {mode_str}"),
            format!(
                "Log:       tail -f {}",
                resp.log_path.as_deref().unwrap_or_default()
            ),
        ]);
    } else {
        lines.push(format!("Boot mode: {mode_str}"));
    }
    lines
}

fn create_response_boot_mode_label(mode: i32) -> &'static str {
    match BootMode::try_from(mode).unwrap_or(BootMode::Unspecified) {
        BootMode::Cold => "cold",
        BootMode::BaseSnapshot => "base-snapshot",
        BootMode::Pool => "pool",
        BootMode::Unspecified => "unknown",
    }
}

async fn handle_start(
    client: &mut SandboxClient,
    sandbox_id: SandboxSelector,
    checkpoint: Option<String>,
    memory: Option<u32>,
    from_warm: bool,
    blocking: bool,
    ready_timeout_secs: Option<u32>,
) {
    let resp = client
        .start(local_start_request(
            sandbox_id.to_string(),
            checkpoint,
            memory,
            from_warm,
            blocking,
            ready_timeout_secs,
        ))
        .await
        .unwrap_or_else(|error| exit_error(error))
        .into_inner();

    println!("Sandbox:   {}", sandbox_label(&resp.sandbox_id));
    println!("PID:       {}", resp.pid);
    println!("Guest IP:  {}", resp.guest_ip);
    println!("Namespace: {}", resp.namespace);
    println!("Log:       tail -f {}", resp.log_path);
}

fn local_start_request(
    sandbox_id: String,
    checkpoint: Option<String>,
    memory: Option<u32>,
    from_warm: bool,
    blocking: bool,
    ready_timeout_secs: Option<u32>,
) -> StartRequest {
    StartRequest {
        activation_auto_lifecycle: None,
        operation_activation_attempt_id: None,
        restore_artifact_as_new: None,
        expected_local_artifact_id: None,
        sandbox_id,
        vcpu: None,
        memory_mib: memory,
        checkpoint,
        from_warm,
        blocking,
        netns_path: None,
        ready_timeout_secs,
        target_state: 0,
        suspend_restore_storage_uri: None,
        suspend_restore_source_node: None,
        route_identity: None,
    }
}

async fn handle_stop(
    client: &mut SandboxClient,
    id: Option<SandboxSelector>,
    all: bool,
    template: Option<String>,
    blocking: bool,
    immediate_kill: bool,
) {
    let id_prefix = id_prefix_for_selected_scope(id, all).unwrap_or_else(|error| exit_error(error));
    let stopped = super::lifecycle_selection::run(
        client,
        id_prefix,
        template,
        blocking,
        super::lifecycle_selection::Mutation::Stop { immediate_kill },
    )
    .await
    .unwrap_or_else(|error| exit_error(error));
    if stopped == 0 {
        println!("No sandboxes stopped.");
    }
}

async fn handle_delete(
    client: &mut SandboxClient,
    id: Option<SandboxSelector>,
    all: bool,
    template: Option<String>,
    blocking: bool,
) {
    let id_prefix = id_prefix_for_selected_scope(id, all).unwrap_or_else(|error| exit_error(error));
    let deleted = super::lifecycle_selection::run(
        client,
        id_prefix,
        template,
        blocking,
        super::lifecycle_selection::Mutation::Delete,
    )
    .await
    .unwrap_or_else(|error| exit_error(error));
    if deleted == 0 {
        println!("No sandboxes deleted.");
    }
}

/// An operator's discard delete of one sandbox, by its full UUID: a single
/// `Delete` carrying the reason, with no selection scan, so the request names
/// exactly the sandbox the operator inspected.
async fn handle_discard_delete(
    client: &mut SandboxClient,
    id: Option<SandboxSelector>,
    blocking: bool,
    reason: String,
) {
    let sandbox_id = discard_target(id).unwrap_or_else(|error| exit_error(error));
    // Checked here as well as by sandbox-manager: one that predates the
    // discard ignores the field and would run an ordinary delete.
    let sandbox = client
        .state(StateRequest {
            sandbox_ids: vec![sandbox_id.to_string()],
        })
        .await
        .unwrap_or_else(|status| {
            exit_error(format!("read sandbox {sandbox_id}: {}", status.message()))
        })
        .into_inner()
        .sandboxes
        .into_iter()
        .next()
        .unwrap_or_else(|| exit_error(format!("no sandbox {sandbox_id} on this node")));
    discard_precheck(&sandbox).unwrap_or_else(|error| exit_error(error));
    let deleted = client
        .delete(DeleteRequest {
            sandbox_id: sandbox_id.to_string(),
            template_filter: None,
            blocking,
            discard_retained_filesystem_reason: Some(reason),
        })
        .await
        .unwrap_or_else(|status| {
            exit_error(format!("discard delete refused: {}", status.message()))
        })
        .into_inner()
        .deleted_ids;
    // Without `--blocking`, sandbox-manager answers once the discard is
    // admitted; its cleanup erases the filesystem afterwards.
    for id in &deleted {
        if blocking {
            println!("Deleted: {id} (retained filesystem discarded)");
        } else {
            println!(
                "Discard requested: {id} (its retained filesystem is erased in the background)"
            );
        }
    }
}

/// A discard applies only to a sandbox quarantined, or failed, with the
/// failure reason that marks its retained filesystem as possibly its only
/// copy.
fn discard_precheck(sandbox: &SandboxInfo) -> Result<(), String> {
    use pplx_space_sbx_mgr_client::proto::SandboxLifecycleState;
    let lifecycle = sandbox
        .lifecycle
        .and_then(|value| SandboxLifecycleState::try_from(value).ok());
    let held = matches!(
        lifecycle,
        Some(SandboxLifecycleState::Quarantined | SandboxLifecycleState::Failed)
    ) && pplx_space_sbx_mgr_client::suspend_evidence_requires_retention(
        sandbox.failure_reason.as_deref(),
    );
    if held {
        return Ok(());
    }
    Err(format!(
        "sandbox {} is {} with failure reason {}; --discard-retained-filesystem applies only to a \
         sandbox quarantined or failed with {} or {}",
        sandbox.sandbox_id,
        lifecycle.map_or("in an unknown lifecycle", |state| state.as_str_name()),
        sandbox.failure_reason.as_deref().unwrap_or("none"),
        pplx_space_sbx_mgr_client::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE,
        pplx_space_sbx_mgr_client::SUSPEND_CAPTURE_UNRESOLVED
    ))
}

fn discard_target(
    id: Option<SandboxSelector>,
) -> Result<pplx_space_common_core::types::SandboxId, &'static str> {
    id.and_then(|id| id.to_string().parse().ok())
        .ok_or("--discard-retained-filesystem takes the sandbox's full UUID")
}

fn id_prefix_for_selected_scope(
    id: Option<SandboxSelector>,
    all: bool,
) -> Result<String, &'static str> {
    if all {
        Ok("all".to_string())
    } else if let Some(id) = id {
        Ok(id.to_string())
    } else {
        Err("provide a sandbox ID or --all")
    }
}

#[cfg(test)]
mod tests;

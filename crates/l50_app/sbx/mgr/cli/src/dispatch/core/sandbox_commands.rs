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
        } => handle_delete(client, id, all, template, blocking).await,
        _ => unreachable!("command routed to wrong dispatch group"),
    }
}

struct CreateCommandInput {
    content_hash: Option<pplx_space_common_core::types::ContentHash>,
    environment: Option<pplx_space_common_core::types::EnvironmentReference>,
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

use clap::Parser;
use comfy_table::{Table, presets::UTF8_FULL_CONDENSED};

use pplx_space_common_core::types::PolicyAuthority;
use pplx_space_net_common::proto::{
    EgressDefaultAction as NetworkPolicyAction, NetworkPolicyV1 as NetworkPolicyProto,
    NetworkPolicyVersion,
};
use pplx_space_node_api_client::proto::GetControllerStateRequest;
use pplx_space_template_mgr_client::proto::{
    CreatePoolRequest, DeletePoolRequest, DeleteTemplateBuildRequest, DeleteTemplateRequest,
    GetTemplateRequest, ListTemplatesRequest, PoolStatusRequest, RegisterTemplateRequest,
    TemplateRef, UpdateTemplateSettingsRequest, UpdateTemplateSnapshotPolicyRequest,
};

const FACET_NETWORK_POLICY: &str = "network-policies";
const FACET_TEMPLATE_BASE_SNAPSHOT_POLICY: &str = "template-base-snapshot-policy";

#[derive(Parser)]
#[command(
    name = "space-tmplmgr",
    about = "Manage templates",
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
    /// Set desired base snapshot policy for a template.
    BaseSnapshot {
        #[arg(long)]
        template: String,
        #[arg(long = "state")]
        #[arg(action = clap::ArgAction::Set)]
        state: bool,
        #[arg(long)]
        vcpu: Option<u32>,
        #[arg(long)]
        memory_mib: Option<u32>,
    },
    /// Create a pool for a template.
    CreatePool {
        #[arg(long)]
        template: String,
        #[arg(long)]
        count: Option<u32>,
        /// Boot mode for pool sandboxes: "snapshot" (from base snapshot) or "cold" (full boot)
        #[arg(long)]
        boot_mode: String,
    },
    /// Delete a pool.
    DeletePool {
        #[arg(long)]
        template: String,
    },
    /// Show pool status.
    PoolStatus {
        #[arg(long, default_value = "")]
        template: String,
    },
    /// Delete a template (stops pool, removes snapshot, deletes all files).
    DeleteTemplate {
        #[arg(long)]
        template: String,
    },
    /// Register a template on disk (compute content hash, index in DB).
    Register {
        #[arg(long)]
        template: String,
        #[arg(long, default_value = "latest")]
        tag: String,
        /// Prepared build.importing.* directory beneath this template's cache directory.
        #[arg(long)]
        staging_dir: Option<String>,
        /// Set the template network policy while registering it.
        #[arg(long)]
        network_policy: Option<NetworkPolicyMode>,
    },
    /// Get template build metadata, resolve tag/hash, match snapshot.
    Get {
        #[arg(long)]
        template: String,
        #[arg(long, default_value = "")]
        tag: String,
        #[arg(long, default_value = "")]
        content_hash: String,
        #[arg(long)]
        vcpu: Option<u32>,
        #[arg(long)]
        memory_mib: Option<u32>,
    },
    /// Delete a specific build (refuses if referenced by sandboxes/pools).
    DeleteBuild {
        #[arg(long)]
        name: String,
        #[arg(long)]
        content_hash: String,
    },
    /// Get or update template network policy.
    NetworkPolicy {
        #[arg(long)]
        template: String,
        /// Set policy to open or close.
        #[arg(long)]
        set: Option<NetworkPolicyMode>,
    },
    /// List all templates.
    List,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum NetworkPolicyMode {
    Open,
    Close,
}

impl NetworkPolicyMode {
    fn to_proto(self) -> NetworkPolicyProto {
        let action = match self {
            Self::Open => NetworkPolicyAction::Allow,
            Self::Close => NetworkPolicyAction::Deny,
        };
        NetworkPolicyProto {
            version: NetworkPolicyVersion::V1.into(),
            egress_default_action: action.into(),
            ..Default::default()
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Close => "close",
        }
    }
}

fn network_policy_mode_from_proto(
    policy: &NetworkPolicyProto,
) -> anyhow::Result<NetworkPolicyMode> {
    let action = NetworkPolicyAction::try_from(policy.egress_default_action)
        .unwrap_or(NetworkPolicyAction::Unspecified);
    match action {
        NetworkPolicyAction::Allow => Ok(NetworkPolicyMode::Open),
        NetworkPolicyAction::Deny => Ok(NetworkPolicyMode::Close),
        NetworkPolicyAction::Unspecified => Err(anyhow::anyhow!(
            "network policy has unspecified egress default action"
        )),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pplx_space_common_cli::init_logging("space-tmplmgr");
    let cli = Cli::parse();
    let mut client = pplx_space_template_mgr_client::connect()
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;

    match cli.command {
        Commands::BaseSnapshot {
            template,
            state,
            vcpu,
            memory_mib,
        } => {
            warn_if_inbound_reconciliation_active(FACET_TEMPLATE_BASE_SNAPSHOT_POLICY).await;
            let resp = client
                .update_template_snapshot_policy(UpdateTemplateSnapshotPolicyRequest {
                    template: template.clone(),
                    ensure_base_snapshot: state,
                    snapshot_vcpu: vcpu,
                    snapshot_memory_mib: memory_mib,
                })
                .await?;
            let r = resp.into_inner();
            println!("base snapshot policy for {template}: state={state}");
            if let Some(error) = r.error {
                println!("reconcile_error: {error}");
            } else if r.snapshot_built {
                println!(
                    "base snapshot built{}",
                    r.content_hash
                        .as_deref()
                        .map(|hash| format!(" for {hash}"))
                        .unwrap_or_default()
                );
            } else if r.snapshot_deleted {
                println!("base snapshot deleted");
            } else if r.reconciled {
                println!("reconciled");
            }
        }
        Commands::CreatePool {
            template,
            count,
            boot_mode,
        } => {
            if boot_mode != "snapshot" && boot_mode != "cold" {
                eprintln!("ERROR: --boot-mode must be 'snapshot' or 'cold'");
                std::process::exit(1);
            }
            client
                .create_pool(CreatePoolRequest {
                    template: template.clone(),
                    target_count: count,
                    boot_mode: boot_mode.clone(),
                    vcpu: None,
                    memory_mib: None,
                })
                .await?;
            let target_count = count.unwrap_or(3);
            println!("pool created for {template} (target: {target_count})");
        }
        Commands::DeletePool { template } => {
            let resp = client
                .delete_pool(DeletePoolRequest {
                    template: template.clone(),
                })
                .await?;
            let r = resp.into_inner();
            println!(
                "pool deleted for {template}, cleaned {} sandboxes",
                r.deleted_sandbox_ids.len()
            );
        }
        Commands::PoolStatus { template } => {
            let resp = client
                .pool_status(PoolStatusRequest {
                    template: if template.is_empty() {
                        None
                    } else {
                        Some(template)
                    },
                    ..PoolStatusRequest::default()
                })
                .await?;
            for p in resp.into_inner().pools {
                println!(
                    "{}: {}/{} ready, {} pending",
                    p.template, p.current_count, p.target_count, p.pending_count
                );
            }
        }
        Commands::DeleteTemplate { template } => {
            client
                .delete_template(DeleteTemplateRequest {
                    template: template.clone(),
                })
                .await?;
            println!("template '{template}' deleted");
        }
        Commands::Register {
            template,
            tag,
            network_policy,
            staging_dir,
        } => {
            if network_policy.is_some() {
                warn_if_inbound_reconciliation_active(FACET_NETWORK_POLICY).await;
            }
            let resp = client
                .register_template(RegisterTemplateRequest {
                    template: Some(TemplateRef {
                        name: template.clone(),
                        content_hash: None,
                        tag: Some(tag),
                    }),
                    network_policy: network_policy.map(NetworkPolicyMode::to_proto),
                    staging_dir,
                })
                .await?;
            let r = resp.into_inner();
            let content_hash = r
                .template
                .as_ref()
                .and_then(|t| t.content_hash.as_deref())
                .unwrap_or("<unknown>");
            println!("registered '{template}'");
            println!("  content_hash: {content_hash}");
            println!("  build_date:   {}", r.build_date);
            if let Some(mode) = network_policy {
                println!("  network:      {}", mode.as_str());
            }
        }
        Commands::Get {
            template,
            tag,
            content_hash,
            vcpu,
            memory_mib,
        } => {
            let resp = client
                .get_template(GetTemplateRequest {
                    caller_org_id: None,
                    template: Some(TemplateRef {
                        name: template,
                        content_hash: if content_hash.is_empty() {
                            None
                        } else {
                            Some(content_hash)
                        },
                        tag: if tag.is_empty() { None } else { Some(tag) },
                    }),
                    vcpu,
                    memory_mib,
                })
                .await?;
            let r = resp.into_inner();
            let tmpl = r.template.as_ref();
            println!("template_id:  {}", tmpl.map_or("", |t| t.name.as_str()));
            println!(
                "content_hash: {}",
                tmpl.and_then(|t| t.content_hash.as_deref()).unwrap_or("")
            );
            println!("build_date:   {}", r.build_date);
            println!("state:        {}", r.state);
            println!("runtime:      {}", r.runtime);
            println!("arch:         {}", r.arch);
            if !r.template_dir.is_empty() {
                println!("template_dir: {}", r.template_dir);
            }
            if let Some(ref sd) = r.snapshot_dir
                && !sd.is_empty()
            {
                println!("snapshot_dir: {sd}");
            }
            if let Some(ref sh) = r.snapshot_hash
                && !sh.is_empty()
            {
                println!(
                    "snapshot:     {} (vcpu={}, mem={}MiB)",
                    sh,
                    r.snapshot_vcpu.unwrap_or(0),
                    r.snapshot_memory_mib.unwrap_or(0)
                );
            }
            if !r.available_snapshots.is_empty() {
                println!("available_snapshots:");
                for s in &r.available_snapshots {
                    println!(
                        "  vcpu={}, memory_mib={}, hash={}",
                        s.vcpu, s.memory_mib, s.snapshot_hash
                    );
                }
            }
        }
        Commands::DeleteBuild { name, content_hash } => {
            client
                .delete_template_build(DeleteTemplateBuildRequest {
                    template: Some(TemplateRef {
                        name: name.clone(),
                        content_hash: Some(content_hash.clone()),
                        tag: None,
                    }),
                })
                .await?;
            println!("deleted build {content_hash} of '{name}'");
        }
        Commands::NetworkPolicy { template, set } => {
            if let Some(mode) = set {
                warn_if_inbound_reconciliation_active(FACET_NETWORK_POLICY).await;
                let current_generation = client
                    .list_templates(ListTemplatesRequest::default())
                    .await?
                    .into_inner()
                    .templates
                    .into_iter()
                    .find(|info| info.name == template)
                    .map_or(0, |info| info.network_policy_generation);
                client
                    .update_template_settings(UpdateTemplateSettingsRequest {
                        template: template.clone(),
                        default_auto_lifecycle: None,
                        network_policy: Some(mode.to_proto()),
                        network_policy_generation: current_generation.saturating_add(1).max(1),
                        network_policy_authority: PolicyAuthority::Local.as_proto_i32(),
                        auto_lifecycle_authority: PolicyAuthority::Cluster.as_proto_i32(),
                    })
                    .await?;
            }

            let resp = client
                .list_templates(ListTemplatesRequest::default())
                .await?;
            let template_info = resp
                .into_inner()
                .templates
                .into_iter()
                .find(|entry| entry.name == template)
                .ok_or_else(|| anyhow::anyhow!("template not found: {template}"))?;
            let policy = template_info
                .network_policy
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("template {template} has no network policy"))?;
            let mode = network_policy_mode_from_proto(policy)?;

            println!("template: {template}");
            println!("policy: {}", mode.as_str());
        }
        Commands::List => {
            let resp = client
                .list_templates(ListTemplatesRequest::default())
                .await?;
            let templates = resp.into_inner().templates;
            if templates.is_empty() {
                println!("no templates registered");
                return Ok(());
            }
            let mut table = Table::new();
            table.load_style(UTF8_FULL_CONDENSED);
            table.set_header(vec![
                "NAME",
                "CONTENT HASH",
                "RUNTIME",
                "BUILT",
                "DISK",
                "ON DISK",
                "BASE SNAP",
                "POOL",
            ]);
            for t in templates {
                let content_hash = t.content_hash.as_deref().unwrap_or("");
                let hash_display = content_hash
                    .strip_prefix("sha256:")
                    .or_else(|| content_hash.strip_prefix("blake3:"))
                    .unwrap_or(content_hash);
                let hash_short = hash_display.chars().take(12).collect::<String>();
                let base_snapshot_col = format_base_snapshot(&t);
                table.add_row(vec![
                    t.name.clone(),
                    if hash_short.is_empty() {
                        "-".into()
                    } else {
                        hash_short
                    },
                    t.runtime.unwrap_or_else(|| "-".into()),
                    t.built_at.unwrap_or_else(|| "-".into()),
                    t.disk_size_mb.map_or_else(|| "-".into(), format_mib),
                    t.cache_size_bytes.map_or_else(|| "-".into(), format_bytes),
                    base_snapshot_col,
                    if let Some(pool_target) = t.pool_target {
                        format!("{}/{}", t.pool_current, pool_target)
                    } else {
                        "-".into()
                    },
                ]);
            }
            println!("{table}");
        }
    }

    Ok(())
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

fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        return "-".into();
    }
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    if bytes < 1024 * 1024 {
        return format!("{:.1} KiB", bytes as f64 / 1024.0);
    }
    if bytes < 1024 * 1024 * 1024 {
        return format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0));
    }
    format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

fn format_mib(mib: u32) -> String {
    format_bytes(u64::from(mib) * 1024 * 1024)
}

fn format_base_snapshot(
    template: &pplx_space_template_mgr_client::proto::TemplateFullInfo,
) -> String {
    let vcpu = template
        .snapshot_vcpu
        .or(template.vcpu)
        .filter(|value| *value > 0)
        .unwrap_or(2);
    let memory_mib = template
        .snapshot_memory_mib
        .or(template.memory_mib)
        .filter(|value| *value > 0)
        .unwrap_or(1024);
    let desired_config = format_snapshot_config(vcpu, memory_mib);

    if template.has_base_snapshot {
        let observed = template
            .snapshots
            .iter()
            .find(|snapshot| snapshot.vcpu == vcpu && snapshot.memory_mib == memory_mib)
            .or_else(|| template.snapshots.first());
        let observed_config = observed.map_or_else(
            || desired_config.clone(),
            |snapshot| format_snapshot_config(snapshot.vcpu, snapshot.memory_mib),
        );
        let status = if observed_config == desired_config {
            format!("on {desired_config}")
        } else {
            format!("on {observed_config}; want {desired_config}")
        };
        return template
            .base_snapshot_size_bytes
            .map(|bytes| format!("{status} ({})", format_bytes(bytes)))
            .unwrap_or(status);
    }

    if template.ensure_base_snapshot.unwrap_or(false) {
        format!("missing {desired_config}")
    } else {
        format!("off {desired_config}")
    }
}

fn format_snapshot_config(vcpu: u32, memory_mib: u32) -> String {
    format!("{vcpu}c/{memory_mib}MiB")
}

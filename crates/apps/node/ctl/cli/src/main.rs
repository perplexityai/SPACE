use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::{Parser, Subcommand};
use comfy_table::{Cell, Table, presets::UTF8_FULL};
use pplx_space_common_core::types::{SimpleDurationParseError, parse_simple_duration_secs};
use pplx_space_node_api_client::proto::node_ctl_client::NodeCtlClient;
use pplx_space_node_api_client::proto::{
    FacetControllerState, FacetDirection, FacetQueueItem, FacetRef, FacetSyncOverride,
    GetControllerStateRequest, GetControllerStateResponse, MarkFacetDirtyRequest,
    NetworkPolicyTargetRef, ReconcileFacet, ReconciliationStatus, RefreshFacetStateRequest,
    SetFacetSyncDisabledRequest, facet_ref, network_policy_target_ref,
};
use serde::Serialize;
use tonic::transport::Channel;

#[derive(Parser)]
#[command(
    name = "space-nodectl",
    about = "SPACE node control CLI",
    version,
    disable_version_flag = true
)]
struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    _version: (),

    #[arg(long, default_value = pplx_space_common_core::host::linux::paths::NODE_CTL_SOCKET)]
    socket: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Show node control status.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Facet controller operations.
    Facet {
        #[command(subcommand)]
        command: FacetCommand,
    },
    /// Ask node control to reconcile state now.
    Reconcile {
        #[arg(long)]
        facet: String,

        #[arg(long)]
        element: Vec<String>,
    },
    /// Temporarily pause one reconciliation direction for a facet.
    Disable {
        facet: String,
        direction: SyncDirectionArg,

        #[arg(long, default_value = "10m")]
        ttl: String,

        #[arg(long)]
        no_expiry: bool,

        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Resume one reconciliation direction for a facet.
    Enable {
        facet: String,
        direction: SyncDirectionArg,
    },
}

#[derive(Subcommand)]
enum FacetCommand {
    /// List known facets and supported directions.
    List {
        #[arg(long)]
        json: bool,
    },
    /// Show per-facet inbound/outbound controller state.
    Status {
        facet: Option<String>,

        #[arg(long)]
        json: bool,
    },
    /// Show outbound dirty queue rows.
    Queue {
        facet: Option<String>,

        #[arg(long)]
        json: bool,
    },
    /// Show active inbound/outbound pauses.
    Pauses {
        #[arg(long)]
        json: bool,
    },
    /// Temporarily pause one reconciliation direction for a facet.
    Pause {
        facet: String,
        direction: SyncDirectionArg,

        #[arg(long, default_value = "10m")]
        ttl: String,

        #[arg(long)]
        no_expiry: bool,

        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Resume one reconciliation direction for a facet.
    Resume {
        facet: String,
        direction: SyncDirectionArg,
    },
    /// Reconcile inbound state now.
    Reconcile {
        facet: Option<String>,

        #[arg(long)]
        element: Vec<String>,
    },
    /// Publish outbound state now instead of on the next poll. Without
    /// --element the whole facet is published: a full sandbox-inventory
    /// census, or template-inventory with the node's Kubernetes object
    /// reconciles that ride it.
    Publish {
        facet: String,

        #[arg(long)]
        element: Vec<String>,
    },
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum SyncDirectionArg {
    Inbound,
    Outbound,
}

impl SyncDirectionArg {
    fn to_proto(self) -> FacetDirection {
        match self {
            Self::Inbound => FacetDirection::Inbound,
            Self::Outbound => FacetDirection::Outbound,
        }
    }
}

#[derive(Serialize)]
struct ControllerStateJson {
    cluster_sync_configured: bool,
    facets: Vec<FacetJson>,
    outbound_queue: Vec<QueueItemJson>,
    pauses: Vec<PauseJson>,
}

#[derive(Serialize)]
struct FacetJson {
    facet: String,
    supports_inbound: bool,
    supports_outbound: bool,
    inbound: Option<DirectionJson>,
    outbound: Option<DirectionJson>,
}

#[derive(Serialize)]
struct DirectionJson {
    state: String,
    running: bool,
    paused: Option<PauseJson>,
    last_started_at_unix_secs: u64,
    last_completed_at_unix_secs: u64,
    last_success_at_unix_secs: u64,
    next_due_at_unix_secs: u64,
    templates_seen: u64,
    sandboxes_seen: u64,
    templates_applied: u64,
    sandboxes_applied: u64,
    last_error: String,
}

#[derive(Clone, Serialize)]
struct PauseJson {
    facet: String,
    direction: String,
    disabled_until_unix_secs: Option<u64>,
    reason: String,
    updated_at_unix_secs: u64,
}

#[derive(Serialize)]
struct QueueItemJson {
    facet: String,
    element: Option<String>,
    attempts: u64,
    last_error: String,
    updated_at_unix_secs: u64,
    last_attempt_at_unix_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut client = connect(cli.socket).await?;

    match cli.command.unwrap_or(Command::Status { json: false }) {
        Command::Status { json } => {
            let state = get_controller_state(&mut client, None, true).await?;
            if json {
                print_json(&state)?;
            } else {
                print_controller_status(&state);
            }
        }
        Command::Facet { command } => handle_facet_command(&mut client, command).await?,
        Command::Reconcile { facet, element } => {
            reconcile_facet(&mut client, Some(facet), element).await?;
            println!("facet reconciliation enqueued");
        }
        Command::Disable {
            facet,
            direction,
            ttl,
            no_expiry,
            reason,
        } => {
            pause_facet(&mut client, facet, direction, ttl, no_expiry, reason).await?;
            println!("facet sync paused");
        }
        Command::Enable { facet, direction } => {
            resume_facet(&mut client, facet, direction).await?;
            println!("facet sync resumed");
        }
    }

    Ok(())
}

async fn handle_facet_command(
    client: &mut NodeCtlClient<Channel>,
    command: FacetCommand,
) -> Result<()> {
    match command {
        FacetCommand::List { json } => {
            let state = get_controller_state(client, None, false).await?;
            if json {
                print_json(&state)?;
            } else {
                print_facet_list(&state.facets);
            }
        }
        FacetCommand::Status { facet, json } => {
            let state = get_controller_state(client, facet, true).await?;
            if json {
                print_json(&state)?;
            } else {
                print_controller_status(&state);
            }
        }
        FacetCommand::Queue { facet, json } => {
            let state = get_controller_state(client, facet, true).await?;
            if json {
                print_json(&state)?;
            } else {
                print_queue(&state.outbound_queue);
            }
        }
        FacetCommand::Pauses { json } => {
            let state = get_controller_state(client, None, false).await?;
            if json {
                print_json(&state)?;
            } else {
                print_pauses(&state.sync_overrides);
            }
        }
        FacetCommand::Pause {
            facet,
            direction,
            ttl,
            no_expiry,
            reason,
        } => {
            pause_facet(client, facet, direction, ttl, no_expiry, reason).await?;
            println!("facet sync paused");
        }
        FacetCommand::Resume { facet, direction } => {
            resume_facet(client, facet, direction).await?;
            println!("facet sync resumed");
        }
        FacetCommand::Reconcile { facet, element } => {
            reconcile_facet(client, facet, element).await?;
            println!("facet reconciliation enqueued");
        }
        FacetCommand::Publish { facet, element } => {
            publish_facet(client, facet, element).await?;
            println!("facet publication enqueued");
        }
    }
    Ok(())
}

async fn connect(path: PathBuf) -> Result<NodeCtlClient<Channel>> {
    let socket_path = path.display().to_string();
    let channel = tonic::transport::Endpoint::try_from("http://[::]:50051")
        .expect("static endpoint URI is valid")
        .connect_with_connector(tower::service_fn(move |_| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
        .map_err(|error| {
            if error_chain_has_io_kind(&error, std::io::ErrorKind::PermissionDenied) {
                anyhow!(
                    "failed to connect to node-ctl socket {socket_path}: {error}\n\
                     hint: reconnect to pick up the space-nodectlclients group, disable SSH ControlMaster for this login, or run `sg space-nodectlclients -c 'space-nodectl status'`"
                )
            } else {
                anyhow!("failed to connect to node-ctl socket {socket_path}: {error}")
            }
        })?;
    Ok(NodeCtlClient::new(channel))
}

fn error_chain_has_io_kind(
    error: &(dyn std::error::Error + 'static),
    kind: std::io::ErrorKind,
) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == kind)
        {
            return true;
        }
        current = error.source();
    }
    false
}

async fn get_controller_state(
    client: &mut NodeCtlClient<Channel>,
    facet: Option<String>,
    include_queue: bool,
) -> Result<GetControllerStateResponse> {
    Ok(client
        .get_controller_state(GetControllerStateRequest {
            facet,
            include_queue,
        })
        .await?
        .into_inner())
}

async fn pause_facet(
    client: &mut NodeCtlClient<Channel>,
    facet: String,
    direction: SyncDirectionArg,
    ttl: String,
    no_expiry: bool,
    reason: String,
) -> Result<()> {
    let ttl_secs = if no_expiry {
        None
    } else {
        Some(parse_duration_secs(&ttl)?)
    };
    client
        .set_facet_sync_disabled(SetFacetSyncDisabledRequest {
            facet,
            direction: direction.to_proto().into(),
            disabled: true,
            ttl_secs,
            no_expiry,
            reason,
        })
        .await?;
    Ok(())
}

async fn resume_facet(
    client: &mut NodeCtlClient<Channel>,
    facet: String,
    direction: SyncDirectionArg,
) -> Result<()> {
    client
        .set_facet_sync_disabled(SetFacetSyncDisabledRequest {
            facet,
            direction: direction.to_proto().into(),
            disabled: false,
            ttl_secs: None,
            no_expiry: false,
            reason: String::new(),
        })
        .await?;
    Ok(())
}

async fn reconcile_facet(
    client: &mut NodeCtlClient<Channel>,
    facet: Option<String>,
    elements: Vec<String>,
) -> Result<()> {
    let refs = if let Some(facet) = facet {
        if elements.is_empty() {
            vec![facet_ref_from_args(&facet, None)?]
        } else {
            elements
                .into_iter()
                .map(|element| facet_ref_from_args(&facet, Some(element)))
                .collect::<Result<Vec<_>>>()?
        }
    } else {
        Vec::new()
    };
    client
        .refresh_facet_state(RefreshFacetStateRequest { refs })
        .await?;
    Ok(())
}

/// Mark `facet` (or the named elements of it) dirty, so node control's
/// outbound sync publishes it now. Refused for a facet node control does not
/// publish: its dirty row would never be claimed.
async fn publish_facet(
    client: &mut NodeCtlClient<Channel>,
    facet: String,
    elements: Vec<String>,
) -> Result<()> {
    let state = get_controller_state(client, Some(facet.clone()), false).await?;
    if !state
        .facets
        .iter()
        .any(|known| known.facet == facet && known.supports_outbound)
    {
        anyhow::bail!("facet {facet:?} has no outbound publication");
    }
    let refs = if elements.is_empty() {
        vec![facet_ref_from_args(&facet, None)?]
    } else {
        elements
            .into_iter()
            .map(|element| facet_ref_from_args(&facet, Some(element)))
            .collect::<Result<Vec<_>>>()?
    };
    client
        .mark_facet_dirty(MarkFacetDirtyRequest { refs })
        .await?;
    Ok(())
}

fn facet_ref_from_args(facet: &str, element: Option<String>) -> Result<FacetRef> {
    let (facet, element) = match facet {
        "network-policy" => (
            ReconcileFacet::NetworkPolicy,
            element
                .as_deref()
                .map(network_policy_target_ref_from_arg)
                .transpose()?
                .map(facet_ref::Element::NetworkPolicyTarget),
        ),
        "sandbox-inventory" | "sandbox-desired-state" => (
            if facet == "sandbox-inventory" {
                ReconcileFacet::SandboxInventory
            } else {
                ReconcileFacet::SandboxDesiredState
            },
            element.map(facet_ref::Element::SandboxId),
        ),
        "template-inventory" => (
            ReconcileFacet::TemplateInventory,
            element.map(facet_ref::Element::Template),
        ),
        "template-base-snapshot-policy" => (
            ReconcileFacet::TemplateBaseSnapshotPolicy,
            element.map(facet_ref::Element::Template),
        ),
        "template-desired-state" => (
            ReconcileFacet::TemplateDesiredState,
            element.map(facet_ref::Element::Template),
        ),
        "node-status" | "runtime-provisioning" => {
            if element.is_some() {
                anyhow::bail!("{facet} facet does not accept an element");
            }
            let facet = if facet == "node-status" {
                ReconcileFacet::NodeStatus
            } else {
                ReconcileFacet::RuntimeProvisioning
            };
            (facet, None)
        }
        _ => anyhow::bail!("unknown facet {facet:?}"),
    };
    Ok(FacetRef {
        facet: facet as i32,
        element,
    })
}

fn network_policy_target_ref_from_arg(value: &str) -> Result<NetworkPolicyTargetRef> {
    if let Some(template) = value.strip_prefix("template:") {
        return Ok(NetworkPolicyTargetRef {
            target: Some(network_policy_target_ref::Target::Template(
                template.to_string(),
            )),
        });
    }
    if let Some(sandbox_id) = value.strip_prefix("sandbox:") {
        return Ok(NetworkPolicyTargetRef {
            target: Some(network_policy_target_ref::Target::SandboxId(
                sandbox_id.to_string(),
            )),
        });
    }
    anyhow::bail!("network-policy element must start with template: or sandbox:")
}

fn print_controller_status(state: &GetControllerStateResponse) {
    println!(
        "Cluster sync: {}",
        if state.cluster_sync_configured {
            "configured"
        } else {
            "not configured"
        }
    );

    let mut table = Table::new();
    table.load_style(UTF8_FULL);
    table.set_header(["Facet", "Inbound", "Outbound", "Queue", "Error"]);

    for facet in &state.facets {
        let queue_count = state
            .outbound_queue
            .iter()
            .filter(|item| item.facet == facet.facet)
            .count();
        table.add_row([
            Cell::new(&facet.facet),
            Cell::new(direction_summary(
                facet.supports_inbound,
                facet.inbound.as_ref(),
                facet.inbound_pause.as_ref(),
            )),
            Cell::new(direction_summary(
                facet.supports_outbound,
                facet.outbound.as_ref(),
                facet.outbound_pause.as_ref(),
            )),
            Cell::new(if facet.supports_outbound {
                queue_count.to_string()
            } else {
                "n/a".to_string()
            }),
            Cell::new(first_error(facet).unwrap_or_else(|| "-".to_string())),
        ]);
    }
    println!("{table}");
}

fn print_facet_list(facets: &[FacetControllerState]) {
    let mut table = Table::new();
    table.load_style(UTF8_FULL);
    table.set_header(["Facet", "Inbound", "Outbound"]);
    for facet in facets {
        table.add_row([
            Cell::new(&facet.facet),
            Cell::new(if facet.supports_inbound { "yes" } else { "no" }),
            Cell::new(if facet.supports_outbound { "yes" } else { "no" }),
        ]);
    }
    println!("{table}");
}

fn print_queue(rows: &[FacetQueueItem]) {
    if rows.is_empty() {
        println!("Outbound queue is empty.");
        return;
    }

    let mut table = Table::new();
    table.load_style(UTF8_FULL);
    table.set_header([
        "Facet",
        "Element",
        "Dirty Since",
        "Attempts",
        "Last Attempt",
        "Error",
    ]);
    for row in rows {
        table.add_row([
            Cell::new(&row.facet),
            Cell::new(row.element.as_deref().unwrap_or("*")),
            Cell::new(format_past(row.updated_at_unix_secs)),
            Cell::new(row.attempts),
            Cell::new(format_past(row.last_attempt_at_unix_secs)),
            Cell::new(if row.last_error.is_empty() {
                "-".to_string()
            } else {
                row.last_error.clone()
            }),
        ]);
    }
    println!("{table}");
}

fn print_pauses(rows: &[FacetSyncOverride]) {
    if rows.is_empty() {
        println!("No active facet sync pauses.");
        return;
    }

    let mut table = Table::new();
    table.load_style(UTF8_FULL);
    table.set_header(["Facet", "Direction", "Disabled Until", "Updated", "Reason"]);
    for row in rows {
        table.add_row([
            Cell::new(&row.facet),
            Cell::new(direction_name(row.direction)),
            Cell::new(
                row.disabled_until_unix_secs
                    .map_or_else(|| "no expiry".to_string(), format_next),
            ),
            Cell::new(format_past(row.updated_at_unix_secs)),
            Cell::new(if row.reason.is_empty() {
                "-".to_string()
            } else {
                row.reason.clone()
            }),
        ]);
    }
    println!("{table}");
}

fn direction_summary(
    supported: bool,
    status: Option<&ReconciliationStatus>,
    pause: Option<&FacetSyncOverride>,
) -> String {
    if !supported {
        return "n/a".to_string();
    }
    if let Some(pause) = pause {
        return pause.disabled_until_unix_secs.map_or_else(
            || "paused, no expiry".to_string(),
            |disabled_until| format!("paused, resumes {}", relative_next(disabled_until)),
        );
    }
    let Some(status) = status else {
        return "never".to_string();
    };
    if status.running {
        return append_next("running".to_string(), status.next_due_at_unix_secs);
    }
    if !status.last_error.is_empty() {
        return append_next(
            format!("failed {}", age(status.last_completed_at_unix_secs)),
            status.next_due_at_unix_secs,
        );
    }
    if status.last_success_at_unix_secs != 0 {
        return append_next(
            format!("ok {}", age(status.last_success_at_unix_secs)),
            status.next_due_at_unix_secs,
        );
    }
    if status.last_completed_at_unix_secs != 0 {
        return append_next(
            format!("idle {}", age(status.last_completed_at_unix_secs)),
            status.next_due_at_unix_secs,
        );
    }
    "never".to_string()
}

fn append_next(summary: String, next_due_at_unix_secs: u64) -> String {
    if next_due_at_unix_secs == 0 {
        summary
    } else {
        format!("{summary}, next: {}", relative_next(next_due_at_unix_secs))
    }
}

fn first_error(facet: &FacetControllerState) -> Option<String> {
    facet
        .inbound
        .as_ref()
        .and_then(|status| non_empty(status.last_error.as_str()))
        .or_else(|| {
            facet
                .outbound
                .as_ref()
                .and_then(|status| non_empty(status.last_error.as_str()))
        })
}

fn print_json(state: &GetControllerStateResponse) -> Result<(), serde_json::Error> {
    println!(
        "{}",
        serde_json::to_string_pretty(&controller_state_json(state))?
    );
    Ok(())
}

fn controller_state_json(state: &GetControllerStateResponse) -> ControllerStateJson {
    ControllerStateJson {
        cluster_sync_configured: state.cluster_sync_configured,
        facets: state
            .facets
            .iter()
            .map(|facet| FacetJson {
                facet: facet.facet.clone(),
                supports_inbound: facet.supports_inbound,
                supports_outbound: facet.supports_outbound,
                inbound: direction_json(
                    facet.supports_inbound,
                    facet.inbound.as_ref(),
                    facet.inbound_pause.as_ref(),
                ),
                outbound: direction_json(
                    facet.supports_outbound,
                    facet.outbound.as_ref(),
                    facet.outbound_pause.as_ref(),
                ),
            })
            .collect(),
        outbound_queue: state
            .outbound_queue
            .iter()
            .map(|row| QueueItemJson {
                facet: row.facet.clone(),
                element: row.element.clone(),
                attempts: row.attempts,
                last_error: row.last_error.clone(),
                updated_at_unix_secs: row.updated_at_unix_secs,
                last_attempt_at_unix_secs: row.last_attempt_at_unix_secs,
            })
            .collect(),
        pauses: state.sync_overrides.iter().map(pause_json).collect(),
    }
}

fn direction_json(
    supported: bool,
    status: Option<&ReconciliationStatus>,
    pause: Option<&FacetSyncOverride>,
) -> Option<DirectionJson> {
    supported.then(|| {
        let state = if pause.is_some() {
            "paused"
        } else if status.is_some_and(|status| status.running) {
            "running"
        } else if status.is_some_and(|status| !status.last_error.is_empty()) {
            "failed"
        } else if status.is_some_and(|status| status.last_success_at_unix_secs != 0) {
            "ok"
        } else {
            "never"
        };
        let status = status.cloned().unwrap_or_default();
        DirectionJson {
            state: state.to_string(),
            running: status.running,
            paused: pause.map(pause_json),
            last_started_at_unix_secs: status.last_started_at_unix_secs,
            last_completed_at_unix_secs: status.last_completed_at_unix_secs,
            last_success_at_unix_secs: status.last_success_at_unix_secs,
            next_due_at_unix_secs: status.next_due_at_unix_secs,
            templates_seen: status.templates_seen,
            sandboxes_seen: status.sandboxes_seen,
            templates_applied: status.templates_applied,
            sandboxes_applied: status.sandboxes_applied,
            last_error: status.last_error,
        }
    })
}

fn pause_json(row: &FacetSyncOverride) -> PauseJson {
    PauseJson {
        facet: row.facet.clone(),
        direction: direction_name(row.direction).to_string(),
        disabled_until_unix_secs: row.disabled_until_unix_secs,
        reason: row.reason.clone(),
        updated_at_unix_secs: row.updated_at_unix_secs,
    }
}

fn now_secs() -> u64 {
    jiff::Timestamp::now().as_second().try_into().unwrap_or(0)
}

fn format_duration(seconds: u64) -> String {
    let seconds = i64::try_from(seconds).unwrap_or(i64::MAX);
    let duration = jiff::SignedDuration::from_secs(seconds);
    format!("{duration:#}")
}

fn format_past(value: u64) -> String {
    if value == 0 {
        return "-".to_string();
    }
    let now = now_secs();
    if value <= now {
        format!("{value} ({} ago)", format_duration(now - value))
    } else {
        format!("{value} (in {})", format_duration(value - now))
    }
}

fn format_next(value: u64) -> String {
    if value == 0 {
        return "-".to_string();
    }
    let now = now_secs();
    if value <= now {
        format!("{value} (due {} ago)", format_duration(now - value))
    } else {
        format!("{value} (in {})", format_duration(value - now))
    }
}

fn relative_next(value: u64) -> String {
    if value == 0 {
        return "-".to_string();
    }
    let now = now_secs();
    if value < now {
        format!("due {} ago", format_duration(now - value))
    } else if value == now {
        "due now".to_string()
    } else {
        format!("in {}", format_duration(value - now))
    }
}

fn age(value: u64) -> String {
    if value == 0 {
        return "never".to_string();
    }
    let now = now_secs();
    if value <= now {
        format!("{} ago", format_duration(now - value))
    } else {
        format!("in {}", format_duration(value - now))
    }
}

fn direction_name(direction: i32) -> &'static str {
    match FacetDirection::try_from(direction).unwrap_or(FacetDirection::Unspecified) {
        FacetDirection::Inbound => "inbound",
        FacetDirection::Outbound => "outbound",
        FacetDirection::Unspecified => "unspecified",
    }
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn parse_duration_secs(value: &str) -> Result<u64, SimpleDurationParseError> {
    parse_simple_duration_secs(value)
}

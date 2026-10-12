use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use std::fmt::Write as _;

use comfy_table::{Table, presets::NOTHING};
use pplx_space_common_core::types::{
    ArtifactComponentKind as ComponentKind, ArtifactVersion, Digest, JobId, RuntimeKind as Runtime,
    RuntimeVersion,
};
use pplx_space_node_runtime_manager_client::execution::{ArtifactComponentKind, RuntimeKind};
use pplx_space_node_runtime_manager_client::labels::label;
use pplx_space_node_runtime_manager_client::proto::{self, *};

#[derive(Parser)]
#[command(
    name = "space-rtmgr",
    about = "Inspect the SPACE node runtime manager and manage its runtime releases and guest kernels",
    version
)]
struct Cli {
    /// Runtime-manager Unix socket.
    #[arg(long, global = true, default_value = pplx_space_node_runtime_manager_client::RUNTIME_MANAGER_SOCKET)]
    socket: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List the runtime releases and guest kernels on this node.
    List {
        /// Only list these kinds. Repeat to list several; omit to list all.
        #[arg(long = "kind", value_enum)]
        kinds: Vec<KindArg>,

        /// Also show architecture, state format, digest, layout, provider,
        /// and the scanned roots.
        #[arg(long, conflicts_with = "json")]
        wide: bool,

        /// Print the listing response as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Check that the paths the runtime manager relies on are protected from
    /// other accounts. Exits non-zero when any path is absent or unprotected.
    Status {
        /// Print the status response as JSON.
        #[arg(long)]
        json: bool,
    },
    /// List and manage components by verified identity.
    Components {
        #[command(subcommand)]
        command: ComponentsCommand,
    },
    /// Shorthand for kernel component operations using a version label.
    Kernels {
        #[command(subcommand)]
        command: KernelsCommand,
    },
    /// List, install, and uninstall runtime releases. An install never
    /// changes the release new sandboxes boot on: that is the release root's
    /// `default` alias.
    Runtimes {
        #[command(subcommand)]
        command: RuntimesCommand,
    },
    /// Show one install or uninstall.
    Operation {
        operation_id: JobId,
        /// Print the operation as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Cancel an operation that is still queued or downloading.
    Cancel {
        operation_id: JobId,
        /// Print the operation as JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ComponentsCommand {
    Available {
        #[arg(long)]
        kind: ComponentKind,
        #[arg(long, value_parser = parse_runtime)]
        runtime: Option<RuntimeArg>,
        #[arg(long)]
        json: bool,
    },
    Install {
        #[arg(long)]
        kind: ComponentKind,
        version: ArtifactVersion,
        digest: Digest,
        #[arg(long)]
        no_wait: bool,
        #[arg(long)]
        json: bool,
    },
    Uninstall {
        #[arg(long)]
        kind: ComponentKind,
        version: ArtifactVersion,
        digest: Digest,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum KernelsCommand {
    /// List the kernels published for a runtime's release line, and whether
    /// each is on this node.
    Available {
        /// The runtime, optionally with a line (fc-1.15) or release
        /// (fc-1.15.1). A runtime alone lists every pinned line; no flag means
        /// the runtime the component adapter publishes for.
        #[arg(long, value_parser = parse_runtime)]
        runtime: Option<RuntimeArg>,
        /// Print the response as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Download and install a kernel published for a runtime's release line.
    /// Requires root or the local operator account.
    Install {
        /// The kernel version, as `kernels available` lists it.
        version: String,
        /// The runtime line (fc-1.15) or release (fc-1.15.1) to install for.
        #[arg(long, value_parser = parse_runtime)]
        runtime: RuntimeArg,
        /// Return once the install is accepted instead of waiting for it.
        #[arg(long)]
        no_wait: bool,
        /// Print the operation as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Remove a kernel the runtime manager installed. Requires root or the
    /// local operator account.
    Uninstall {
        version: ArtifactVersion,
        /// Print the operation as JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum RuntimesCommand {
    /// List the releases this build can install, and whether each is on
    /// this node.
    Available {
        /// Print the response as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Download and install a runtime release. Requires root or the local
    /// operator account.
    Install {
        /// The release, such as fc-1.16.2.
        #[arg(value_parser = parse_release)]
        release: ReleaseArg,
        /// Return once the install is accepted instead of waiting for it.
        #[arg(long)]
        no_wait: bool,
        /// Print the operation as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Remove a runtime release the runtime manager installed. Requires root
    /// or the local operator account.
    Uninstall {
        /// The release, such as fc-1.16.2.
        #[arg(value_parser = parse_release)]
        release: ReleaseArg,
        /// Print the operation as JSON.
        #[arg(long)]
        json: bool,
    },
}

/// A runtime and, optionally, one of its versions: `fc`, `fc-1.15`,
/// `cloud-hypervisor-48.0`. Runtimes are spelled as the shared vocabulary
/// spells them, by alias or full name.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeArg {
    kind: Runtime,
    version: Option<String>,
}

impl RuntimeArg {
    /// The runtime as a request names it.
    fn kind(&self) -> i32 {
        RuntimeKind::from(self.kind) as i32
    }
}

fn parse_runtime(value: &str) -> Result<RuntimeArg, String> {
    if let Ok(kind) = Runtime::parse(value) {
        return Ok(RuntimeArg {
            kind,
            version: None,
        });
    }
    // A name may itself hold a hyphen, so try each one as the separator.
    value
        .match_indices('-')
        .find_map(|(at, _)| {
            let kind = Runtime::parse(value.get(..at)?).ok()?;
            let version = value.get(at + 1..).filter(|version| !version.is_empty())?;
            Some(RuntimeArg {
                kind,
                version: Some(version.to_string()),
            })
        })
        .ok_or_else(|| {
            format!("'{value}' is not a runtime; expected fc, or fc-<version> such as fc-1.15")
        })
}

/// A runtime and one of its releases: `fc-1.16.2`.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ReleaseArg {
    kind: Runtime,
    version: RuntimeVersion,
}

fn parse_release(value: &str) -> Result<ReleaseArg, String> {
    let runtime = parse_runtime(value)?;
    let version = runtime.version.ok_or_else(|| {
        format!("'{value}' names no release; expected fc-<version> such as fc-1.16.2")
    })?;
    let version = RuntimeVersion::parse(version.strip_prefix('v').unwrap_or(&version))
        .map_err(|error| format!("'{value}' names no release: {error}"))?;
    Ok(ReleaseArg {
        kind: runtime.kind,
        version,
    })
}

/// `fc-1.15.1`: a runtime's alias and a version of it. A kind this build
/// does not know shows its label.
fn runtime_label(kind: i32, version: &str) -> String {
    let name = RuntimeKind::try_from(kind)
        .ok()
        .and_then(|kind| Runtime::try_from(kind).ok())
        .map_or_else(
            || label::<RuntimeKind>(kind),
            |kind| kind.alias().to_string(),
        );
    format!("{name}-{version}")
}

#[derive(Clone, Copy, ValueEnum)]
enum KindArg {
    Runtime,
    Kernel,
}

/// Map `--kind` flags onto the request's artifact and component kinds. A
/// kernel is a component, so naming it also narrows components to kernels.
fn list_request(kinds: &[KindArg]) -> ListExecutionArtifactsRequest {
    let mut request = ListExecutionArtifactsRequest::default();
    for kind in kinds {
        match kind {
            KindArg::Runtime => request.kinds.push(ExecutionArtifactKind::Runtime as i32),
            KindArg::Kernel => {
                request.kinds.push(ExecutionArtifactKind::Component as i32);
                request
                    .component_kinds
                    .push(ArtifactComponentKind::Kernel as i32);
            }
        }
    }
    request
}

/// `firecracker` for a runtime, `kernel` for a component.
fn what(kind: i32, runtime_kind: i32, component_kind: i32) -> String {
    match ExecutionArtifactKind::try_from(kind) {
        Ok(ExecutionArtifactKind::Runtime) => label::<RuntimeKind>(runtime_kind),
        Ok(ExecutionArtifactKind::Component) => label::<ArtifactComponentKind>(component_kind),
        _ => label::<ExecutionArtifactKind>(kind),
    }
}

/// `space` for the runtime manager's own artifacts, `external` for everything
/// it only lists; `wide` adds the external provider.
fn managed_by(management: i32, provider: i32, wide: bool) -> String {
    match ArtifactManagement::try_from(management) {
        Ok(ArtifactManagement::SpaceManaged) => "space".to_string(),
        Ok(ArtifactManagement::External) if wide => {
            format!("external/{}", label::<ArtifactProvider>(provider))
        }
        Ok(ArtifactManagement::External) => "external".to_string(),
        _ => label::<ArtifactManagement>(management),
    }
}

/// `vendor/version-directory`, for example.
fn found_in(location: i32, provenance: i32) -> String {
    format!(
        "{}/{}",
        label::<ArtifactLocation>(location),
        label::<ArtifactProvenance>(provenance)
    )
}

/// A refused install, uninstall, or cancel, with how to get permission when
/// that is why.
fn mutation_error(status: tonic::Status) -> String {
    if status.code() == tonic::Code::PermissionDenied {
        format!(
            "ERROR: {}\nInstalling, uninstalling, and cancelling change the node: run the command as root or the local operator account.",
            status.message()
        )
    } else {
        format!("ERROR: {}", status.message())
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    pplx_space_common_cli::init_logging("space-rtmgr");
    match run(Cli::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let socket = pplx_space_node_runtime_manager_client::UnixSocketPath::parse(cli.socket.clone())
        .map_err(|error| format!("ERROR: invalid runtime-manager socket: {error}"))?;
    let mut client = pplx_space_node_runtime_manager_client::connect_at(socket)
        .await
        .map_err(|error| {
            format!(
                "ERROR: cannot connect to the runtime manager at {} (is space-rtmgr-svc running?): {error}",
                cli.socket.display()
            )
        })?;

    match cli.command {
        Command::List { kinds, wide, json } => {
            let response = client
                .list_execution_artifacts(list_request(&kinds))
                .await
                .map_err(|status| format!("ERROR: {}", status.message()))?
                .into_inner();
            if json {
                let rendered = serde_json::to_string_pretty(&response)
                    .map_err(|error| format!("ERROR: render JSON: {error}"))?;
                println!("{rendered}");
            } else {
                print!("{}", render_listing(&response, wide));
            }
            if !response.complete {
                eprintln!("WARNING: the listing is incomplete; see the issues it reports");
            }
            Ok(())
        }
        Command::Status { json } => {
            let response = client
                .get_status(GetStatusRequest {})
                .await
                .map_err(|status| format!("ERROR: {}", status.message()))?
                .into_inner();
            if json {
                let rendered = serde_json::to_string_pretty(&response)
                    .map_err(|error| format!("ERROR: render JSON: {error}"))?;
                println!("{rendered}");
            } else {
                print_status(&response);
            }
            status_outcome(&response)
        }
        Command::Kernels { command } => match command {
            KernelsCommand::Available { runtime, json } => {
                let response = available(
                    &mut client,
                    false,
                    Some(ComponentKind::Kernel),
                    runtime.as_ref(),
                )
                .await?;
                show_available(&response, json)
            }
            KernelsCommand::Install {
                version,
                runtime,
                no_wait,
                json,
            } => {
                let response = available(
                    &mut client,
                    false,
                    Some(ComponentKind::Kernel),
                    Some(&runtime),
                )
                .await?;
                let identity = unique_artifact(
                    response
                        .artifacts
                        .into_iter()
                        .filter_map(|entry| entry.identity)
                        .filter(|identity| artifact_version(identity) == Some(version.as_str())),
                )?;
                mutate(
                    &mut client,
                    identity,
                    ArtifactAction::Install,
                    no_wait,
                    json,
                )
                .await
            }
            KernelsCommand::Uninstall { version, json } => {
                let response = client
                    .list_execution_artifacts(list_request(&[KindArg::Kernel]))
                    .await
                    .map_err(mutation_error)?
                    .into_inner();
                let identity = installed_identity(response, InstalledArtifact::Kernel(&version))?;
                mutate(
                    &mut client,
                    identity,
                    ArtifactAction::Uninstall,
                    false,
                    json,
                )
                .await
            }
        },
        Command::Components { command } => match command {
            ComponentsCommand::Available {
                kind,
                runtime,
                json,
            } => {
                let response = available(&mut client, false, Some(kind), runtime.as_ref()).await?;
                show_available(&response, json)
            }
            ComponentsCommand::Install {
                kind,
                version,
                digest,
                no_wait,
                json,
            } => {
                mutate(
                    &mut client,
                    component_identity(kind, version, digest),
                    ArtifactAction::Install,
                    no_wait,
                    json,
                )
                .await
            }
            ComponentsCommand::Uninstall {
                kind,
                version,
                digest,
                json,
            } => {
                mutate(
                    &mut client,
                    component_identity(kind, version, digest),
                    ArtifactAction::Uninstall,
                    false,
                    json,
                )
                .await
            }
        },
        Command::Runtimes { command } => match command {
            RuntimesCommand::Available { json } => {
                let response = available(&mut client, true, None, None).await?;
                show_available(&response, json)
            }
            RuntimesCommand::Install {
                release,
                no_wait,
                json,
            } => {
                let identity = resolve_release(&mut client, &release).await?;
                mutate(
                    &mut client,
                    identity,
                    ArtifactAction::Install,
                    no_wait,
                    json,
                )
                .await
            }
            RuntimesCommand::Uninstall { release, json } => {
                let listing = client
                    .list_execution_artifacts(list_request(&[KindArg::Runtime]))
                    .await
                    .map_err(mutation_error)?
                    .into_inner();
                let identity = installed_identity(listing, InstalledArtifact::Runtime(&release))?;
                mutate(
                    &mut client,
                    identity,
                    ArtifactAction::Uninstall,
                    false,
                    json,
                )
                .await
            }
        },
        Command::Operation { operation_id, json } => {
            let operation = client
                .get_operation(GetOperationRequest {
                    operation_id: operation_id.to_string(),
                })
                .await
                .map_err(|status| format!("ERROR: {}", status.message()))?
                .into_inner()
                .operation
                .ok_or("ERROR: the runtime manager returned no operation")?;
            if json {
                print_json(&operation)
            } else {
                print!("{}", render_operation(&operation));
                Ok(())
            }
        }
        Command::Cancel { operation_id, json } => {
            let operation = client
                .cancel_operation(CancelOperationRequest {
                    operation_id: operation_id.to_string(),
                })
                .await
                .map_err(mutation_error)?
                .into_inner()
                .operation
                .ok_or("ERROR: the runtime manager returned no operation")?;
            // Cancelled is what was asked for; anything else the operation
            // ended as is reported as usual.
            if operation.state == OperationState::Cancelled as i32 {
                return if json {
                    print_json(&operation)
                } else {
                    print!("{}", render_operation(&operation));
                    Ok(())
                };
            }
            report(&operation, json)
        }
    }
}

const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

fn in_progress(operation: &Operation) -> bool {
    matches!(
        OperationState::try_from(operation.state),
        Ok(OperationState::Queued
            | OperationState::Downloading
            | OperationState::Publishing
            | OperationState::Removing
            | OperationState::Cancelling)
    )
}

async fn wait_for(
    client: &mut pplx_space_node_runtime_manager_client::RuntimeManagerGrpcClient,
    mut operation: Operation,
) -> Result<Operation, String> {
    while in_progress(&operation) {
        tokio::time::sleep(POLL_INTERVAL).await;
        operation = client
            .get_operation(GetOperationRequest {
                operation_id: operation.operation_id.clone(),
            })
            .await
            .map_err(|status| format!("ERROR: {}", status.message()))?
            .into_inner()
            .operation
            .ok_or("ERROR: the runtime manager returned no operation")?;
    }
    Ok(operation)
}

fn print_json(value: &impl serde::Serialize) -> Result<(), String> {
    let rendered = serde_json::to_string_pretty(value)
        .map_err(|error| format!("ERROR: render JSON: {error}"))?;
    println!("{rendered}");
    Ok(())
}

/// Print an operation and fail unless it succeeded or is still running.
fn report(operation: &Operation, json: bool) -> Result<(), String> {
    if json {
        print_json(operation)?;
    } else {
        print!("{}", render_operation(operation));
    }
    operation_outcome(operation)
}

fn operation_outcome(operation: &Operation) -> Result<(), String> {
    match OperationState::try_from(operation.state) {
        Ok(OperationState::Succeeded) => Ok(()),
        _ if in_progress(operation) => Ok(()),
        _ => Err(format!(
            "ERROR: {} {}: {}",
            label::<ArtifactAction>(operation.action),
            label::<OperationState>(operation.state),
            operation.error.as_deref().unwrap_or("no reason recorded")
        )),
    }
}

fn artifact_label(identity: Option<&ArtifactIdentity>) -> String {
    match identity.and_then(|identity| identity.artifact.as_ref()) {
        Some(artifact_identity::Artifact::Runtime(runtime)) => format!(
            "{} {}",
            runtime_label(runtime.kind, &runtime.version),
            runtime.digest
        ),
        Some(artifact_identity::Artifact::Component(component)) => format!(
            "{} {} {}",
            label::<ArtifactComponentKind>(component.kind),
            component.version,
            component.digest
        ),
        None => "unknown".into(),
    }
}

fn render_operation(operation: &Operation) -> String {
    let mut fields = vec![
        ("Operation", operation.operation_id.clone()),
        ("Action", label::<ArtifactAction>(operation.action)),
        ("Artifact", artifact_label(operation.identity.as_ref())),
    ];
    let state = label::<OperationState>(operation.state);
    fields.push((
        "State",
        match OperationOutcome::try_from(operation.outcome) {
            Ok(OperationOutcome::Unspecified) | Err(_) => state,
            Ok(_) => format!("{state} ({})", label::<OperationOutcome>(operation.outcome)),
        },
    ));
    let on_disk = operation.on_disk.as_ref();
    for (name, value) in [
        ("Error", operation.error.clone()),
        ("Path", on_disk.map(|on_disk| on_disk.path.clone())),
        ("Digest", on_disk.map(|on_disk| on_disk.digest.clone())),
    ] {
        if let Some(value) = value {
            fields.push((name, value));
        }
    }
    let mut out = String::new();
    for (name, value) in fields {
        let _ = writeln!(out, "{:<13}{value}", format!("{name}:"));
    }
    out
}

/// Megabytes with one decimal, which is how kernel sizes read best.
fn megabytes(bytes: u64) -> String {
    let tenths = (bytes * 10 + 524_288) / 1_048_576;
    format!("{}.{} MiB", tenths / 10, tenths % 10)
}

fn component_identity(
    kind: ComponentKind,
    version: ArtifactVersion,
    digest: Digest,
) -> ArtifactIdentity {
    ArtifactIdentity {
        artifact: Some(artifact_identity::Artifact::Component(
            proto::ComponentIdentity {
                kind: ArtifactComponentKind::from(kind) as i32,
                digest: digest.to_string(),
                version: version.to_string(),
            },
        )),
    }
}

fn artifact_version(identity: &ArtifactIdentity) -> Option<&str> {
    match identity.artifact.as_ref()? {
        artifact_identity::Artifact::Runtime(runtime) => Some(&runtime.version),
        artifact_identity::Artifact::Component(component) => Some(&component.version),
    }
}

/// Labels are conveniences; immutable identity must be unambiguous.
fn unique_artifact(
    identities: impl IntoIterator<Item = ArtifactIdentity>,
) -> Result<ArtifactIdentity, String> {
    let mut unique = None;
    for identity in identities {
        match &unique {
            Some(previous) if previous != &identity => {
                return Err("ERROR: ambiguous version; supply an explicit identity".into());
            }
            Some(_) => {}
            None => unique = Some(identity),
        }
    }
    unique.ok_or_else(|| "ERROR: no matching artifact identity".into())
}

async fn available(
    client: &mut pplx_space_node_runtime_manager_client::RuntimeManagerGrpcClient,
    runtimes: bool,
    component: Option<ComponentKind>,
    runtime: Option<&RuntimeArg>,
) -> Result<ListAvailableArtifactsResponse, String> {
    client
        .list_available_artifacts(ListAvailableArtifactsRequest {
            kinds: vec![if runtimes {
                ExecutionArtifactKind::Runtime
            } else {
                ExecutionArtifactKind::Component
            } as i32],
            component_kinds: component
                .map(|kind| vec![ArtifactComponentKind::from(kind) as i32])
                .unwrap_or_default(),
            runtime_kind: runtime.map_or(RuntimeKind::Unspecified as i32, RuntimeArg::kind),
            runtime_version: runtime.and_then(|runtime| runtime.version.clone()),
        })
        .await
        .map_err(mutation_error)
        .map(tonic::Response::into_inner)
}

enum InstalledArtifact<'a> {
    Runtime(&'a ReleaseArg),
    Kernel(&'a ArtifactVersion),
}

fn installed_identity(
    listing: ListExecutionArtifactsResponse,
    requested: InstalledArtifact<'_>,
) -> Result<ArtifactIdentity, String> {
    let (kind, runtime, component, version) = match requested {
        InstalledArtifact::Runtime(release) => (
            ExecutionArtifactKind::Runtime,
            RuntimeKind::from(release.kind),
            ArtifactComponentKind::Unspecified,
            release.version.as_str(),
        ),
        InstalledArtifact::Kernel(version) => (
            ExecutionArtifactKind::Component,
            RuntimeKind::Unspecified,
            ArtifactComponentKind::Kernel,
            version.as_str(),
        ),
    };
    let mut identities = Vec::new();
    for entry in listing.artifacts.into_iter().filter(|entry| {
        entry.kind == kind as i32
            && entry.management == ArtifactManagement::SpaceManaged as i32
            && entry.version.as_deref() == Some(version)
            && match requested {
                InstalledArtifact::Runtime(_) => entry.runtime_kind == runtime as i32,
                InstalledArtifact::Kernel(_) => entry.component_kind == component as i32,
            }
    }) {
        let digest = entry.digest.ok_or_else(|| {
            format!(
                "ERROR: managed artifact {} is present but its digest is unavailable: {}; inspect the listing and resolve missing evidence before uninstalling",
                entry.path,
                entry.detail.as_deref().unwrap_or("not measured"),
            )
        })?;
        let artifact = match requested {
            InstalledArtifact::Runtime(_) => {
                artifact_identity::Artifact::Runtime(proto::RuntimeArtifactIdentity {
                    kind: runtime as i32,
                    version: version.to_string(),
                    digest,
                })
            }
            InstalledArtifact::Kernel(_) => {
                artifact_identity::Artifact::Component(proto::ComponentIdentity {
                    kind: component as i32,
                    version: version.to_string(),
                    digest,
                })
            }
        };
        identities.push(ArtifactIdentity {
            artifact: Some(artifact),
        });
    }
    if identities.is_empty() && !listing.complete {
        return Err("ERROR: incomplete scan; cannot establish whether the managed artifact is installed; inspect scan issues and resolve the limit or unreadable root".into());
    }
    // A limit elsewhere does not invalidate measured identity evidence. The
    // node manager checks these bytes again under the store lock before removal.
    unique_artifact(identities)
}

async fn resolve_release(
    client: &mut pplx_space_node_runtime_manager_client::RuntimeManagerGrpcClient,
    release: &ReleaseArg,
) -> Result<ArtifactIdentity, String> {
    let filter = RuntimeArg {
        kind: release.kind,
        version: Some(release.version.to_string()),
    };
    let response = available(client, true, None, Some(&filter)).await?;
    unique_artifact(
        response
            .artifacts
            .into_iter()
            .filter_map(|entry| entry.identity)
            .filter(|identity| artifact_version(identity) == Some(release.version.as_str())),
    )
}

async fn mutate(
    client: &mut pplx_space_node_runtime_manager_client::RuntimeManagerGrpcClient,
    identity: ArtifactIdentity,
    action: ArtifactAction,
    no_wait: bool,
    json: bool,
) -> Result<(), String> {
    let operation = match action {
        ArtifactAction::Install => {
            client
                .install_artifact(InstallArtifactRequest {
                    identity: Some(identity),
                })
                .await
                .map_err(mutation_error)?
                .into_inner()
                .operation
        }
        ArtifactAction::Uninstall => {
            client
                .uninstall_artifact(UninstallArtifactRequest {
                    identity: Some(identity),
                })
                .await
                .map_err(mutation_error)?
                .into_inner()
                .operation
        }
        ArtifactAction::Unspecified => return Err("ERROR: an artifact action is required".into()),
    }
    .ok_or("ERROR: the runtime manager returned no operation")?;
    let operation = if no_wait {
        operation
    } else {
        wait_for(client, operation).await?
    };
    report(&operation, json)
}

fn show_available(response: &ListAvailableArtifactsResponse, json: bool) -> Result<(), String> {
    if json {
        print_json(response)
    } else {
        print!("{}", render_available(response));
        Ok(())
    }
}

fn render_available(response: &ListAvailableArtifactsResponse) -> String {
    let mut table = plain_table(&["ARTIFACT", "COMPATIBLE RUNTIMES", "SIZE", "MANAGED STORE"]);
    for artifact in &response.artifacts {
        table.add_row(vec![
            artifact_label(artifact.identity.as_ref()),
            artifact
                .compatible_runtimes
                .iter()
                .map(|runtime| runtime_label(runtime.kind, &runtime.version))
                .collect::<Vec<_>>()
                .join(", "),
            artifact.size.map_or_else(|| "—".into(), megabytes),
            label::<ArtifactPresence>(artifact.presence),
        ]);
    }
    format!("{table}\n")
}

/// Succeeds only when every path is protected, naming absent and unprotected
/// paths separately: an absent path is unprovisioned, an unprotected one is
/// exposed.
fn status_outcome(response: &GetStatusResponse) -> Result<(), String> {
    let count = |protection: PathProtection| {
        response
            .protected_paths
            .iter()
            .filter(|path| path.protection == protection as i32)
            .count()
    };
    let protected = count(PathProtection::Protected);
    let total = response.protected_paths.len();
    if protected == total {
        return Ok(());
    }
    let absent = count(PathProtection::Absent);
    let other = total - protected - absent;
    Err(format!(
        "ERROR: {protected} of {total} runtime-manager paths are protected; {absent} absent, {other} unprotected"
    ))
}

fn print_status(response: &GetStatusResponse) {
    println!(
        "Runtime manager account: uid {} gid {}",
        response.service_uid, response.service_gid
    );
    for path in &response.protected_paths {
        println!(
            "  {:<12} {:<19} {}",
            label::<PathProtection>(path.protection),
            label::<ProtectedPathRole>(path.role),
            path.path
        );
        for problem in &path.problems {
            println!("      - {problem}");
        }
    }
}

fn optional(value: Option<&str>) -> String {
    value.unwrap_or("-").to_string()
}

/// Marks a version the layout did not name.
const DERIVED_VERSION_MARK: &str = "[*]";

/// A borderless table, the columns separated by padding alone.
fn plain_table(header: &[&str]) -> Table {
    let mut table = Table::new();
    table.load_style(NOTHING);
    table.set_header(header.to_vec());
    table
}

/// The listing as text. The default fits a terminal: what each artifact is,
/// whether it is usable, whose it is, and where it lives, with problems listed
/// below the table instead of widening it. `wide` adds the evidence columns and
/// the scanned roots.
fn render_listing(response: &ListExecutionArtifactsResponse, wide: bool) -> String {
    let mut out = String::new();
    let mut derived_versions = false;
    if response.artifacts.is_empty() {
        out.push_str("No runtime releases or kernels found.\n");
    } else {
        let mut table = if wide {
            plain_table(&[
                "KIND", "VERSION", "STATUS", "OWNER", "ARCH", "STATE", "DIGEST", "FOUND IN", "PATH",
            ])
        } else {
            plain_table(&["KIND", "VERSION", "STATUS", "OWNER", "PATH"])
        };
        for artifact in &response.artifacts {
            let version = match artifact.version.as_deref() {
                Some(version)
                    if artifact.version_source
                        == ExecutionArtifactVersionSource::ImageBanner as i32 =>
                {
                    derived_versions = true;
                    format!("{version} {DERIVED_VERSION_MARK}")
                }
                version => optional(version),
            };
            let mut row = vec![
                what(
                    artifact.kind,
                    artifact.runtime_kind,
                    artifact.component_kind,
                ),
                version,
                label::<ExecutionArtifactStatus>(artifact.status),
                managed_by(artifact.management, artifact.provider, wide),
            ];
            if wide {
                row.extend([
                    optional(artifact.architecture.as_deref()),
                    artifact.state_format.as_ref().map_or_else(
                        || "-".to_string(),
                        |state| format!("{} {}", state.format, state.version),
                    ),
                    optional(artifact.digest.as_deref()),
                    found_in(artifact.location, artifact.provenance),
                ]);
            }
            row.push(artifact.path.clone());
            table.add_row(row);
        }
        let _ = writeln!(out, "{table}");
    }
    if derived_versions {
        let _ = writeln!(
            out,
            "{DERIVED_VERSION_MARK} version read from the kernel image"
        );
    }

    let details: Vec<_> = response
        .artifacts
        .iter()
        .filter_map(|artifact| {
            artifact.detail.as_deref().map(|detail| {
                format!(
                    "  {}: {}: {detail}",
                    artifact.path,
                    label::<ExecutionArtifactStatus>(artifact.status)
                )
            })
        })
        .collect();
    if !details.is_empty() {
        out.push_str("\nProblems:\n");
        for line in details {
            let _ = writeln!(out, "{line}");
        }
    }

    if !response.aliases.is_empty() {
        out.push_str("\nAliases (observed on disk; not a runtime-manager selection):\n");
        for alias in &response.aliases {
            let target = match (&alias.target, &alias.resolved) {
                (Some(target), Some(resolved)) if wide && target != resolved => {
                    format!("{target} ({resolved})")
                }
                (Some(target), _) => target.clone(),
                (None, _) => "-".to_string(),
            };
            let _ = match &alias.detail {
                Some(detail) => writeln!(out, "  {} -> {target}  [{detail}]", alias.path),
                None => writeln!(out, "  {} -> {target}", alias.path),
            };
        }
    }

    if wide {
        out.push_str("\nScanned roots:\n");
        let mut table = plain_table(&["KIND", "FOUND IN", "PRESENCE", "PATH"]);
        for root in &response.roots {
            table.add_row(vec![
                what(root.kind, root.runtime_kind, root.component_kind),
                found_in(root.location, root.provenance),
                label::<ScanRootPresence>(root.presence),
                root.path.clone(),
            ]);
        }
        let _ = writeln!(out, "{table}");
    }

    if !response.issues.is_empty() {
        out.push_str("\nIssues (the listing is incomplete):\n");
        for issue in &response.issues {
            let _ = writeln!(
                out,
                "  {} {}: {}",
                label::<ScanIssueKind>(issue.kind),
                issue.path,
                issue.detail
            );
        }
    }
    out
}

#[cfg(test)]
mod tests;

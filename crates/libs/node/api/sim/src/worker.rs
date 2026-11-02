//! In-memory simulator implementation of `WorkerApi` + `WorkerApiInternal`.
//!
//! Lifecycle RPCs mutate `SimState`; suspend writes a snapshot file under the
//! shared snapshot directory and the matching wake/restore reads it back on
//! the target node. RPCs outside the simulated surface return `Unimplemented`
//! so callers can't misinterpret a no-op as success. Network and facet RPCs
//! are no-op so reconcilers can observe the expected post-call state
//! transition without a real `node-manager`.
//!
//! The simulator deliberately does NOT depend on `pplx-space-node-api-server`:
//! the production server forwards everything to local Unix-socket backends,
//! which the simulator has no way of running. Keeping the impl self-contained
//! also satisfies "do not weaken production node-api-svc behavior"

use std::{collections::HashMap, pin::Pin, sync::Arc};

use pplx_space_common_core::types::{
    BackupAttemptId, EnvMap, EnvSanitizationIssue, LocalArtifactId, NodeManagedBy, PolicyAuthority,
};
use pplx_space_net_gateway_client::proto::{ServicePortIngressFrame, service_port_ingress_frame};
use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternal;
use pplx_space_node_api_client::proto::worker_api_server::WorkerApi;
use pplx_space_node_api_client::proto::{
    ListNodeTemplatesRequest, ListNodeTemplatesResponse, PullTemplateRequest, PullTemplateResponse,
    TriggerStateReconciliationRequest, TriggerStateReconciliationResponse,
};
use pplx_space_sbx_mgr_client::proto as sbx;
use pplx_space_template_mgr_client::proto as tmpl;
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::state::{SandboxRecord, SimState, SimStateError, status};
use crate::templates::default_template_builds;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// All WorkerApi methods that are out of scope for the first cut share this
/// stable error message so callers see a uniform reason.
const UNIMPLEMENTED_REASON: &str = "not implemented by simulated node-api";

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented(UNIMPLEMENTED_REASON))
}

fn map_fs_error(err: crate::fs::FsError) -> Status {
    use crate::fs::FsError;
    match err {
        FsError::NotAbsolute(p) | FsError::UnsafePath(p) => {
            Status::invalid_argument(format!("invalid guest path: {p}"))
        }
        FsError::UnsafeSandboxId(id) => {
            Status::invalid_argument(format!("invalid sandbox id: {id}"))
        }
        FsError::Io { path, source } => io_to_status(&path, source),
    }
}

fn sanitize_process_env(
    process_env: std::collections::HashMap<String, String>,
    context: &'static str,
) -> std::collections::BTreeMap<String, String> {
    let (env, issues) = EnvMap::sanitize(process_env);
    warn_env_sanitization_issues(context, &issues);
    env.into()
}

fn warn_env_sanitization_issues(context: &'static str, issues: &[EnvSanitizationIssue]) {
    if issues.is_empty() {
        return;
    }
    tracing::warn!(
        context = context,
        env_issue_count = issues.len(),
        env_issues = ?issues,
        "dropped invalid env entries"
    );
}

fn io_to_status(path: &str, err: std::io::Error) -> Status {
    use std::io::ErrorKind;
    let msg = format!("{path}: {err}");
    match err.kind() {
        ErrorKind::NotFound => Status::not_found(msg),
        ErrorKind::AlreadyExists => Status::already_exists(msg),
        ErrorKind::PermissionDenied => Status::permission_denied(msg),
        ErrorKind::InvalidInput => Status::invalid_argument(msg),
        _ => Status::internal(msg),
    }
}

async fn remove_state_record_if_present(state: &SimState, sandbox_id: &str) -> Result<(), Status> {
    match state.remove(sandbox_id).await {
        Ok(_) | Err(SimStateError::NotFound(_)) => Ok(()),
        Err(err) => Err(map_state_error(err)),
    }
}

/// The spaced process RPCs carry their payload in `operation`; the sandbox
/// manager rejects a missing payload with `InvalidArgument`, and so does the
/// simulator, so a simulator-backed client cannot pass a request production
/// would refuse. Same message as `grpc_spaced_files::required_operation`.
fn required_operation<T>(operation: Option<T>) -> Result<T, Status> {
    operation.ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))
}

fn required_process_id(process_id: &str) -> Result<(), Status> {
    if process_id.is_empty() {
        return Err(Status::invalid_argument("process_id is required"));
    }
    Ok(())
}

/// The one place execution ends for a sandbox. Every handler that takes a
/// sandbox out of execution (stop, kill, delete, cleanup) goes through here so
/// the fake processes cannot outlive it: a `sleep` started before a stop must
/// not still be listed, or keep a stream open, after the sandbox restarts.
async fn teardown_sandbox_execution(state: &SimState, sandbox_id: &str) {
    state.processes().terminate_sandbox(sandbox_id).await;
}

async fn remove_snapshot_if_present(state: &SimState, sandbox_id: &str) -> Result<(), Status> {
    let snapshot = state.snapshot_dir().join(format!("{sandbox_id}.json"));
    match tokio::fs::remove_file(&snapshot).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(io_to_status(&snapshot.display().to_string(), err)),
    }
}

async fn file_info_from_metadata(
    guest_path: &str,
    meta: &std::fs::Metadata,
) -> sbx::SpacedFileInfo {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let file_type = if meta.is_dir() {
        sbx::SpacedFileType::Directory as i32
    } else if meta.file_type().is_symlink() {
        sbx::SpacedFileType::Symlink as i32
    } else if meta.is_file() {
        sbx::SpacedFileType::File as i32
    } else {
        sbx::SpacedFileType::Other as i32
    };
    let modified_at = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| prost_types::Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        });
    let name = std::path::Path::new(guest_path)
        .file_name()
        .and_then(|s| s.to_str())
        .map(str::to_string);
    sbx::SpacedFileInfo {
        path: guest_path.to_string(),
        name,
        file_type,
        size_bytes: meta.len(),
        mode: meta.permissions().mode(),
        modified_at,
        owner_uid: 0,
        owner_gid: 0,
        symlink_target: None,
        link_count: Some(meta.nlink()),
    }
}

/// Collect directory entries up to `depth` levels under `host_root`, returning
/// guest-visible paths relative to the requested guest path.
async fn collect_dir_entries(
    guest_root: &str,
    host_root: &std::path::Path,
    depth: u32,
) -> Result<Vec<sbx::SpacedFileInfo>, Status> {
    let mut out = Vec::new();
    let mut stack: Vec<(std::path::PathBuf, String, u32)> = vec![(
        host_root.to_path_buf(),
        guest_root.trim_end_matches('/').to_string(),
        0,
    )];
    while let Some((host_dir, guest_dir, level)) = stack.pop() {
        if level >= depth {
            continue;
        }
        let mut read = match tokio::fs::read_dir(&host_dir).await {
            Ok(r) => r,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(err) => return Err(io_to_status(&host_dir.display().to_string(), err)),
        };
        while let Some(entry) = read
            .next_entry()
            .await
            .map_err(|err| io_to_status(&host_dir.display().to_string(), err))?
        {
            let name = entry.file_name().to_string_lossy().to_string();
            let guest_path = if guest_dir.is_empty() {
                format!("/{name}")
            } else {
                format!("{guest_dir}/{name}")
            };
            let meta = entry
                .metadata()
                .await
                .map_err(|err| io_to_status(&entry.path().display().to_string(), err))?;
            let is_dir = meta.is_dir();
            out.push(file_info_from_metadata(&guest_path, &meta).await);
            if is_dir {
                stack.push((entry.path(), guest_path, level + 1));
            }
        }
    }
    Ok(out)
}

fn map_state_error(err: SimStateError) -> Status {
    match err {
        SimStateError::NotFound(id) => Status::not_found(format!("sandbox {id} not found")),
        SimStateError::AlreadyExists(id) => {
            Status::already_exists(format!("sandbox {id} already exists"))
        }
        SimStateError::InvalidSandboxId(id) => {
            Status::invalid_argument(format!("invalid sandbox id: {id:?}"))
        }
        SimStateError::InvalidTransition {
            sandbox_id,
            from,
            to,
        } => Status::failed_precondition(format!(
            "sandbox {sandbox_id} cannot transition from {from} to {to}"
        )),
        SimStateError::SnapshotMissing { sandbox_id, dir } => Status::not_found(format!(
            "no snapshot for sandbox {sandbox_id} in shared dir {dir}"
        )),
        SimStateError::SnapshotIo { sandbox_id, source } => {
            Status::internal(format!("snapshot io error for {sandbox_id}: {source}"))
        }
        SimStateError::SnapshotDecode { sandbox_id, source } => {
            Status::internal(format!("snapshot decode error for {sandbox_id}: {source}"))
        }
    }
}

fn published_artifact(
    kind: sbx::ArtifactKind,
    origin: sbx::ArtifactOrigin,
) -> sbx::PublishedArtifact {
    sbx::PublishedArtifact {
        local_artifact_id: LocalArtifactId::new().to_string(),
        artifact_kind: kind as i32,
        origin: origin as i32,
        snapshot_name: None,
    }
}

fn sim_lifecycle(label: &str) -> sbx::SandboxLifecycleState {
    if label == status::RUNNING {
        sbx::SandboxLifecycleState::Started
    } else {
        sbx::SandboxLifecycleState::from_label(label)
            .unwrap_or(sbx::SandboxLifecycleState::Unspecified)
    }
}

/// The policy a record holds, reported as sandbox-manager reports its row's.
fn decoded_policy(encoded: &[u8]) -> Option<sbx::AutoLifecyclePolicy> {
    use prost::Message;
    if encoded.is_empty() {
        return None;
    }
    sbx::AutoLifecyclePolicy::decode(encoded).ok()
}

fn sandbox_info_from_record(rec: &crate::state::SandboxRecord) -> sbx::SandboxInfo {
    sbx::SandboxInfo {
        lifecycle: Some(sim_lifecycle(&rec.status) as i32),
        sandbox_id: rec.sandbox_id.clone(),
        template: Some(rec.template.clone()),
        pid: None,
        namespace: None,
        uptime: None,
        owner: Some(rec.user.clone()),
        image_hash: rec.content_hash.clone(),
        spaced_host_alive: rec.status == status::RUNNING,
        uid: None,
        slot: None,
        guest_cid: None,
        created_at: Some(rec.created_at.clone()),
        runtime: Some(rec.runtime.clone()),
        execution: None,
        netgtw_alive: true,
        snapshots_enabled: rec.snapshots_enabled,
        pool_template: rec.pool_template.clone(),
        auto_lifecycle: decoded_policy(&rec.access_idle_policy),
        auto_lifecycle_status: None,
        failure_reason: None,
        quarantine_reason_code: None,
        transition_receipt: None,
        state_degraded: false,
        runtime_alive: rec.status == status::RUNNING,
        vsock_raw_ping_ok: rec.status == status::RUNNING,
        last_guest_ready_at: None,
        last_guest_heartbeat_at: None,
        network_tx_bytes: None,
        network_rx_bytes: None,
        network_policy_generation: 0,
        applied_network_policy_generation: 0,
        applied_network_policy_source: 0,
        network_policy_authority: PolicyAuthority::Cluster.as_proto_i32(),
        auto_lifecycle_authority: rec.auto_lifecycle_authority.as_proto_i32(),
        current_local_artifact_id: None,
        current_local_artifact_kind: None,
        current_local_artifact_origin: None,
        current_local_artifact_created_at: None,
        state_changed_at: None,
        execution_generation: rec.execution_generation,
        pod_uid: None,
        activation_attempt_id: None,
        spaced_version: None,
        netgtw_git_sha: None,
        netgtw_stale: None,
        health_status: sbx::SandboxHealthStatus::Healthy as i32,
        health_observed_at: None,
        route_provenance_generation: rec.route_generation,
        route_provenance_execution_generation: rec.execution_generation,
        quarantine_recovery: None,
        route_provenance_substrate_kind: match rec.route_substrate_kind {
            Some(NodeManagedBy::Space) => sbx::SandboxSubstrateKind::Direct as i32,
            Some(NodeManagedBy::Kubernetes) => sbx::SandboxSubstrateKind::Kubernetes as i32,
            None => sbx::SandboxSubstrateKind::Unspecified as i32,
        },
    }
}

/// Implementation of SpacedWrite shared by the trait method and unit tests.
/// Tonic's trait signature pins the stream type to `tonic::Streaming<_>`,
/// which tests can't construct from a plain channel, so we extract the logic
/// here and let callers pass any `Stream` over `SpacedWriteRequest`.
fn required_spaced_operation<T>(operation: Option<T>) -> Result<T, Status> {
    operation.ok_or_else(|| Status::invalid_argument("spaced operation payload is required"))
}

pub(crate) async fn spaced_write_impl<S>(
    state: &SimState,
    mut stream: S,
) -> Result<Response<sbx::SpacedWriteResponse>, Status>
where
    S: tokio_stream::Stream<Item = Result<sbx::SpacedWriteRequest, Status>> + Unpin,
{
    use tokio::io::AsyncWriteExt;
    use tokio_stream::StreamExt;

    let first = match stream.next().await {
        Some(Ok(msg)) => msg,
        Some(Err(err)) => return Err(err),
        None => {
            return Err(Status::invalid_argument(
                "SpacedWrite stream closed before SpacedWriteOpen",
            ));
        }
    };
    let sandbox_id = first.sandbox_id;
    let open = match required_spaced_operation(first.operation)?.message {
        Some(sbx::spaced_write_request::Message::Open(open)) => open,
        _ => {
            return Err(Status::invalid_argument(
                "first SpacedWrite message must be SpacedWriteOpen",
            ));
        }
    };

    state
        .ensure_default_running(&sandbox_id)
        .await
        .map_err(map_state_error)?;
    let host = state
        .fs()
        .host_path(&sandbox_id, &open.path)
        .map_err(map_fs_error)?;
    if let Some(parent) = host.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| io_to_status(&parent.display().to_string(), err))?;
    }

    let mut file = if open.append {
        tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&host)
            .await
    } else {
        tokio::fs::File::create(&host).await
    }
    .map_err(|err| io_to_status(&host.display().to_string(), err))?;

    let mut bytes_written: u64 = 0;
    while let Some(item) = stream.next().await {
        let msg = item?;
        if msg.sandbox_id != sandbox_id {
            return Err(Status::invalid_argument(
                "write stream changed sandbox target",
            ));
        }
        match required_spaced_operation(msg.operation)?.message {
            Some(sbx::spaced_write_request::Message::Data(data)) => {
                file.write_all(&data)
                    .await
                    .map_err(|err| io_to_status(&host.display().to_string(), err))?;
                bytes_written += data.len() as u64;
            }
            Some(sbx::spaced_write_request::Message::Open(_)) => {
                return Err(Status::invalid_argument(
                    "SpacedWriteOpen received twice on the same stream",
                ));
            }
            None => continue,
        }
    }
    file.flush()
        .await
        .map_err(|err| io_to_status(&host.display().to_string(), err))?;

    Ok(Response::new(sbx::SpacedWriteResponse { bytes_written }))
}

pub struct SimWorkerApiService {
    state: SimState,
    teardown_suspend_journal: Arc<Mutex<HashMap<String, SimTeardownSuspend>>>,
}

#[derive(Clone)]
struct SimTeardownSuspend {
    generation: i64,
    published_artifact: Option<sbx::PublishedArtifact>,
}

impl SimWorkerApiService {
    pub fn new(state: SimState) -> Self {
        Self {
            state,
            teardown_suspend_journal: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl WorkerApi for SimWorkerApiService {
    type SpacedListStream = BoxStream<sbx::SpacedFileInfo>;
    type SpacedReadStream = BoxStream<sbx::SpacedFileChunk>;
    type SpacedWatchStream = BoxStream<sbx::SpacedFileEvent>;
    type SpacedStartProcessStream = BoxStream<sbx::SpacedProcessEvent>;
    type SpacedConnectProcessStream = BoxStream<sbx::SpacedProcessEvent>;
    type ServicePortIngressStream = BoxStream<ServicePortIngressFrame>;
    type NodeExportTemplateStream = BoxStream<tmpl::ExportTemplateChunk>;

    // ── Sandbox lifecycle ────────────────────────────────────────────────────

    async fn create_sandbox(
        &self,
        request: Request<sbx::CreateRequest>,
    ) -> Result<Response<sbx::CreateResponse>, Status> {
        let req = request.into_inner();
        let sandbox_id = req
            .sandbox_id
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| format!("sim-{}", uuid::Uuid::now_v7()));

        let template = if req.template.is_empty() {
            "seed".to_string()
        } else {
            req.template
        };
        let runtime = if req.runtime.is_empty() {
            "fc".to_string()
        } else {
            req.runtime
        };
        let user = if req.owner.is_empty() {
            "root".to_string()
        } else {
            req.owner
        };

        let mut record = crate::state::SandboxRecord::new_creating(
            sandbox_id.clone(),
            template,
            runtime,
            user,
            req.content_hash,
            req.snapshots_enabled,
        )
        .with_pool_template(req.pool_template);
        if req.autostart {
            record.status = status::RUNNING.to_string();
        }
        self.state
            .insert_new(record)
            .await
            .map_err(map_state_error)?;

        let route_identity = req
            .autostart
            .then(|| sim_direct_route_identity(self.state.node_id(), req.route_identity));
        if let Some(identity) = &route_identity {
            self.state
                .set_route_identity_provenance(
                    &sandbox_id,
                    identity.route_generation,
                    identity.execution_generation,
                    identity.activation_attempt_id.clone(),
                )
                .await
                .map_err(map_state_error)?;
        }
        Ok(Response::new(sbx::CreateResponse {
            environment: None,
            sandbox_id,
            pid: None,
            guest_ip: None,
            namespace: None,
            log_path: None,
            vsock_uds_path: None,
            boot_mode_used: req.boot_mode,
            reused_existing: false,
            route_identity,
        }))
    }

    async fn start_sandbox(
        &self,
        request: Request<sbx::StartRequest>,
    ) -> Result<Response<sbx::StartResponse>, Status> {
        let req = request.into_inner();
        if req.netns_path.is_some() {
            return Err(Status::invalid_argument(
                "StartSandbox.netns_path is internal-only; use WorkerApiInternal",
            ));
        }
        // A Start carrying the suspend checkpoint restores a locally
        // suspended sandbox, as sandbox-manager does.
        let suspend_checkpoint = req.checkpoint.as_deref() == Some("suspend");
        let from: &[&str] = if suspend_checkpoint {
            &[status::CREATING, status::STOPPED, status::SUSPENDED]
        } else {
            &[status::CREATING, status::STOPPED]
        };
        // With a durable storage URI it restores, from that snapshot, a
        // sandbox this node holds no record of: a cross-node restore.
        let cross_node_restore = suspend_checkpoint
            && req.suspend_restore_storage_uri.is_some()
            && matches!(
                self.state.get(&req.sandbox_id).await,
                Err(SimStateError::NotFound(_))
            );
        let rec = if cross_node_restore {
            self.state.ensure_default_running(&req.sandbox_id).await
        } else {
            self.state
                .transition(&req.sandbox_id, from, status::RUNNING)
                .await
        }
        .map_err(map_state_error)?;

        sim_install_activation_policy(
            &self.state,
            &req.sandbox_id,
            req.activation_auto_lifecycle.as_ref(),
        )
        .await?;

        let route_identity = sim_direct_route_identity(self.state.node_id(), req.route_identity);
        self.state
            .set_route_identity_provenance(
                &rec.sandbox_id,
                route_identity.route_generation,
                route_identity.execution_generation,
                route_identity.activation_attempt_id.clone(),
            )
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::StartResponse {
            sandbox_id: rec.sandbox_id,
            pid: 1,
            guest_ip: "127.0.0.1".into(),
            guest_gateway: "127.0.0.1".into(),
            namespace: "sim".into(),
            log_path: String::new(),
            vsock_uds_path: String::new(),
            route_identity: Some(route_identity),
        }))
    }

    async fn stop_sandbox(
        &self,
        request: Request<sbx::StopRequest>,
    ) -> Result<Response<sbx::StopResponse>, Status> {
        let req = request.into_inner();
        let ids = self
            .state
            .ids_matching_exact(&req.sandbox_id, req.template_filter.as_deref())
            .await;
        for id in &ids {
            self.state
                .set_status(id, status::STOPPED)
                .await
                .map_err(map_state_error)?;
            teardown_sandbox_execution(&self.state, id).await;
        }
        Ok(Response::new(sbx::StopResponse { stopped_ids: ids }))
    }

    async fn kill_sandbox(
        &self,
        request: Request<sbx::KillRequest>,
    ) -> Result<Response<sbx::KillResponse>, Status> {
        let req = request.into_inner();
        self.state
            .set_status(&req.sandbox_id, status::STOPPED)
            .await
            .map_err(map_state_error)?;
        teardown_sandbox_execution(&self.state, &req.sandbox_id).await;
        Ok(Response::new(sbx::KillResponse {}))
    }

    async fn delete_sandbox(
        &self,
        request: Request<sbx::DeleteRequest>,
    ) -> Result<Response<sbx::DeleteResponse>, Status> {
        let req = request.into_inner();
        let ids = self
            .state
            .ids_matching_exact(&req.sandbox_id, req.template_filter.as_deref())
            .await;
        for id in &ids {
            remove_state_record_if_present(&self.state, id).await?;
            remove_snapshot_if_present(&self.state, id).await?;
            teardown_sandbox_execution(&self.state, id).await;
            self.state
                .fs()
                .remove_sandbox_root(id)
                .await
                .map_err(map_fs_error)?;
        }
        Ok(Response::new(sbx::DeleteResponse { deleted_ids: ids }))
    }

    async fn sandbox_state(
        &self,
        request: Request<sbx::StateRequest>,
    ) -> Result<Response<sbx::StateResponse>, Status> {
        let req = request.into_inner();
        let (found, missing) = self.state.state_of(&req.sandbox_ids).await;
        Ok(Response::new(sbx::StateResponse {
            sandboxes: found.iter().map(sandbox_info_from_record).collect(),
            missing_ids: missing,
        }))
    }

    // ── Sandbox state management ─────────────────────────────────────────────

    async fn pause_sandbox(
        &self,
        request: Request<sbx::PauseRequest>,
    ) -> Result<Response<sbx::PauseResponse>, Status> {
        let req = request.into_inner();
        self.state
            .transition(&req.sandbox_id, &[status::RUNNING], status::PAUSED)
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::PauseResponse {}))
    }

    async fn resume_sandbox(
        &self,
        request: Request<sbx::ResumeRequest>,
    ) -> Result<Response<sbx::ResumeResponse>, Status> {
        let req = request.into_inner();
        if req.activation_auto_lifecycle.is_some() {
            let row = self
                .state
                .get(&req.sandbox_id)
                .await
                .map_err(map_state_error)?;
            if req.expected_execution_generation != row.execution_generation {
                return Err(Status::failed_precondition(
                    "resident execution changed before resume",
                ));
            }
        }
        let rec = self
            .state
            .transition(&req.sandbox_id, &[status::PAUSED], status::RUNNING)
            .await
            .map_err(map_state_error)?;
        sim_install_activation_policy(
            &self.state,
            &req.sandbox_id,
            req.activation_auto_lifecycle.as_ref(),
        )
        .await?;
        // Resume keeps the execution the control plane already recorded, so
        // the receipt repeats the persisted generations.
        Ok(Response::new(sbx::ResumeResponse {
            route_identity: Some(
                record_route_identity(self.state.node_id(), &rec)
                    .unwrap_or_else(|| sim_direct_route_identity(self.state.node_id(), None)),
            ),
        }))
    }

    async fn create_sandbox_snapshot(
        &self,
        request: Request<sbx::CreateSnapshotRequest>,
    ) -> Result<Response<sbx::CreateSnapshotResponse>, Status> {
        let request = request.into_inner();
        let durable_artifact = request
            .source
            .and_then(|source| source.source)
            .and_then(|source| match source {
                sbx::create_snapshot_source::Source::DurableArtifact(source) => Some(source),
                sbx::create_snapshot_source::Source::CurrentSandbox(_) => None,
            })
            .ok_or_else(|| Status::unimplemented("sim only supports durable snapshot sources"))?;
        self.state
            .get(&request.sandbox_id)
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::CreateSnapshotResponse {
            snapshot_name: String::new(),
            r#type: sbx::SnapshotType::Disk as i32,
            published_artifact: None,
            published_durable_artifact: Some(sbx::PublishedDurableArtifact {
                storage_uri: durable_artifact.destination_storage_uri,
                bytes_published: 0,
            }),
        }))
    }

    async fn suspend_sandbox(
        &self,
        request: Request<sbx::SuspendRequest>,
    ) -> Result<Response<sbx::SuspendResponse>, Status> {
        let req = request.into_inner();
        self.state
            .suspend_to_shared_dir(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::SuspendResponse {
            published_artifact: req
                .blocking
                .then(|| published_artifact(sbx::ArtifactKind::Full, sbx::ArtifactOrigin::Suspend)),
        }))
    }

    async fn prepare_teardown_suspend(
        &self,
        request: Request<sbx::PrepareTeardownSuspendRequest>,
    ) -> Result<Response<sbx::PrepareTeardownSuspendResponse>, Status> {
        let req = request.into_inner();
        if req.execution_generation <= 0 {
            return Err(Status::invalid_argument(
                "execution_generation must be positive",
            ));
        }
        let action = sbx::TeardownSuspendAction::try_from(req.action)
            .ok()
            .filter(|action| *action != sbx::TeardownSuspendAction::Unspecified)
            .ok_or_else(|| Status::invalid_argument("teardown suspend action is required"))?;
        let mut journal = self.teardown_suspend_journal.lock().await;
        if journal
            .get(&req.sandbox_id)
            .is_some_and(|entry| entry.generation > req.execution_generation)
        {
            return Ok(Response::new(sbx::PrepareTeardownSuspendResponse {
                outcome: sbx::TeardownSuspendOutcome::Superseded as i32,
                reason: Some("a newer simulated execution owns suspend".to_string()),
                published_artifact: None,
            }));
        }
        if let Some(entry) = journal
            .get(&req.sandbox_id)
            .filter(|entry| entry.generation == req.execution_generation)
        {
            let outcome = if entry.published_artifact.is_some() {
                sbx::TeardownSuspendOutcome::Durable
            } else {
                sbx::TeardownSuspendOutcome::Pending
            };
            return Ok(Response::new(sbx::PrepareTeardownSuspendResponse {
                outcome: outcome as i32,
                reason: None,
                published_artifact: entry.published_artifact.clone(),
            }));
        }
        if action == sbx::TeardownSuspendAction::ObserveOnly {
            return Ok(Response::new(sbx::PrepareTeardownSuspendResponse {
                outcome: sbx::TeardownSuspendOutcome::Pending as i32,
                reason: Some("generation suspend has not started".to_string()),
                published_artifact: None,
            }));
        }
        journal.insert(
            req.sandbox_id.clone(),
            SimTeardownSuspend {
                generation: req.execution_generation,
                published_artifact: None,
            },
        );
        drop(journal);
        let record = self
            .state
            .get(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        if matches!(record.status.as_str(), status::RUNNING | status::PAUSED) {
            match self.state.suspend_to_shared_dir(&req.sandbox_id).await {
                Ok(_) => {}
                Err(crate::state::SimStateError::InvalidTransition { .. }) => self
                    .state
                    .get(&req.sandbox_id)
                    .await
                    .map_err(map_state_error)
                    .map(|_| ())?,
                Err(error) => return Err(map_state_error(error)),
            }
        }
        let record = self
            .state
            .get(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        if record.status != status::SUSPENDED {
            return Ok(Response::new(sbx::PrepareTeardownSuspendResponse {
                outcome: sbx::TeardownSuspendOutcome::Pending as i32,
                reason: None,
                published_artifact: None,
            }));
        }
        let artifact = published_artifact(sbx::ArtifactKind::Full, sbx::ArtifactOrigin::Suspend);
        let mut journal = self.teardown_suspend_journal.lock().await;
        let Some(entry) = journal
            .get_mut(&req.sandbox_id)
            .filter(|entry| entry.generation == req.execution_generation)
        else {
            return Ok(Response::new(sbx::PrepareTeardownSuspendResponse {
                outcome: sbx::TeardownSuspendOutcome::Superseded as i32,
                reason: Some("a newer simulated execution owns suspend".to_string()),
                published_artifact: None,
            }));
        };
        entry.published_artifact = Some(artifact.clone());
        Ok(Response::new(sbx::PrepareTeardownSuspendResponse {
            outcome: sbx::TeardownSuspendOutcome::Durable as i32,
            reason: None,
            published_artifact: Some(artifact),
        }))
    }

    async fn enqueue_sandbox_artifact_backup(
        &self,
        request: Request<sbx::EnqueueArtifactBackupRequest>,
    ) -> Result<Response<sbx::EnqueueArtifactBackupResponse>, Status> {
        let request = request.into_inner();
        let local_artifact_id = match request.source {
            Some(sbx::enqueue_artifact_backup_request::Source::LocalArtifactId(value)) => value,
            _ => LocalArtifactId::new().to_string(),
        };
        Ok(Response::new(sbx::EnqueueArtifactBackupResponse {
            local_artifact_id,
            artifact_kind: request.expected_artifact_kind,
            backup_attempt_id: BackupAttemptId::new().to_string(),
        }))
    }

    // ── Sandbox operations ───────────────────────────────────────────────────

    async fn sandbox_stats(
        &self,
        _request: Request<sbx::StatsRequest>,
    ) -> Result<Response<sbx::StatsResponse>, Status> {
        unimplemented()
    }

    async fn update_sandbox(
        &self,
        request: Request<sbx::UpdateRequest>,
    ) -> Result<Response<sbx::UpdateResponse>, Status> {
        let req = request.into_inner();
        if !req.replace_process_env && !req.process_env.is_empty() {
            return Err(Status::invalid_argument(
                "replace_process_env must be true when process_env is provided",
            ));
        }
        match (&req.expected_current_owner, &req.owner) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "expected_current_owner and owner must be provided together",
                ));
            }
        }
        let process_env = if req.replace_process_env {
            Some(sanitize_process_env(req.process_env, "sim_sandbox_update"))
        } else {
            None
        };
        self.state
            .update_metadata(
                &req.sandbox_id,
                req.expected_current_owner.as_deref(),
                req.owner.as_deref(),
                process_env,
            )
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::UpdateResponse {}))
    }

    async fn set_sandbox_auto_lifecycle_policy(
        &self,
        request: Request<sbx::SetAutoLifecyclePolicyRequest>,
    ) -> Result<Response<sbx::SetAutoLifecyclePolicyResponse>, Status> {
        let req = request.into_inner();
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::SetAutoLifecyclePolicyResponse {
            effective_policy: req.policy,
            authority: req.authority,
        }))
    }

    async fn touch_sandbox_auto_lifecycle(
        &self,
        request: Request<sbx::TouchAutoLifecycleRequest>,
    ) -> Result<Response<sbx::TouchAutoLifecycleResponse>, Status> {
        let req = request.into_inner();
        let result = match self.state.get(&req.sandbox_id).await {
            Ok(record) if record.status == status::RUNNING => {
                sbx::AutoLifecycleTouchResult::AlreadyCurrent
            }
            Ok(_) | Err(crate::state::SimStateError::NotFound(_)) => {
                sbx::AutoLifecycleTouchResult::NotStarted
            }
            Err(err) => return Err(map_state_error(err)),
        };

        let mut acknowledged = false;
        if let Some(update) = req.access_idle_update {
            if result != sbx::AutoLifecycleTouchResult::AlreadyCurrent {
                return Err(Status::failed_precondition(
                    "deadline requires running execution",
                ));
            }
            if update
                .expires_at
                .as_ref()
                .is_none_or(|expires| expires.seconds <= jiff::Timestamp::now().as_second())
            {
                return Err(Status::deadline_exceeded("access deadline update expired"));
            }
            let record = self
                .state
                .get(&req.sandbox_id)
                .await
                .map_err(map_state_error)?;
            if record
                .execution_generation
                .is_some_and(|generation| generation != update.execution_generation)
            {
                return Err(Status::failed_precondition("execution changed"));
            }
            let (minimum, policy) = match update.operation {
                Some(sbx::access_idle_update::Operation::ExtendUntil(until)) => {
                    (Some(until.seconds), None)
                }
                Some(sbx::access_idle_update::Operation::ResetPolicy(policy)) => {
                    use prost::Message;
                    (None, Some(policy.encode_to_vec()))
                }
                None => return Err(Status::invalid_argument("deadline operation required")),
            };
            self.state
                .apply_access_deadline(&req.sandbox_id, minimum, policy)
                .await
                .map_err(map_state_error)?;
            acknowledged = true;
        }

        let mut response = Response::new(sbx::TouchAutoLifecycleResponse {
            result: result as i32,
        });
        if acknowledged {
            response.metadata_mut().insert(
                pplx_space_sbx_mgr_client::ACCESS_IDLE_UPDATE_ACK_METADATA,
                tonic::metadata::MetadataValue::from_static("1"),
            );
        }
        Ok(response)
    }

    async fn set_sandbox_network_policy(
        &self,
        _request: Request<sbx::SetNetworkPolicyRequest>,
    ) -> Result<Response<sbx::SetNetworkPolicyResponse>, Status> {
        unimplemented()
    }

    // ── Spaced guest operations: status/wait_ready stub healthy ──────────────

    async fn spaced_get_status(
        &self,
        request: Request<sbx::SpacedGetStatusRequest>,
    ) -> Result<Response<sbx::SpacedGetStatusResponse>, Status> {
        let req = request.into_inner();
        let rec = self
            .state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::SpacedGetStatusResponse {
            ready: rec.status == status::RUNNING,
            guest_uptime: None,
            host_version: Some(sbx::SpacedVersionInfo {
                version: "sim-0.1.0".into(),
                compatibility_family: "sim".into(),
            }),
            guest_version: Some(sbx::SpacedVersionInfo {
                version: "sim-0.1.0".into(),
                compatibility_family: "sim".into(),
            }),
        }))
    }

    async fn spaced_wait_ready(
        &self,
        request: Request<sbx::SpacedWaitReadyRequest>,
    ) -> Result<Response<sbx::SpacedWaitReadyResponse>, Status> {
        let route_token = request
            .metadata()
            .get(pplx_space_common_svc::space_api_route::SPACE_API_ROUTE_ED25519_TOKEN_METADATA_KEY)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        self.state.record_wait_ready_route_token(route_token).await;
        let req = request.into_inner();
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::SpacedWaitReadyResponse {
            guest_uptime: None,
        }))
    }

    // ── Spaced file operations: backed by a per-sandbox temp dir ─────────────

    async fn spaced_stat(
        &self,
        request: Request<sbx::SpacedStatRequest>,
    ) -> Result<Response<sbx::SpacedStatResponse>, Status> {
        let req = request.into_inner();
        let operation = required_spaced_operation(req.operation)?;
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        let path = self
            .state
            .fs()
            .host_path(&req.sandbox_id, &operation.path)
            .map_err(map_fs_error)?;
        let meta = tokio::fs::symlink_metadata(&path)
            .await
            .map_err(|err| io_to_status(&path.display().to_string(), err))?;
        Ok(Response::new(sbx::SpacedStatResponse {
            info: Some(file_info_from_metadata(&operation.path, &meta).await),
        }))
    }

    async fn spaced_list(
        &self,
        request: Request<sbx::SpacedListRequest>,
    ) -> Result<Response<Self::SpacedListStream>, Status> {
        let req = request.into_inner();
        let operation = required_spaced_operation(req.operation)?;
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        let root = self
            .state
            .fs()
            .host_path(&req.sandbox_id, &operation.path)
            .map_err(map_fs_error)?;
        let entries =
            collect_dir_entries(&operation.path, &root, operation.depth.unwrap_or(1).max(1))
                .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            for info in entries {
                if tx.send(Ok(info)).await.is_err() {
                    return;
                }
            }
        });
        let stream: Self::SpacedListStream =
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn spaced_read(
        &self,
        request: Request<sbx::SpacedReadRequest>,
    ) -> Result<Response<Self::SpacedReadStream>, Status> {
        let req = request.into_inner();
        let operation = required_spaced_operation(req.operation)?;
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        let host = self
            .state
            .fs()
            .host_path(&req.sandbox_id, &operation.path)
            .map_err(map_fs_error)?;
        let data = crate::fs::read_range(&host, operation.offset, operation.length)
            .await
            .map_err(map_fs_error)?;

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            // Chunk into 64 KiB pieces so the receiver sees realistic stream
            // semantics instead of one giant message.
            for chunk in data.chunks(64 * 1024) {
                if tx
                    .send(Ok(sbx::SpacedFileChunk {
                        data: chunk.to_vec().into(),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        let stream: Self::SpacedReadStream =
            Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn spaced_write(
        &self,
        request: Request<tonic::Streaming<sbx::SpacedWriteRequest>>,
    ) -> Result<Response<sbx::SpacedWriteResponse>, Status> {
        let stream = request.into_inner();
        spaced_write_impl(&self.state, stream).await
    }

    async fn spaced_write_batch(
        &self,
        request: Request<sbx::SpacedWriteBatchRequest>,
    ) -> Result<Response<sbx::SpacedWriteBatchResponse>, Status> {
        use tokio::io::AsyncWriteExt;

        let req = request.into_inner();
        let operation = required_spaced_operation(req.operation)?;
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;

        let mut files_written: u64 = 0;
        for entry in &operation.entries {
            let host = self
                .state
                .fs()
                .host_path(&req.sandbox_id, &entry.path)
                .map_err(map_fs_error)?;
            if let Some(parent) = host.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|err| io_to_status(&parent.display().to_string(), err))?;
            }
            let mut file = if entry.append {
                tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&host)
                    .await
            } else {
                tokio::fs::File::create(&host).await
            }
            .map_err(|err| io_to_status(&host.display().to_string(), err))?;
            file.write_all(&entry.data)
                .await
                .map_err(|err| io_to_status(&host.display().to_string(), err))?;
            file.flush()
                .await
                .map_err(|err| io_to_status(&host.display().to_string(), err))?;
            files_written += 1;
        }

        Ok(Response::new(sbx::SpacedWriteBatchResponse {
            files_written,
        }))
    }

    async fn spaced_mkdir(
        &self,
        request: Request<sbx::SpacedMkdirRequest>,
    ) -> Result<Response<sbx::SpacedMkdirResponse>, Status> {
        let req = request.into_inner();
        let operation = required_spaced_operation(req.operation)?;
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        let host = self
            .state
            .fs()
            .host_path(&req.sandbox_id, &operation.path)
            .map_err(map_fs_error)?;
        let result = if operation.parents {
            tokio::fs::create_dir_all(&host).await
        } else {
            tokio::fs::create_dir(&host).await
        };
        match result {
            Ok(()) => Ok(Response::new(sbx::SpacedMkdirResponse {})),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && operation.exist_ok => {
                Ok(Response::new(sbx::SpacedMkdirResponse {}))
            }
            Err(err) => Err(io_to_status(&host.display().to_string(), err)),
        }
    }

    async fn spaced_remove(
        &self,
        request: Request<sbx::SpacedRemoveRequest>,
    ) -> Result<Response<sbx::SpacedRemoveResponse>, Status> {
        let req = request.into_inner();
        let operation = required_spaced_operation(req.operation)?;
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        let host = self
            .state
            .fs()
            .host_path(&req.sandbox_id, &operation.path)
            .map_err(map_fs_error)?;
        let meta = match tokio::fs::symlink_metadata(&host).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && operation.missing_ok => {
                return Ok(Response::new(sbx::SpacedRemoveResponse {}));
            }
            Err(err) => return Err(io_to_status(&host.display().to_string(), err)),
        };
        let result = if meta.is_dir() {
            if operation.recursive {
                tokio::fs::remove_dir_all(&host).await
            } else {
                tokio::fs::remove_dir(&host).await
            }
        } else {
            tokio::fs::remove_file(&host).await
        };
        match result {
            Ok(()) => Ok(Response::new(sbx::SpacedRemoveResponse {})),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && operation.missing_ok => {
                Ok(Response::new(sbx::SpacedRemoveResponse {}))
            }
            Err(err) => Err(io_to_status(&host.display().to_string(), err)),
        }
    }

    async fn spaced_rename(
        &self,
        request: Request<sbx::SpacedRenameRequest>,
    ) -> Result<Response<sbx::SpacedRenameResponse>, Status> {
        let req = request.into_inner();
        let operation = required_spaced_operation(req.operation)?;
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;
        let from = self
            .state
            .fs()
            .host_path(&req.sandbox_id, &operation.old_path)
            .map_err(map_fs_error)?;
        let to = self
            .state
            .fs()
            .host_path(&req.sandbox_id, &operation.new_path)
            .map_err(map_fs_error)?;
        if let Some(parent) = to.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| io_to_status(&parent.display().to_string(), err))?;
        }
        tokio::fs::rename(&from, &to)
            .await
            .map_err(|err| io_to_status(&from.display().to_string(), err))?;
        Ok(Response::new(sbx::SpacedRenameResponse {}))
    }

    async fn spaced_normalize_watch_root(
        &self,
        _request: Request<sbx::SpacedNormalizeWatchRootRequest>,
    ) -> Result<Response<sbx::SpacedNormalizeWatchRootResponse>, Status> {
        unimplemented()
    }

    async fn spaced_watch(
        &self,
        _request: Request<sbx::SpacedWatchRequest>,
    ) -> Result<Response<Self::SpacedWatchStream>, Status> {
        unimplemented()
    }

    async fn spaced_create_watcher(
        &self,
        _request: Request<sbx::SpacedCreateWatcherRequest>,
    ) -> Result<Response<sbx::SpacedCreateWatcherResponse>, Status> {
        unimplemented()
    }

    async fn spaced_get_watcher_events(
        &self,
        _request: Request<sbx::SpacedGetWatcherEventsRequest>,
    ) -> Result<Response<sbx::SpacedGetWatcherEventsResponse>, Status> {
        unimplemented()
    }

    async fn spaced_remove_watcher(
        &self,
        _request: Request<sbx::SpacedRemoveWatcherRequest>,
    ) -> Result<Response<sbx::SpacedRemoveWatcherResponse>, Status> {
        unimplemented()
    }

    async fn spaced_enable_file_changes(
        &self,
        _request: Request<sbx::SpacedEnableFileChangesRequest>,
    ) -> Result<Response<sbx::SpacedEnableFileChangesResponse>, Status> {
        unimplemented()
    }

    async fn spaced_disable_file_changes(
        &self,
        _request: Request<sbx::SpacedDisableFileChangesRequest>,
    ) -> Result<Response<sbx::SpacedDisableFileChangesResponse>, Status> {
        unimplemented()
    }

    async fn spaced_get_modified_files(
        &self,
        _request: Request<sbx::SpacedGetModifiedFilesRequest>,
    ) -> Result<Response<sbx::SpacedGetModifiedFilesResponse>, Status> {
        unimplemented()
    }

    async fn spaced_start_process(
        &self,
        request: Request<sbx::SpacedStartProcessRequest>,
    ) -> Result<Response<Self::SpacedStartProcessStream>, Status> {
        let req = request.into_inner();
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;

        let operation = required_operation(req.operation)?;
        if operation.command.is_empty() {
            return Err(Status::invalid_argument("command is required"));
        }
        let spec = crate::processes::SimProcessSpec {
            command: operation.command,
            cwd: operation.cwd,
            env: operation.env,
            user: operation.user,
            tag: operation.tag,
            pty: operation.pty.is_some(),
            background: operation.background,
        };
        let stream = self.state.processes().start(&req.sandbox_id, spec).await;
        Ok(Response::new(stream))
    }

    async fn spaced_connect_process(
        &self,
        request: Request<sbx::SpacedConnectProcessRequest>,
    ) -> Result<Response<Self::SpacedConnectProcessStream>, Status> {
        let req = request.into_inner();
        self.state
            .ensure_default_running(&req.sandbox_id)
            .await
            .map_err(map_state_error)?;

        let operation = required_operation(req.operation)?;
        required_process_id(&operation.process_id)?;
        let stream = self
            .state
            .processes()
            .connect(
                &req.sandbox_id,
                &operation.process_id,
                operation.output_cursor,
            )
            .await?;
        Ok(Response::new(stream))
    }

    async fn spaced_list_processes(
        &self,
        request: Request<sbx::SpacedListProcessesRequest>,
    ) -> Result<Response<sbx::SpacedListProcessesResponse>, Status> {
        let req = request.into_inner();
        let operation = required_operation(req.operation)?;
        let processes = self
            .state
            .processes()
            .list(
                &req.sandbox_id,
                operation.include_exited,
                operation.process_id.as_deref(),
            )
            .await;
        Ok(Response::new(sbx::SpacedListProcessesResponse {
            processes,
        }))
    }

    async fn spaced_send_signal(
        &self,
        request: Request<sbx::SpacedSendSignalRequest>,
    ) -> Result<Response<sbx::SpacedSendSignalResponse>, Status> {
        let req = request.into_inner();
        let operation = required_operation(req.operation)?;
        required_process_id(&operation.process_id)?;
        if operation.signal <= 0 {
            return Err(Status::invalid_argument("signal must be greater than 0"));
        }
        self.state
            .processes()
            .signal(&req.sandbox_id, &operation.process_id, operation.signal)
            .await?;
        Ok(Response::new(sbx::SpacedSendSignalResponse {}))
    }

    async fn spaced_send_input(
        &self,
        _request: Request<sbx::SpacedSendInputRequest>,
    ) -> Result<Response<sbx::SpacedSendInputResponse>, Status> {
        unimplemented()
    }

    async fn spaced_close_stdin(
        &self,
        _request: Request<sbx::SpacedCloseStdinRequest>,
    ) -> Result<Response<sbx::SpacedCloseStdinResponse>, Status> {
        unimplemented()
    }

    async fn spaced_resize_pty(
        &self,
        _request: Request<sbx::SpacedResizePtyRequest>,
    ) -> Result<Response<sbx::SpacedResizePtyResponse>, Status> {
        unimplemented()
    }

    async fn spaced_kill_process(
        &self,
        request: Request<sbx::SpacedKillProcessRequest>,
    ) -> Result<Response<sbx::SpacedKillProcessResponse>, Status> {
        let req = request.into_inner();
        let operation = required_operation(req.operation)?;
        required_process_id(&operation.process_id)?;
        self.state
            .processes()
            .kill(&req.sandbox_id, &operation.process_id)
            .await?;
        Ok(Response::new(sbx::SpacedKillProcessResponse {}))
    }

    async fn spaced_open_loopback_bridge(
        &self,
        _request: Request<sbx::SpacedOpenLoopbackBridgeRequest>,
    ) -> Result<Response<sbx::SpacedOpenLoopbackBridgeResponse>, Status> {
        Ok(Response::new(sbx::SpacedOpenLoopbackBridgeResponse {
            status: sbx::SpacedLoopbackBridgeStatus::Direct as i32,
            detail: String::new(),
            service_generation: "sim-generation".to_string(),
        }))
    }

    async fn spaced_close_loopback_bridge(
        &self,
        _request: Request<sbx::SpacedCloseLoopbackBridgeRequest>,
    ) -> Result<Response<sbx::SpacedCloseLoopbackBridgeResponse>, Status> {
        unimplemented()
    }

    async fn service_port_ingress(
        &self,
        request: Request<tonic::Streaming<ServicePortIngressFrame>>,
    ) -> Result<Response<Self::ServicePortIngressStream>, Status> {
        let mut stream = request.into_inner();
        let first = stream.message().await?.ok_or_else(|| {
            Status::invalid_argument("service port ingress start frame is required")
        })?;
        let start = match first.message {
            Some(service_port_ingress_frame::Message::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "service port ingress first frame must be start",
                ));
            }
        };
        self.state
            .ensure_default_running(&start.sandbox_id)
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(Box::pin(stream)))
    }

    // ── Sandbox snapshots (templates have their own pull/export) ─────────────

    async fn list_snapshots(
        &self,
        _request: Request<sbx::ListSnapshotsRequest>,
    ) -> Result<Response<sbx::ListSnapshotsResponse>, Status> {
        unimplemented()
    }

    async fn rollback_sandbox(
        &self,
        _request: Request<sbx::RollbackRequest>,
    ) -> Result<Response<sbx::RollbackResponse>, Status> {
        unimplemented()
    }

    // ── Templates (template-manager pass-throughs) ───────────────────────────

    async fn delete_template(
        &self,
        _request: Request<tmpl::DeleteTemplateRequest>,
    ) -> Result<Response<tmpl::DeleteTemplateResponse>, Status> {
        unimplemented()
    }

    async fn delete_template_build(
        &self,
        _request: Request<tmpl::DeleteTemplateBuildRequest>,
    ) -> Result<Response<tmpl::DeleteTemplateBuildResponse>, Status> {
        unimplemented()
    }

    // ── Network (no-op; preserve transitions reconcilers expect) ─────────────

    async fn expose_port(
        &self,
        _request: Request<pplx_space_node_mgr_client::proto::ExposePortRequest>,
    ) -> Result<Response<pplx_space_node_mgr_client::proto::ExposePortResponse>, Status> {
        Ok(Response::new(
            pplx_space_node_mgr_client::proto::ExposePortResponse {},
        ))
    }

    async fn allow_port(
        &self,
        _request: Request<pplx_space_node_mgr_client::proto::AllowPortRequest>,
    ) -> Result<Response<pplx_space_node_mgr_client::proto::AllowPortResponse>, Status> {
        Ok(Response::new(
            pplx_space_node_mgr_client::proto::AllowPortResponse {},
        ))
    }

    // ── Custom orchestration ─────────────────────────────────────────────────

    async fn trigger_state_reconciliation(
        &self,
        _request: Request<TriggerStateReconciliationRequest>,
    ) -> Result<Response<TriggerStateReconciliationResponse>, Status> {
        Ok(Response::new(TriggerStateReconciliationResponse {}))
    }

    // ── Template transfer (seeded inventory only) ────────────────────────────

    async fn pull_template(
        &self,
        _request: Request<PullTemplateRequest>,
    ) -> Result<Response<PullTemplateResponse>, Status> {
        unimplemented()
    }

    async fn node_export_template(
        &self,
        _request: Request<tmpl::ExportTemplateRequest>,
    ) -> Result<Response<Self::NodeExportTemplateStream>, Status> {
        unimplemented()
    }

    async fn list_node_templates(
        &self,
        _request: Request<ListNodeTemplatesRequest>,
    ) -> Result<Response<ListNodeTemplatesResponse>, Status> {
        Ok(Response::new(ListNodeTemplatesResponse {
            builds: default_template_builds(),
        }))
    }
}

async fn sim_install_activation_policy(
    state: &SimState,
    sandbox_id: &str,
    policy: Option<&sbx::AutoLifecyclePolicy>,
) -> Result<(), Status> {
    use prost::Message;
    if let Some(policy) = policy {
        pplx_space_common_core::types::AutoLifecyclePolicy::try_from(policy)
            .map_err(Status::from)?;
        let encoded = policy.encode_to_vec();
        let record = state.get(sandbox_id).await.map_err(map_state_error)?;
        if record.auto_lifecycle_authority != pplx_space_common_core::types::PolicyAuthority::Local
            && record.access_idle_policy != encoded
        {
            state
                .apply_access_deadline(sandbox_id, None, Some(encoded))
                .await
                .map_err(map_state_error)?;
        }
    }
    Ok(())
}

/// Implementation of `WorkerApiInternal.WakeSandbox`. Picks the appropriate
/// WakeTransition based on the current in-memory state.
/// `requested_identity` is the reserved identity the control plane passes in
/// `WakeStartContext.route_identity` when a direct wake hydrates a new
/// execution. A start or restore prefers it over the record's last identity
/// and records it, so the receipt carries the reserved generation rather than
/// a stale or default one; resume and no-op wakes ignore it, as the recorded
/// execution stays in place.
async fn sim_wake(
    state: &SimState,
    sandbox_id: &str,
    requested_identity: Option<sbx::SpaceApiRouteIdentity>,
    implicit_wake_source: i32,
    activation_policy: Option<sbx::AutoLifecyclePolicy>,
    expected_execution_generation: Option<i64>,
) -> Result<Response<sbx::WakeResponse>, Status> {
    if expected_execution_generation.is_some() && activation_policy.is_none() {
        return Err(Status::invalid_argument(
            "wake execution generation requires a policy",
        ));
    }
    if expected_execution_generation.is_some_and(|generation| generation <= 0) {
        return Err(Status::invalid_argument("invalid execution generation"));
    }
    let rec = state.get(sandbox_id).await;
    // Direct fresh activation requires new control-plane authority. A resident
    // observation is only a resume fence and cannot authorize reusing its ID.
    if let Ok(record) = &rec
        && matches!(record.status.as_str(), status::STOPPED | status::SUSPENDED)
        && record.route_substrate_kind == Some(pplx_space_common_core::types::NodeManagedBy::Space)
        && let Some(retained) = record.execution_generation
    {
        let Some(identity) = &requested_identity else {
            return Err(
                pplx_space_sbx_mgr_client::authoritative_route_identity_required_status(
                    "fresh activation requires authoritative route identity",
                ),
            );
        };
        if identity
            .execution_generation
            .is_none_or(|generation| generation <= retained)
        {
            return Err(Status::aborted("activation generation is already retired"));
        }
    }
    let source = sbx::AutoLifecycleWakeSource::try_from(implicit_wake_source)
        .map_err(|_| Status::invalid_argument("invalid implicit wake source"))?;
    if source != sbx::AutoLifecycleWakeSource::Unspecified {
        let record = rec
            .as_ref()
            .map_err(|_| Status::not_found("sandbox not resident"))?;
        if record.status != status::RUNNING {
            use prost::Message;
            let policy = match activation_policy.as_ref() {
                Some(policy)
                    if record.auto_lifecycle_authority
                        != pplx_space_common_core::types::PolicyAuthority::Local =>
                {
                    policy.clone()
                }
                _ => sbx::AutoLifecyclePolicy::decode(record.access_idle_policy.as_slice())
                    .map_err(|_| Status::internal("invalid simulated lifecycle policy"))?,
            };
            let sources = policy.wake.map(|wake| wake.on).unwrap_or_default();
            let allowed = sources.contains(&(sbx::AutoLifecycleWakeSource::Access as i32))
                || source == sbx::AutoLifecycleWakeSource::Command
                    && sources.contains(&(sbx::AutoLifecycleWakeSource::Command as i32));
            if !allowed {
                return Err(Status::failed_precondition("automatic wake is disabled"));
            }
        }
    }
    let (rec, transition) = match rec {
        Ok(rec) => match rec.status.as_str() {
            status::RUNNING => (rec, sbx::WakeTransition::Noop),
            status::PAUSED => {
                if activation_policy.is_some()
                    && expected_execution_generation != rec.execution_generation
                {
                    return Err(Status::failed_precondition(
                        "resident execution changed before resume",
                    ));
                }
                let r = state
                    .transition(sandbox_id, &[status::PAUSED], status::RUNNING)
                    .await
                    .map_err(map_state_error)?;
                (r, sbx::WakeTransition::Resume)
            }
            status::STOPPED | status::CREATING => {
                let r = state
                    .transition(
                        sandbox_id,
                        &[status::STOPPED, status::CREATING],
                        status::RUNNING,
                    )
                    .await
                    .map_err(map_state_error)?;
                (r, sbx::WakeTransition::Start)
            }
            status::SUSPENDED => {
                let r = state
                    .restore_from_shared_dir(sandbox_id)
                    .await
                    .map_err(map_state_error)?;
                (r, sbx::WakeTransition::Restore)
            }
            _ => (rec, sbx::WakeTransition::Noop),
        },
        Err(SimStateError::NotFound(_)) => {
            // No in-memory record: try restore from the shared dir first; if
            // there's no snapshot either (K8s substrate's "create" path skips
            // CreateSandbox and the sim per-sandbox pod is fresh), fall
            // through to a default-running auto-create so the api-server can
            // proceed.
            match state.restore_from_shared_dir(sandbox_id).await {
                Ok(rec) => (rec, sbx::WakeTransition::Restore),
                Err(SimStateError::SnapshotMissing { .. }) => {
                    let rec = state
                        .ensure_default_running(sandbox_id)
                        .await
                        .map_err(map_state_error)?;
                    (rec, sbx::WakeTransition::Start)
                }
                Err(err) => return Err(map_state_error(err)),
            }
        }
        Err(err) => return Err(map_state_error(err)),
    };
    if transition != sbx::WakeTransition::Noop {
        sim_install_activation_policy(state, sandbox_id, activation_policy.as_ref()).await?;
    }
    // Authoritative Wake adopts and acknowledges the requested identity even
    // when the resident lifecycle needs no transition. Without a request,
    // retain the legacy resident response: the record does not retain enough
    // runtime-owned Kubernetes dimensions to reconstruct a complete receipt.
    let route_identity = match transition {
        sbx::WakeTransition::Noop | sbx::WakeTransition::Resume | sbx::WakeTransition::Wait
            if requested_identity.is_none() =>
        {
            None
        }
        _ => {
            let identity = match requested_identity {
                Some(requested) => sim_direct_route_identity(state.node_id(), Some(requested)),
                None => record_route_identity(state.node_id(), &rec)
                    .unwrap_or_else(|| sim_direct_route_identity(state.node_id(), None)),
            };
            state
                .set_route_identity_provenance(
                    &rec.sandbox_id,
                    identity.route_generation,
                    identity.execution_generation,
                    identity.activation_attempt_id.clone(),
                )
                .await
                .map_err(map_state_error)?;
            Some(identity)
        }
    };
    Ok(Response::new(sbx::WakeResponse {
        sandbox_id: rec.sandbox_id,
        transition: transition.into(),
        route_identity,
    }))
}

/// The identity the simulator last reported for `rec`, if it has one.
fn record_route_identity(node_id: &str, rec: &SandboxRecord) -> Option<sbx::SpaceApiRouteIdentity> {
    if rec.route_generation.is_none() && rec.execution_generation.is_none() {
        return None;
    }
    Some(sim_direct_route_identity(
        node_id,
        Some(sbx::SpaceApiRouteIdentity {
            route_generation: rec.route_generation,
            execution_generation: rec.execution_generation,
            activation_attempt_id: rec.activation_attempt_id.clone(),
            ..Default::default()
        }),
    ))
}

fn sim_direct_route_identity(
    node_id: &str,
    requested_identity: Option<sbx::SpaceApiRouteIdentity>,
) -> sbx::SpaceApiRouteIdentity {
    let requested_identity = requested_identity.unwrap_or_default();
    let kubernetes_identity =
        requested_identity.substrate_kind == sbx::SandboxSubstrateKind::Kubernetes as i32;
    let route_generation = requested_identity
        .route_generation
        .or_else(|| {
            requested_identity
                .execution_generation
                .and_then(|generation| u64::try_from(generation).ok())
        })
        .unwrap_or(1);
    let execution_generation = requested_identity
        .execution_generation
        .or_else(|| i64::try_from(route_generation).ok())
        .unwrap_or(1);
    sbx::SpaceApiRouteIdentity {
        sandbox_id: requested_identity.sandbox_id,
        route_generation: Some(route_generation),
        current_node_id: if kubernetes_identity {
            requested_identity.current_node_id.or_else(|| {
                Some(
                    pplx_space_common_svc::types::k8s::space_k8s_node_id(node_id, "worker")
                        .to_string(),
                )
            })
        } else {
            Some(node_id.to_string())
        },
        execution_generation: Some(execution_generation),
        pod_uid: kubernetes_identity
            .then_some(requested_identity.pod_uid)
            .flatten(),
        activation_attempt_id: requested_identity
            .activation_attempt_id
            .or_else(|| Some(uuid::Uuid::now_v7().to_string())),
        substrate_kind: if kubernetes_identity {
            sbx::SandboxSubstrateKind::Kubernetes.into()
        } else {
            sbx::SandboxSubstrateKind::Direct.into()
        },
    }
}

pub struct SimWorkerApiInternalService {
    state: SimState,
}

impl SimWorkerApiInternalService {
    pub fn new(state: SimState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl WorkerApiInternal for SimWorkerApiInternalService {
    async fn adopt_pool_sandbox(
        &self,
        request: Request<sbx::AdoptPoolSandboxRequest>,
    ) -> Result<Response<sbx::AdoptPoolSandboxResponse>, Status> {
        let req = request.into_inner();
        let Some(record) = self
            .state
            .adopt_pool_sandbox(
                &req.template,
                req.content_hash.as_deref(),
                &req.owner,
                req.env.clone().into_iter().collect(),
            )
            .await
        else {
            return Err(Status::resource_exhausted(format!(
                "no paused pool sandbox available for template {}",
                req.template
            )));
        };
        let identity = sim_direct_route_identity(self.state.node_id(), req.route_identity);
        self.state
            .set_route_identity_provenance(
                &record.sandbox_id,
                identity.route_generation,
                identity.execution_generation,
                identity.activation_attempt_id.clone(),
            )
            .await
            .map_err(map_state_error)?;
        Ok(Response::new(sbx::AdoptPoolSandboxResponse {
            sandbox_id: record.sandbox_id,
            route_identity: Some(identity),
        }))
    }

    async fn wake_sandbox(
        &self,
        request: Request<sbx::WakeRequest>,
    ) -> Result<Response<sbx::WakeResponse>, Status> {
        let req = request.into_inner();
        let activation_policy = req
            .start_context
            .as_ref()
            .and_then(|context| context.activation_auto_lifecycle.clone());
        let expected_execution_generation = req
            .start_context
            .as_ref()
            .and_then(|context| context.expected_execution_generation);
        let requested_identity = req.start_context.and_then(|context| context.route_identity);
        sim_wake(
            &self.state,
            &req.sandbox_id,
            requested_identity,
            req.implicit_wake_source,
            activation_policy,
            expected_execution_generation,
        )
        .await
    }
}

#[cfg(test)]
mod tests;

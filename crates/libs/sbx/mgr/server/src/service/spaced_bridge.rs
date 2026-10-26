use super::activation_support::{GuestActivityKind, spaced_host_socket_path};
use super::*;
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};

pub(super) async fn launch_spaced_host_via_node(
    node_client: &mut NodeManagerGrpcClient,
    execution_fence: pplx_space_node_mgr_client::execution_fence::ExecutionFence,
    sandbox_id: &str,
    uid: u32,
    vsock_uds_path: &str,
    host_control_socket_path: &Path,
    boot_type: &'static str,
) -> Result<pplx_space_node_mgr_client::proto::LaunchResponse, Status> {
    let host_control_socket_path = host_control_socket_path
        .to_str()
        .ok_or_else(|| Status::internal("host control socket path is not valid UTF-8"))?
        .to_string();

    let result = node_client
        .launch(LaunchRequest {
            execution_fence: Some(execution_fence.into()),
            sandbox_id: sandbox_id.to_string(),
            program: pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN.to_string(),
            args: vec![
                "--socket".to_string(),
                spaced_host_socket_path(sandbox_id),
                "--sandbox-id".to_string(),
                sandbox_id.to_string(),
                "--vsock-uds".to_string(),
                vsock_uds_path.to_string(),
                "--host-control-socket".to_string(),
                host_control_socket_path,
                "--boot-type".to_string(),
                boot_type.to_string(),
            ],
            uid,
            gid: uid,
            env: crate::sidecar_env::telemetry_env_for_service("space-spaced-host"),
        })
        .instrument(tracing::info_span!(
            "sbxmgr.start.node.launch_spaced_host",
            sandbox_id,
            boot_type
        ))
        .await
        .map(Response::into_inner);

    match result {
        Ok(response) => {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::GuestBridgeReady,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .boot_type(boot_type)
                    .pid(response.pid.to_string())
                    .program(pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN),
            );
            Ok(response)
        }
        Err(error) => {
            emit_sandbox_event(
                sandbox_id,
                SandboxEventType::GuestBridgeFailed,
                SandboxEventSource::SbxManager,
                SandboxEventDetails::default()
                    .boot_type(boot_type)
                    .program(pplx_space_common_core::host::linux::paths::SPACED_HOST_SVC_BIN)
                    .error(error.to_string()),
            );
            Err(Status::internal(format!(
                "spaced-host launch failed: {error}"
            )))
        }
    }
}

pub(super) fn cleanup_restored_snapshot_dir_async(
    sandbox_id: &str,
    snapshot_dir: &str,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) {
    // The consumed snapshot moves into the staging sweep's lane first, so a
    // deferred capture's read-only subvolumes, which this service cannot
    // delete, have an owner even if nothing below runs; then it is reclaimed
    // at once, locally or through node-manager.
    let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{sandbox_id}"));
    let lane =
        super::suspend_snapshot_publication::suspend_snapshot_staging_parent_dir(&sandbox_dir);
    let garbage = super::suspend_snapshot_publication::garbage_suspend_snapshot_dir(&sandbox_dir);
    match std::fs::create_dir_all(&lane).and_then(|()| std::fs::rename(snapshot_dir, &garbage)) {
        Ok(()) => {
            drop(super::recovery::staging_cleanup::reclaim_garbage_now(
                sandbox_id.to_string(),
                garbage,
                node_manager.clone(),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(
            sandbox_id,
            path = snapshot_dir,
            %error,
            "failed to move restored snapshot directory aside for cleanup"
        ),
    }
}

impl SandboxManagerService {
    pub(super) async fn connect_spaced_host(
        &self,
        sandbox: &crate::db::SandboxRow,
    ) -> Result<pplx_space_spaced_host_client::SpacedHostClient, Status> {
        self.connect_spaced_host_with_policy(sandbox, SpacedEndpointProofPolicy::Required)
            .await
    }

    pub(super) async fn connect_spaced_host_with_policy(
        &self,
        sandbox: &crate::db::SandboxRow,
        policy: SpacedEndpointProofPolicy,
    ) -> Result<pplx_space_spaced_host_client::SpacedHostClient, Status> {
        super::guest_image::require_agent(sandbox)?;
        use pplx_space_metrics::sandbox::worker::{
            SpacedEndpointAttestationResult, spaced_endpoint_attestation,
        };

        let requirement = activation_endpoint_proof_requirement(
            &sandbox.space_api_route_identity,
            sandbox.pool_template.as_deref(),
        );
        if let (
            SpacedEndpointProofPolicy::AllowIdentityFreePoolDonor,
            ActivationEndpointProofRequirement::DeferredUntilPoolAdoption,
        ) = (policy, requirement)
        {
            spaced_endpoint_attestation(
                SpacedEndpointAttestationResult::DeferredIdentityFreePoolDonor,
            );
            return connect_spaced_host_live(sandbox).await;
        }

        let runtime = spaced_endpoint_runtime_identity(sandbox)?;
        let mut node_client = self.node_manager.lazy();
        let proof = match sandbox.spaced_endpoint_proof.clone() {
            Some(proof) => proof,
            None => {
                let captured = capture_spaced_endpoint_proof(&mut node_client, runtime.clone())
                    .await
                    .inspect_err(record_endpoint_attestation_error)?;
                if policy == SpacedEndpointProofPolicy::ValidateOnly {
                    captured
                } else {
                    let persisted = {
                        let db = self
                            .db_conn()
                            .inspect_err(record_endpoint_attestation_error)?;
                        crate::db::persist_spaced_endpoint_proof_if_missing(
                            &db, &runtime, &captured,
                        )
                        .map_err(|error| {
                            Status::internal(format!("persist spaced endpoint proof: {error}"))
                        })
                        .inspect_err(record_endpoint_attestation_error)?
                    };
                    if persisted {
                        captured
                    } else {
                        let db = self
                            .db_conn()
                            .inspect_err(record_endpoint_attestation_error)?;
                        let winning_proof = crate::db::read::sandbox(&db, &sandbox.id)
                            .map_err(|error| {
                                Status::internal(format!("reload spaced endpoint proof: {error}"))
                            })
                            .inspect_err(record_endpoint_attestation_error)?
                            .and_then(|row| row.spaced_endpoint_proof)
                            .ok_or_else(endpoint_proof_cas_lost_status)
                            .inspect_err(record_endpoint_attestation_error)?;
                        resolve_endpoint_proof_cas_winner(&captured, winning_proof)
                            .inspect_err(record_endpoint_attestation_error)?
                    }
                }
            }
        };
        let client = connect_spaced_host_live(sandbox).await?;
        if let Err(error) = validate_spaced_endpoint_proof(&mut node_client, &proof, &runtime).await
        {
            record_endpoint_attestation_error(&error);
            return Err(error);
        }
        pplx_space_metrics::sandbox::worker::spaced_endpoint_attestation(
            pplx_space_metrics::sandbox::worker::SpacedEndpointAttestationResult::Ok,
        );
        Ok(client)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SpacedEndpointProofPolicy {
    Required,
    /// Attest the live endpoint without changing a recovery observation's
    /// durable witness when an older row needs a proof captured.
    ValidateOnly,
    AllowIdentityFreePoolDonor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivationEndpointProofRequirement {
    Required,
    DeferredUntilPoolAdoption,
}

pub(super) fn activation_endpoint_proof_requirement(
    route_identity: &pplx_space_common_core::types::SpaceApiRouteIdentity,
    pool_template: Option<&str>,
) -> ActivationEndpointProofRequirement {
    if pool_template.is_some() && route_identity.is_absent() {
        ActivationEndpointProofRequirement::DeferredUntilPoolAdoption
    } else {
        ActivationEndpointProofRequirement::Required
    }
}

fn spaced_endpoint_runtime_identity(
    sandbox: &crate::db::SandboxRow,
) -> Result<crate::endpoint_proof::SpacedEndpointRuntimeIdentity, Status> {
    endpoint_runtime_identity_for_activation(
        &sandbox.id,
        &sandbox.space_api_route_identity,
        sandbox
            .spaced_host_pid
            .ok_or_else(|| Status::failed_precondition("spaced-host pid is not recorded"))?,
        sandbox
            .pid
            .ok_or_else(|| Status::failed_precondition("sandbox runtime pid is not recorded"))?,
        sandbox
            .uid
            .ok_or_else(|| Status::failed_precondition("sandbox uid is not recorded"))?,
        sandbox
            .guest_cid
            .ok_or_else(|| Status::failed_precondition("guest CID is not recorded"))?,
        sandbox
            .vsock_uds_path
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("vsock endpoint path is not recorded"))?,
    )
}

pub(super) fn endpoint_runtime_identity_for_activation(
    sandbox_id: &str,
    route_identity: &pplx_space_common_core::types::SpaceApiRouteIdentity,
    spaced_host_pid: u32,
    runtime_pid: u32,
    sandbox_uid: u32,
    guest_cid: u32,
    vsock_path: &str,
) -> Result<crate::endpoint_proof::SpacedEndpointRuntimeIdentity, Status> {
    use pplx_space_common_core::types::{LinuxPid, LinuxUid, VsockCid};

    Ok(crate::endpoint_proof::SpacedEndpointRuntimeIdentity {
        sandbox_id: sandbox_id.parse().map_err(|error| {
            Status::failed_precondition(format!("sandbox id is invalid: {error}"))
        })?,
        execution_generation: route_identity.execution_generation.ok_or_else(|| {
            Status::failed_precondition(
                "sandbox execution generation is missing for endpoint attestation",
            )
        })?,
        activation_attempt_id: route_identity.activation_attempt_id.ok_or_else(|| {
            Status::failed_precondition(
                "sandbox activation attempt is missing for endpoint attestation",
            )
        })?,
        spaced_host_pid: LinuxPid::try_from_u32(spaced_host_pid)
            .map_err(|error| Status::failed_precondition(error.to_string()))?,
        runtime_pid: LinuxPid::try_from_u32(runtime_pid)
            .map_err(|error| Status::failed_precondition(error.to_string()))?,
        sandbox_uid: LinuxUid::new(sandbox_uid),
        guest_cid: VsockCid::try_from_u32(guest_cid)
            .map_err(|error| Status::failed_precondition(error.to_string()))?,
        vsock_path: vsock_path.into(),
    })
}

pub(super) async fn capture_spaced_endpoint_proof(
    node_client: &mut NodeManagerGrpcClient,
    runtime: crate::endpoint_proof::SpacedEndpointRuntimeIdentity,
) -> Result<crate::endpoint_proof::SpacedEndpointProof, Status> {
    const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    let mut request = Request::new(
        pplx_space_node_mgr_client::proto::CaptureSandboxEndpointProofRequest {
            sandbox_id: runtime.sandbox_id.to_string(),
        },
    );
    request.set_timeout(RPC_TIMEOUT);
    let node_proof = node_client
        .capture_sandbox_endpoint_proof(request)
        .await
        .map(Response::into_inner)
        .map_err(node_endpoint_proof_status)?;
    crate::endpoint_proof::SpacedEndpointProof::from_node_proof(&runtime, node_proof)
        .map_err(endpoint_proof_error_status)
}

pub(super) async fn capture_spaced_endpoint_proof_for_activation(
    node_client: &mut NodeManagerGrpcClient,
    runtime: crate::endpoint_proof::SpacedEndpointRuntimeIdentity,
) -> Result<crate::endpoint_proof::SpacedEndpointProof, Status> {
    let mut attempt = 0;
    loop {
        match capture_spaced_endpoint_proof(node_client, runtime.clone()).await {
            Ok(proof) => return Ok(proof),
            Err(error) => {
                let Some(delay) = activation_capture_retry_delay(attempt, &error) else {
                    return Err(error);
                };
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

fn activation_capture_retry_delay(attempt: usize, error: &Status) -> Option<std::time::Duration> {
    const DELAYS: [std::time::Duration; 2] = [
        std::time::Duration::from_millis(50),
        std::time::Duration::from_millis(200),
    ];
    (error.code() == tonic::Code::Unavailable)
        .then(|| DELAYS.get(attempt).copied())
        .flatten()
}

async fn validate_spaced_endpoint_proof(
    node_client: &mut NodeManagerGrpcClient,
    proof: &crate::endpoint_proof::SpacedEndpointProof,
    runtime: &crate::endpoint_proof::SpacedEndpointRuntimeIdentity,
) -> Result<(), Status> {
    const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
    proof
        .validate_runtime(runtime)
        .map_err(endpoint_proof_error_status)?;
    let node_client = node_client.clone();
    let node_proof = proof.node_proof();
    call_endpoint_proof_rpc("validate_sandbox_endpoint_proof", || {
        let mut node_client = node_client.clone();
        let node_proof = node_proof.clone();
        async move {
            let mut request = Request::new(
                pplx_space_node_mgr_client::proto::ValidateSandboxEndpointProofRequest {
                    proof: Some(node_proof),
                },
            );
            request.set_timeout(RPC_TIMEOUT);
            node_client.validate_sandbox_endpoint_proof(request).await
        }
    })
    .await
    .map(|_| ())
    .map_err(node_endpoint_proof_status)
}

async fn call_endpoint_proof_rpc<T, F, Fut>(
    command: &'static str,
    operation: F,
) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    // Endpoint proof reads can be replayed safely. Their retry window spans
    // node-manager's service-mutex handoff without giving an API request the
    // full node-manager shutdown budget.
    const POLICY: pplx_space_common_grpc::GrpcRetryPolicy =
        pplx_space_common_grpc::GrpcRetryPolicy {
            max_retries: 32,
            initial_delay: std::time::Duration::from_millis(100),
            backoff_multiplier: 2.0,
            max_delay: std::time::Duration::from_millis(500),
            max_elapsed: std::time::Duration::from_secs(15),
            jitter_factor: 0.2,
            max_server_retry_after: std::time::Duration::from_secs(2),
        };
    pplx_space_common_grpc::retry_grpc_status_with_context_if(
        "node-manager",
        command,
        POLICY,
        pplx_space_common_grpc::GrpcRetryContext::with_timeout(POLICY.max_elapsed),
        |status| {
            matches!(
                status.code(),
                tonic::Code::DeadlineExceeded | tonic::Code::Unimplemented
            )
        },
        operation,
    )
    .await
}

fn endpoint_proof_error_status(error: crate::endpoint_proof::SpacedEndpointProofError) -> Status {
    match error {
        crate::endpoint_proof::SpacedEndpointProofError::Invalid(_) => {
            Status::internal(format!("spaced endpoint proof is malformed: {error}"))
        }
        crate::endpoint_proof::SpacedEndpointProofError::Mismatch(_) => {
            Status::permission_denied(format!("spaced endpoint attestation failed: {error}"))
        }
    }
}

fn node_endpoint_proof_status(error: Status) -> Status {
    match error.code() {
        tonic::Code::NotFound
        | tonic::Code::Unavailable
        | tonic::Code::Unimplemented
        | tonic::Code::Unknown
        | tonic::Code::DeadlineExceeded => {
            Status::unavailable("spaced endpoint proof is temporarily unavailable")
        }
        tonic::Code::InvalidArgument => {
            Status::internal("node-manager returned an invalid spaced endpoint proof")
        }
        tonic::Code::FailedPrecondition | tonic::Code::PermissionDenied => {
            Status::permission_denied("spaced endpoint attestation failed")
        }
        _ => Status::internal("spaced endpoint attestation could not be evaluated"),
    }
}

fn endpoint_proof_cas_lost_status() -> Status {
    Status::unavailable(
        "sandbox execution changed while endpoint proof was captured; retry activation",
    )
}

fn resolve_endpoint_proof_cas_winner(
    captured: &crate::endpoint_proof::SpacedEndpointProof,
    winning: crate::endpoint_proof::SpacedEndpointProof,
) -> Result<crate::endpoint_proof::SpacedEndpointProof, Status> {
    if captured == &winning {
        Ok(winning)
    } else {
        Err(endpoint_proof_cas_lost_status())
    }
}

fn record_endpoint_attestation_error(error: &Status) {
    use pplx_space_metrics::sandbox::worker::{
        SpacedEndpointAttestationResult, spaced_endpoint_attestation,
    };

    let result = match error.code() {
        tonic::Code::PermissionDenied | tonic::Code::FailedPrecondition => {
            SpacedEndpointAttestationResult::LocalBindingRejected
        }
        _ => SpacedEndpointAttestationResult::Unavailable,
    };
    spaced_endpoint_attestation(result);
}

async fn connect_spaced_host_live(
    sandbox: &crate::db::SandboxRow,
) -> Result<pplx_space_spaced_host_client::SpacedHostClient, Status> {
    use pplx_space_metrics::sandbox::worker::{
        SpacedEndpointAttestationResult, spaced_endpoint_attestation,
    };

    let socket_path = spaced_host_socket_path(&sandbox.id);
    let spaced_host_pid = sandbox
        .spaced_host_pid
        .ok_or_else(|| Status::failed_precondition("spaced-host pid is not recorded"))?;
    let uid = sandbox
        .uid
        .ok_or_else(|| Status::failed_precondition("sandbox uid is not recorded"))?;
    if let Err(error) = validate_spaced_endpoint_path(Path::new(&socket_path), uid).await {
        let result = if error.code() == tonic::Code::Unavailable {
            SpacedEndpointAttestationResult::Unavailable
        } else {
            SpacedEndpointAttestationResult::LocalBindingRejected
        };
        spaced_endpoint_attestation(result);
        return Err(error);
    }
    let config = pplx_space_spaced_host_client::SpacedHostClientConfig::default()
        .with_socket_path(socket_path);
    match pplx_space_spaced_host_client::SpacedHostClient::new_with_config_and_expected_peer(
        config,
        pplx_space_spaced_common::ExpectedUdsPeer::new(
            i32::try_from(spaced_host_pid)
                .map_err(|_| Status::failed_precondition("spaced-host pid is invalid"))?,
            uid,
        ),
    )
    .await
    {
        Ok(client) => Ok(client),
        Err(error) => {
            if error_chain_has_permission_denied(&error) {
                spaced_endpoint_attestation(SpacedEndpointAttestationResult::UdsPeerRejected);
                Err(Status::permission_denied(format!(
                    "spaced-host endpoint identity verification failed: {error}"
                )))
            } else {
                spaced_endpoint_attestation(SpacedEndpointAttestationResult::Unavailable);
                Err(Status::unavailable(format!(
                    "spaced-host unavailable: {error}"
                )))
            }
        }
    }
}

async fn validate_spaced_endpoint_path(
    socket_path: &Path,
    expected_uid: u32,
) -> Result<(), Status> {
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    let metadata = tokio::fs::symlink_metadata(socket_path)
        .await
        .map_err(|error| {
            Status::unavailable(format!(
                "spaced-host unavailable: inspect socket ownership: {error}"
            ))
        })?;
    if !metadata.file_type().is_socket() {
        return Err(Status::permission_denied(
            "spaced-host endpoint is not a Unix socket",
        ));
    }
    if metadata.uid() != expected_uid {
        return Err(Status::permission_denied(format!(
            "spaced-host endpoint owner mismatch: expected uid={expected_uid}, got uid={}",
            metadata.uid()
        )));
    }
    Ok(())
}

fn error_chain_has_permission_denied(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
        {
            return true;
        }
        current = error.source();
    }
    false
}

/// Upper bound on one loopback-bridge operation against spaced-host: the
/// connect to the host process plus the bridge RPC itself.
///
/// The guest half of the RPC crosses the firecracker vsock CONNECT
/// handshake, which has no timeout of its own. The guest-bridge state gate
/// admits wake-in-flight states (`Resuming`, `Restoring`) where the VM is
/// resident but its vCPUs are not thawed yet; against such a frozen guest
/// the spaced-host connect succeeds (the host process is alive) and the
/// handshake then parks until the caller's own deadline. Without a local
/// bound, one frozen guest holds its caller — typically a node-ctl
/// exposure-reconcile slot — for the full client deadline on every tick.
///
/// Latency contract: comfortably exceeds the server-side worst case for
/// both bridge RPCs — callers pass a ~2 s guest-side open wait (which
/// itself absorbs the in-guest bounded bind retries), the spaced-host
/// connect retries total ~1.3 s, and the close path's command retry budget
/// is 3 × ≤100 ms. A caller that passed a guest-side wait at or above this
/// bound would see a retryable timeout instead of its result.
pub(super) const SPACED_LOOPBACK_BRIDGE_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Run `operation` — the full connect-to-spaced-host + bridge-RPC pipeline
/// — under [`SPACED_LOOPBACK_BRIDGE_RPC_TIMEOUT`]. Elapse maps to
/// `Unavailable`, the same retryable class as a failed spaced-host connect,
/// so the exposure reconciler treats a frozen guest exactly like a
/// not-yet-up one: fail fast now, retry next tick.
pub(super) async fn bounded_loopback_bridge_op<T>(
    rpc_name: &'static str,
    operation: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::time::timeout(SPACED_LOOPBACK_BRIDGE_RPC_TIMEOUT, operation)
        .await
        .map_err(|_| {
            Status::unavailable(format!(
                "{rpc_name} did not complete within \
                 {SPACED_LOOPBACK_BRIDGE_RPC_TIMEOUT:?}; the guest may still be frozen mid-wake"
            ))
        })?
}

/// Whether an `OpenLoopbackBridge` response status warrants one host-driven
/// close + open re-arm cycle before returning to the caller.
///
/// Older spaced-guest builds cache a lost bind race — e.g. the guest
/// address not yet up while a wake converges — as a sticky terminal status
/// and short-circuit every later open to that cached failure, leaving the
/// exposed port answering 502 for the rest of the spaced-guest process
/// lifetime no matter how often the exposure reconciler retries. `Close`
/// tears the cached entry down on every guest build, so close + one fresh
/// open re-drives the bind. Current guests re-arm failed statuses on open
/// themselves, making the extra cycle redundant but harmless; a genuinely
/// conflicted port fails the re-driven bind again and the caller gets the
/// fresh status.
///
/// Healthy statuses pass through untouched, the transient
/// `WaitingForListener` seed is left to resolve on its own, and unknown
/// wire values must not trigger guest RPC churn.
pub(super) fn loopback_bridge_open_warrants_rearm(status: i32) -> bool {
    use pplx_space_sbx_mgr_client::operations::LoopbackBridgeStatus;
    matches!(
        LoopbackBridgeStatus::try_from(status),
        Ok(LoopbackBridgeStatus::BindConflict | LoopbackBridgeStatus::Failed)
    )
}

pub(super) fn proto_duration_to_std(duration: Option<prost_types::Duration>) -> Option<Duration> {
    let duration = duration?;
    // `Duration::new` panics when nanos >= 1e9. Treat any malformed value
    // (negative, or out-of-range nanos) as "no duration" rather than
    // crashing the sbx-mgr handler. Callers that need stricter behavior
    // should validate before calling.
    if duration.seconds < 0 || duration.nanos < 0 || duration.nanos >= 1_000_000_000 {
        return None;
    }
    Some(Duration::new(
        duration.seconds.cast_unsigned(),
        duration.nanos.cast_unsigned(),
    ))
}

pub(super) fn spaced_version_info(
    version: Option<host_proto::VersionInfo>,
) -> Option<SpacedVersionInfo> {
    version.map(|version| SpacedVersionInfo {
        version: version.version,
        compatibility_family: version.compatibility_family,
    })
}

pub(super) fn stat_to_file_info(
    stat: host_proto::StatResponse,
) -> Result<SpacedStatResponse, Status> {
    let info = stat
        .info
        .ok_or_else(|| Status::internal("spaced host omitted stat file info"))?;
    Ok(SpacedStatResponse { info: Some(info) })
}

pub(super) fn file_event_to_spaced(
    event: host_proto::FileEvent,
) -> Result<SpacedFileEvent, Status> {
    file_event_kind_to_spaced(event.kind)?;
    Ok(event)
}

pub(super) fn file_event_kind_to_spaced(kind: i32) -> Result<i32, Status> {
    if kind == SpacedFileEventKind::Unspecified as i32 {
        return Err(Status::internal(
            "spaced host returned unspecified file event kind",
        ));
    }
    Ok(kind)
}

pub(super) fn write_batch_entry_to_host(
    entry: SpacedWriteBatchEntry,
) -> host_proto::WriteBatchEntry {
    host_proto::WriteBatchEntry {
        path: entry.path,
        data: entry.data,
        append: entry.append,
    }
}

pub(super) fn process_info_to_spaced(info: host_proto::ProcessInfo) -> SpacedProcessInfo {
    SpacedProcessInfo {
        process_id: info.process_id,
        pid: info.pid,
        command: info.command,
        cwd: info.cwd,
        env: info.env,
        user: info.user,
        tag: info.tag,
        pty: info.pty,
        // Unlike a stream's exit event, a listed exit without an outcome is
        // reported as unknown: prost decodes an outcome added by a newer guest
        // as none, and one record must not fail the whole list.
        exit: info.exit.map(|exit| SpacedProcessExit {
            outcome: Some(exit.outcome.map_or(
                crate::proto::spaced_process_exit::Outcome::Unknown(SpacedProcessUnknown {}),
                process_outcome_to_spaced,
            )),
        }),
        ended_at: info.ended_at,
    }
}

// Decode boundary: reject structurally missing wire data (unset oneofs) as a
// protocol error. An explicit `HostOutcome::Unknown` is a valid wire value a
// server may set on purpose and is forwarded as `SpacedOutcome::Unknown`;
// genuinely unknown future oneof variants would decode to `None` under prost
// and hit the same error path, which is the desired behavior here.
pub(super) fn process_event_to_spaced(
    event: host_proto::ProcessEvent,
) -> Result<SpacedProcessEvent, Status> {
    use crate::proto::spaced_process_event::Event as SpacedEvent;
    use host_proto::process_event::Event as HostEvent;

    let host_event = event
        .event
        .ok_or_else(|| Status::internal("spaced host returned process event without payload"))?;
    let event_payload = match host_event {
        HostEvent::Started(started) => SpacedEvent::Started(SpacedProcessStarted {
            start_time: started.start_time,
        }),
        HostEvent::Connected(connected) => SpacedEvent::Connected(SpacedProcessConnected {
            output_cursor: connected.output_cursor,
        }),
        HostEvent::Stdout(output) => SpacedEvent::Stdout(SpacedProcessOutput { data: output.data }),
        HostEvent::Stderr(output) => SpacedEvent::Stderr(SpacedProcessOutput { data: output.data }),
        HostEvent::Exit(exit) => SpacedEvent::Exit(process_exit_to_spaced(exit)?),
    };

    Ok(SpacedProcessEvent {
        process_id: event.process_id,
        pid: event.pid,
        event: Some(event_payload),
    })
}

fn process_exit_to_spaced(exit: host_proto::ProcessExit) -> Result<SpacedProcessExit, Status> {
    let host_outcome = exit
        .outcome
        .ok_or_else(|| Status::internal("spaced host returned exit event without outcome"))?;
    Ok(SpacedProcessExit {
        outcome: Some(process_outcome_to_spaced(host_outcome)),
    })
}

fn process_outcome_to_spaced(
    outcome: host_proto::process_exit::Outcome,
) -> crate::proto::spaced_process_exit::Outcome {
    use crate::proto::spaced_process_exit::Outcome as SpacedOutcome;
    use host_proto::process_exit::Outcome as HostOutcome;

    match outcome {
        HostOutcome::Exited(exited) => SpacedOutcome::Exited(SpacedProcessExited {
            exit_code: exited.exit_code,
        }),
        HostOutcome::Signaled(signaled) => SpacedOutcome::Signaled(SpacedProcessSignaled {
            signal: signaled.signal,
        }),
        HostOutcome::TimedOut(timed_out) => SpacedOutcome::TimedOut(SpacedProcessTimedOut {
            signal: timed_out.signal,
        }),
        HostOutcome::Unknown(_) => SpacedOutcome::Unknown(SpacedProcessUnknown {}),
    }
}

/// Interval between presence touches while a foreground guest stream stays
/// open. Must stay well below the smallest auto-lifecycle idle timeout in use
/// (minutes in practice): a policy whose `stage.at` is at or below this
/// interval is not protected by presence touches.
const STREAM_PRESENCE_TOUCH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Upper bound on how long a single open stream keeps refreshing the idle
/// clock. Bounds the pathological client that holds a stream open forever
/// without ever disconnecting: after the cap, touches stop and normal idle
/// reclaim resumes. A reconnect opens a fresh stream and a fresh cap, since
/// reconnecting is real activity.
const STREAM_PRESENCE_TOUCH_CAP: std::time::Duration = std::time::Duration::from_secs(7_200);

/// Outcome of one presence-touch attempt. The closed vocabulary behind the
/// `sandbox.stream_presence.touch{result:…}` metric (which additionally
/// carries the loop-level `cap_reached` and `failed` results).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamPresenceTouch {
    /// The idle clock was refreshed.
    Applied,
    /// The per-sandbox wake lock was contended (a wake or suspend is in
    /// flight); the tick was skipped, not queued.
    SkippedLockContended,
    /// The sandbox is not `Started`; a background task never re-arms a
    /// suspending/suspended row.
    SkippedNotStarted,
    /// The process is handing ownership to its successor.
    SkippedDraining,
}

impl StreamPresenceTouch {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::SkippedLockContended => "skipped_lock",
            Self::SkippedNotStarted => "skipped_state",
            Self::SkippedDraining => "skipped_draining",
        }
    }
}

/// Presence-based auto-lifecycle touches for a long-lived foreground guest
/// stream: while the stream is open, the idle clock is refreshed every
/// `interval` (up to `cap` total) so a sandbox running a foreground command
/// is not suspended as "idle" mid-command. The forward loop exits when the
/// client disconnects, so an abandoned session stops being touched and is
/// reclaimed on the normal idle schedule.
pub(super) struct StreamPresence {
    pub(super) service: SandboxManagerService,
    pub(super) sandbox_id: String,
    pub(super) activity: GuestActivityKind,
    pub(super) interval: std::time::Duration,
    pub(super) cap: std::time::Duration,
    #[cfg(test)]
    pub(super) tick_observer: Option<tokio::sync::mpsc::UnboundedSender<StreamPresenceTouch>>,
}

impl StreamPresence {
    /// Build the presence config for one stream.
    pub(super) fn resolve(
        service: SandboxManagerService,
        sandbox_id: String,
        activity: GuestActivityKind,
    ) -> Self {
        Self {
            service,
            sandbox_id,
            activity,
            interval: STREAM_PRESENCE_TOUCH_INTERVAL,
            cap: STREAM_PRESENCE_TOUCH_CAP,
            #[cfg(test)]
            tick_observer: None,
        }
    }
}

pub(super) fn forward_host_stream<S, T, U, F>(stream: S, convert: F) -> BoxedStatusStream<U>
where
    S: futures_util::Stream<Item = Result<T, Status>> + Send + Unpin + 'static,
    T: Send + 'static,
    U: Send + 'static,
    F: Fn(T) -> Result<U, Status> + Send + 'static,
{
    forward_host_stream_with_presence(stream, convert, None, None)
}

pub(super) fn fence_status_stream<T>(
    stream: BoxedStatusStream<T>,
    identity_fence: RouteStreamFence,
) -> BoxedStatusStream<T>
where
    T: Send + 'static,
{
    fence_status_stream_with_guard(stream, identity_fence, ())
}

pub(super) fn fence_admitted_status_stream<T, G>(
    stream: BoxedStatusStream<T>,
    identity_fence: RouteStreamFence,
    admission: G,
) -> BoxedStatusStream<T>
where
    T: Send + 'static,
    G: Send + 'static,
{
    fence_status_stream_with_guard(stream, identity_fence, admission)
}

fn fence_status_stream_with_guard<T, G>(
    mut stream: BoxedStatusStream<T>,
    identity_fence: RouteStreamFence,
    guard: G,
) -> BoxedStatusStream<T>
where
    T: Send + 'static,
    G: Send + 'static,
{
    use futures_util::StreamExt;

    // Normal EOF leaves queued output owned by the response. Cancellation
    // discards that output, so it can release admission without waiting for a
    // backpressured consumer. Both release paths take the same guard once.
    let guard = Arc::new(Mutex::new(Some(guard)));
    let producer_guard = guard.clone();
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    let (terminal_tx, terminal_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let terminal_tx = RouteStreamTerminalSender::new(terminal_tx);
        let terminal = loop {
            tokio::select! {
                _ = tx.closed() => {
                    terminal_tx.abandon();
                    return;
                }
                item = stream.next() => {
                    let Some(item) = item else {
                        // Identity cancellation can also make the request side
                        // close and the upstream stream reach EOF. If both are
                        // ready before this producer records a terminal state,
                        // the security fence wins; a Completed state recorded
                        // before a later cancellation remains authoritative.
                        if identity_fence.is_cancelled() {
                            break RouteStreamTerminal::Cancelled(
                                route_stream_cancellation_status(&identity_fence),
                            );
                        }
                        break RouteStreamTerminal::Completed;
                    };
                    let item = match item {
                        Ok(item) => item,
                        Err(status) => break RouteStreamTerminal::UpstreamError(status),
                    };
                    let send_result = tokio::select! {
                        result = tx.send(item) => result,
                        _ = identity_fence.cancelled() => {
                            break RouteStreamTerminal::Cancelled(
                                route_stream_cancellation_status(&identity_fence),
                            );
                        }
                    };
                    if send_result.is_err() {
                        terminal_tx.abandon();
                        return;
                    }
                }
                _ = identity_fence.cancelled() => {
                    break RouteStreamTerminal::Cancelled(
                        route_stream_cancellation_status(&identity_fence),
                    );
                }
            }
        };
        let cancelled = matches!(terminal, RouteStreamTerminal::Cancelled(_));
        terminal_tx.finish(terminal);
        if cancelled {
            let released = producer_guard
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(released);
        }
    });

    Box::pin(GuardedStatusStream {
        stream: FencedStatusStream::new(rx, terminal_rx),
        guard,
    })
}

struct GuardedStatusStream<T, G> {
    stream: FencedStatusStream<T>,
    guard: Arc<Mutex<Option<G>>>,
}

impl<T, G> Drop for GuardedStatusStream<T, G> {
    fn drop(&mut self) {
        let released = self
            .guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(released);
    }
}

impl<T, G> Unpin for GuardedStatusStream<T, G> {}
impl<T, G> futures_util::Stream for GuardedStatusStream<T, G> {
    type Item = Result<T, Status>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.get_mut().stream).poll_next(cx)
    }
}

enum RouteStreamTerminal {
    Completed,
    UpstreamError(Status),
    Cancelled(Status),
    ProducerFailed,
}

struct RouteStreamTerminalSender {
    inner: Option<tokio::sync::oneshot::Sender<RouteStreamTerminal>>,
}

impl RouteStreamTerminalSender {
    fn new(inner: tokio::sync::oneshot::Sender<RouteStreamTerminal>) -> Self {
        Self { inner: Some(inner) }
    }

    fn finish(mut self, terminal: RouteStreamTerminal) {
        if let Some(inner) = self.inner.take() {
            let _ = inner.send(terminal);
        }
    }

    fn abandon(mut self) {
        self.inner.take();
    }
}

impl Drop for RouteStreamTerminalSender {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let _ = inner.send(RouteStreamTerminal::ProducerFailed);
        }
    }
}

struct FencedStatusStream<T> {
    data: tokio_stream::wrappers::ReceiverStream<T>,
    terminal_rx: Option<tokio::sync::oneshot::Receiver<RouteStreamTerminal>>,
    terminal: Option<RouteStreamTerminal>,
    done: bool,
}

impl<T> FencedStatusStream<T> {
    fn new(
        data: tokio::sync::mpsc::Receiver<T>,
        terminal_rx: tokio::sync::oneshot::Receiver<RouteStreamTerminal>,
    ) -> Self {
        Self {
            data: tokio_stream::wrappers::ReceiverStream::new(data),
            terminal_rx: Some(terminal_rx),
            terminal: None,
            done: false,
        }
    }
}

impl<T> tokio_stream::Stream for FencedStatusStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::future::Future as _;

        if self.done {
            return std::task::Poll::Ready(None);
        }

        if self.terminal.is_none() {
            let terminal_result =
                self.terminal_rx
                    .as_mut()
                    .and_then(|receiver| match Pin::new(receiver).poll(cx) {
                        std::task::Poll::Ready(result) => Some(result),
                        std::task::Poll::Pending => None,
                    });
            if let Some(result) = terminal_result {
                self.terminal_rx = None;
                self.terminal = Some(result.unwrap_or(RouteStreamTerminal::ProducerFailed));
            }
        }

        let cancellation = match self.terminal.take() {
            Some(RouteStreamTerminal::Cancelled(status)) => Some(status),
            terminal => {
                self.terminal = terminal;
                None
            }
        };
        if let Some(status) = cancellation {
            self.done = true;
            return std::task::Poll::Ready(Some(Err(status)));
        }

        match Pin::new(&mut self.data).poll_next(cx) {
            std::task::Poll::Ready(Some(item)) => std::task::Poll::Ready(Some(Ok(item))),
            std::task::Poll::Ready(None) => {
                let Some(terminal) = self.terminal.take() else {
                    return std::task::Poll::Pending;
                };
                self.done = true;
                match terminal {
                    RouteStreamTerminal::Completed => std::task::Poll::Ready(None),
                    RouteStreamTerminal::UpstreamError(status)
                    | RouteStreamTerminal::Cancelled(status) => {
                        std::task::Poll::Ready(Some(Err(status)))
                    }
                    RouteStreamTerminal::ProducerFailed => std::task::Poll::Ready(Some(Err(
                        Status::internal("route stream forwarding task failed"),
                    ))),
                }
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

fn route_stream_cancellation_status(identity_fence: &RouteStreamFence) -> Status {
    let close_reason = identity_fence.close_reason();
    pplx_space_metrics::sandbox::worker::route_stream_closed(close_reason);
    Status::aborted(match close_reason {
        pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::IdentityChanged => {
            "sandbox execution identity changed while stream was open"
        }
        pplx_space_metrics::sandbox::worker::RouteStreamCloseReason::LifecycleTeardown => {
            "sandbox lifecycle teardown closed the stream"
        }
    })
}

pub(super) fn forward_host_stream_with_presence<S, T, U, F>(
    mut stream: S,
    convert: F,
    max_items: Option<usize>,
    presence: Option<StreamPresence>,
) -> BoxedStatusStream<U>
where
    S: futures_util::Stream<Item = Result<T, Status>> + Send + Unpin + 'static,
    T: Send + 'static,
    U: Send + 'static,
    F: Fn(T) -> Result<U, Status> + Send + 'static,
{
    use futures_util::StreamExt;

    let (tx, rx) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        let started_at = tokio::time::Instant::now();
        // An explicit cap of zero is "presence disabled for this stream":
        // no ticks ever arm, and no cap-reached warn fires.
        let mut presence_active = presence
            .as_ref()
            .is_some_and(|presence| !presence.cap.is_zero());
        // The first tick fires one full interval in; the stream-start
        // activation already touched the idle clock. Skipped (not bursted)
        // ticks: a forward loop parked in a backpressured `tx.send` for
        // multiple intervals should not fire a catch-up burst of touches.
        // That parking is also a deliberate third presence state between
        // "open" and "disconnected": while a slow reader keeps the 32-slot
        // channel full, this arm is never polled, so a connected-but-not-
        // reading client can go un-touched for multiple intervals and the
        // sandbox may idle-suspend under its nominally open stream — not
        // reading is treated as not present.
        let presence_interval_period = presence
            .as_ref()
            .map(|presence| presence.interval)
            .unwrap_or(STREAM_PRESENCE_TOUCH_INTERVAL);
        let mut presence_interval = tokio::time::interval_at(
            started_at + presence_interval_period,
            presence_interval_period,
        );
        presence_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut sent_items = 0usize;
        loop {
            tokio::select! {
                () = tx.closed() => break,
                _ = presence_interval.tick(), if presence_active => {
                    // Defensive let-else: a panic here would kill the forward
                    // task and silently truncate the client's stream.
                    let Some(presence) = presence.as_ref() else {
                        presence_active = false;
                        continue;
                    };
                    if started_at.elapsed() >= presence.cap {
                        tracing::warn!(
                            sandbox_id = %presence.sandbox_id,
                            elapsed_secs = started_at.elapsed().as_secs(),
                            "stream presence cap reached; idle reclaim resumes for this sandbox"
                        );
                        pplx_space_metrics::stream_presence::touch("cap_reached");
                        presence_active = false;
                        continue;
                    }
                    match presence
                        .service
                        .touch_runtime_state_for_stream_presence(
                            &presence.sandbox_id,
                            presence.activity,
                        )
                        .await
                    {
                        Ok(outcome) => {
                            pplx_space_metrics::stream_presence::touch(outcome.as_str());
                            #[cfg(test)]
                            if let Some(observer) = &presence.tick_observer {
                                let _ = observer.send(outcome);
                            }
                        }
                        Err(error) => {
                            pplx_space_metrics::stream_presence::touch("failed");
                            // Best-effort: a failed touch must never tear down
                            // the data stream. NotFound is the expected shape
                            // (row deleted under an open stream, repeating every
                            // tick until the client disconnects) and stays at
                            // debug; anything else is a real persistence problem
                            // worth a warn.
                            if error.code() == tonic::Code::NotFound {
                                tracing::debug!(
                                    sandbox_id = %presence.sandbox_id,
                                    error = %error.message(),
                                    "stream presence touch skipped: sandbox row is gone"
                                );
                            } else {
                                tracing::warn!(
                                    sandbox_id = %presence.sandbox_id,
                                    error = %error.message(),
                                    "stream presence touch failed"
                                );
                            }
                        }
                    }
                }
                item = stream.next() => {
                    let item = match item {
                        Some(Ok(item)) => convert(item),
                        None => break,
                        Some(Err(status)) => Err(status),
                    };
                    let is_terminal_error = item.is_err();
                    if tx.send(item).await.is_err() || is_terminal_error {
                        break;
                    }
                    sent_items += 1;
                    if max_items.is_some_and(|max_items| sent_items >= max_items) {
                        break;
                    }
                }
            }
        }
    });
    Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
}

#[cfg(test)]
mod tests;

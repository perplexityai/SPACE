use pplx_space_common_core::types::LocalArtifactId;

use super::activation_support::update_state_best_effort;
use super::cache_cleanup::is_sandbox_running;
use super::cleanup_support::require_state_write;
use super::create_support::*;
use super::tail_support::*;
use super::template_snapshot_support::{
    compute_template_base_snapshot_hash, publish_template_base_snapshot_dir,
};
use super::validation::{canonical_batched_sandbox_ids, snapshot_type_from_proto};
use super::*;
use pplx_space_common_core::inventory::InventoryBudget;

fn state_publication_metric_field(
    field: crate::db::SandboxStatePublicationMetadataField,
) -> pplx_space_metrics::sandbox::publication::StatePublicationField {
    use crate::db::SandboxStatePublicationMetadataField as DbField;
    use pplx_space_metrics::sandbox::publication::StatePublicationField as MetricField;

    match field {
        DbField::QuarantineReasonCode => MetricField::QuarantineReasonCode,
        DbField::TransitionReceiptId => MetricField::TransitionReceiptId,
        DbField::TransitionReceiptKind => MetricField::TransitionReceiptKind,
        DbField::TransitionReceiptTargetState => MetricField::TransitionReceiptTargetState,
        DbField::TransitionReceiptExecutionGeneration => {
            MetricField::TransitionReceiptExecutionGeneration
        }
        DbField::TransitionReceiptReasonCode => MetricField::TransitionReceiptReasonCode,
        DbField::TransitionReceiptActivationAttemptId => {
            MetricField::TransitionReceiptActivationAttemptId
        }
    }
}

fn state_publication_metadata_disposition(
    state: SandboxState,
) -> pplx_space_metrics::sandbox::publication::StatePublicationDisposition {
    use pplx_space_metrics::sandbox::publication::StatePublicationDisposition;

    if state == SandboxState::Quarantined {
        StatePublicationDisposition::FallbackQuarantined
    } else {
        StatePublicationDisposition::OmitTransitionReceipt
    }
}

use futures_util::{StreamExt, stream};
use pplx_space_common_core::types::{ArtifactKind, ArtifactOrigin};
use pplx_space_events::{
    SandboxEventDetails, SandboxEventSource, SandboxEventType, emit_sandbox_event,
};

const STATE_INFO_CONCURRENCY: usize = 16;
const MIB_BYTES: u64 = 1024 * 1024;
const OPERATION_SNAPSHOT_ARTIFACT_NAMESPACE: Uuid = Uuid::from_bytes([
    0x5f, 0x95, 0xa2, 0xe8, 0x1d, 0xb4, 0x4c, 0xd2, 0xa8, 0xcf, 0x54, 0x8b, 0xae, 0xc2, 0x75, 0xe1,
]);

fn sqlite_timestamp(value: &str) -> Option<prost_types::Timestamp> {
    match value.parse::<i64>() {
        Ok(seconds) => Some(prost_types::Timestamp { seconds, nanos: 0 }),
        Err(error) => {
            tracing::warn!(%error, "omitting malformed sandbox state change time");
            None
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum BuildQuarantineRecoveryObservationError {
    #[error("read quarantine recovery observation: {0}")]
    Read(#[source] rusqlite::Error),
    #[error("invalid local quarantine id {value:?}: {source}")]
    LocalQuarantineId {
        value: String,
        #[source]
        source: uuid::Error,
    },
    #[error("invalid quarantine recovery attempt count {value}: {source}")]
    AttemptCount {
        value: i64,
        #[source]
        source: std::num::TryFromIntError,
    },
    #[error("invalid quarantine recovery due time {value:?}: {source}")]
    NextAttemptAt {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("quarantine recovery due time is negative: {0}")]
    NegativeNextAttemptAt(i64),
}

fn build_quarantine_recovery_observation(
    raw: crate::db::RawQuarantineRecoveryObservationRead,
) -> Result<crate::proto::QuarantineRecoveryObservation, BuildQuarantineRecoveryObservationError> {
    let raw = raw.map_err(BuildQuarantineRecoveryObservationError::Read)?;
    let local_quarantine_id = raw.local_quarantine_id.parse::<Uuid>().map_err(|source| {
        BuildQuarantineRecoveryObservationError::LocalQuarantineId {
            value: raw.local_quarantine_id.clone(),
            source,
        }
    })?;
    let observed_owner = match raw.owner.as_deref() {
        None => crate::proto::QuarantineRecoveryObservedOwner::Unspecified,
        Some(value) => match value.parse::<crate::db::QuarantineRecoveryOwner>() {
            Ok(crate::db::QuarantineRecoveryOwner::LegacyRecovery) => {
                crate::proto::QuarantineRecoveryObservedOwner::LegacyRecovery
            }
            // The owner column is an ownership boundary, not part of the
            // episode identity. A release that does not know a newer owner's
            // spelling must still publish the exact quarantine and scheduling
            // facts while declining to classify that owner.
            Ok(crate::db::QuarantineRecoveryOwner::OperatorAttention) | Err(_) => {
                crate::proto::QuarantineRecoveryObservedOwner::Unspecified
            }
        },
    };
    let attempt_count = u32::try_from(raw.attempt_count).map_err(|source| {
        BuildQuarantineRecoveryObservationError::AttemptCount {
            value: raw.attempt_count,
            source,
        }
    })?;
    let next_attempt_at = raw
        .next_attempt_at
        .map(|value| {
            let seconds = value.parse::<i64>().map_err(|source| {
                BuildQuarantineRecoveryObservationError::NextAttemptAt {
                    value: value.clone(),
                    source,
                }
            })?;
            if seconds < 0 {
                return Err(
                    BuildQuarantineRecoveryObservationError::NegativeNextAttemptAt(seconds),
                );
            }
            Ok(prost_types::Timestamp { seconds, nanos: 0 })
        })
        .transpose()?;
    Ok(crate::proto::QuarantineRecoveryObservation {
        schema_version: 1,
        local_quarantine_id: local_quarantine_id.to_string(),
        observed_owner: observed_owner as i32,
        attempt_count,
        next_attempt_at,
        degraded: false,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OperationSnapshotPlan {
    source_state: SandboxState,
    flush_guest_filesystems: bool,
}

impl OperationSnapshotPlan {
    const fn from_state(state: SandboxState) -> Option<Self> {
        match state {
            SandboxState::Started => Some(Self {
                source_state: state,
                flush_guest_filesystems: true,
            }),
            SandboxState::Paused => Some(Self {
                source_state: state,
                flush_guest_filesystems: false,
            }),
            _ => None,
        }
    }

    const fn source_state(self) -> SandboxState {
        self.source_state
    }

    const fn flush_guest_filesystems(self) -> bool {
        self.flush_guest_filesystems
    }
}

fn disk_operation_snapshot_response(
    local_artifact_id: LocalArtifactId,
    snapshot_name: &pplx_space_common_core::types::SnapshotName,
) -> crate::proto::CreateSnapshotResponse {
    crate::proto::CreateSnapshotResponse {
        snapshot_name: snapshot_name.to_string(),
        r#type: crate::proto::SnapshotType::Disk as i32,
        published_artifact: Some(crate::proto::PublishedArtifact {
            local_artifact_id: local_artifact_id.to_string(),
            artifact_kind: crate::proto::ArtifactKind::Disk as i32,
            origin: crate::proto::ArtifactOrigin::Snapshot as i32,
            snapshot_name: Some(snapshot_name.to_string()),
        }),
        published_durable_artifact: None,
    }
}

fn disk_operation_snapshot_name(
    request_id: Uuid,
) -> Result<pplx_space_common_core::types::SnapshotName, Status> {
    pplx_space_common_core::types::SnapshotName::parse(format!("d-operation-{request_id}"))
        .map_err(|error| Status::internal(format!("invalid generated snapshot name: {error}")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiskOperationSnapshotPaths {
    capture: String,
    artifact: String,
}

impl DiskOperationSnapshotPaths {
    fn contains(&self, path: &str) -> bool {
        path == self.capture || path == self.artifact
    }

    fn any_exists(&self) -> bool {
        std::path::Path::new(&self.capture).exists()
            || std::path::Path::new(&self.artifact).exists()
    }
}

fn disk_operation_snapshot_paths(
    sandbox_id: &str,
    snapshot_name: &pplx_space_common_core::types::SnapshotName,
) -> DiskOperationSnapshotPaths {
    let capture = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
        sandbox_id,
        snapshot_name.as_str(),
    );
    DiskOperationSnapshotPaths {
        artifact: format!("{capture}-artifact"),
        capture,
    }
}

fn disk_operation_snapshot_artifact_id(
    sandbox_id: &str,
    request_id: Uuid,
    execution_generation: pplx_space_common_core::types::ExecutionGeneration,
    source_state: SandboxState,
) -> LocalArtifactId {
    let identity = format!(
        "{sandbox_id}\0disk\0{request_id}\0{}\0{}",
        execution_generation.get(),
        source_state.as_str(),
    );
    LocalArtifactId::from_uuid(Uuid::new_v5(
        &OPERATION_SNAPSHOT_ARTIFACT_NAMESPACE,
        identity.as_bytes(),
    ))
}

fn emit_disk_operation_snapshot_published(
    sandbox_id: &str,
    local_artifact_id: LocalArtifactId,
    snapshot_name: &pplx_space_common_core::types::SnapshotName,
) {
    emit_sandbox_event(
        sandbox_id,
        SandboxEventType::LocalArtifactPublished,
        SandboxEventSource::SbxManager,
        SandboxEventDetails::default()
            .local_artifact_id(local_artifact_id.to_string())
            .artifact_kind(pplx_space_common_core::types::ArtifactKind::Disk.as_str())
            .artifact_origin("snapshot")
            .snapshot_id(snapshot_name.to_string()),
    );
}

fn disk_operation_snapshot_artifact_matches(
    artifact: &crate::db::LocalArtifactRow,
    expected_local_artifact_id: LocalArtifactId,
    sandbox_id: &str,
    snapshot_name: &pplx_space_common_core::types::SnapshotName,
    paths: &DiskOperationSnapshotPaths,
) -> bool {
    artifact.local_artifact_id == expected_local_artifact_id
        && artifact.sandbox_id == sandbox_id
        && artifact.artifact_kind == pplx_space_common_core::types::ArtifactKind::Disk
        && artifact.artifact_origin == pplx_space_common_core::types::ArtifactOrigin::Snapshot
        && artifact.snapshot_name.as_ref() == Some(snapshot_name)
        && paths.contains(&artifact.local_path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationSnapshotNextStep {
    CreateSnapshot,
    Replay,
    RejectMissingArtifactPath,
    RemoveUnboundSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CurrentSnapshotPrecondition {
    execution_generation: i64,
    lifecycle: i32,
}

fn current_snapshot_precondition(
    req: &crate::proto::CreateSnapshotRequest,
) -> Result<CurrentSnapshotPrecondition, Status> {
    req.source
        .as_ref()
        .ok_or_else(|| {
            Status::invalid_argument("creation_identity and source must be supplied together")
        })
        .and_then(|source| match source.source.as_ref() {
            Some(crate::proto::create_snapshot_source::Source::CurrentSandbox(source)) => {
                Ok(CurrentSnapshotPrecondition {
                    execution_generation: source.execution_generation,
                    lifecycle: source.lifecycle,
                })
            }
            Some(crate::proto::create_snapshot_source::Source::DurableArtifact(_)) => {
                Err(Status::invalid_argument(
                    "durable artifact source must use durable snapshot publication",
                ))
            }
            None => Err(Status::invalid_argument(
                "create snapshot source must be specified",
            )),
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationSnapshotCreateDisposition {
    Created,
    RemoveUnboundSnapshot,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationSnapshotPublishFailureDisposition {
    ReplayConcurrentPublication,
    RemoveCreatedSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationSnapshotPostCreateDisposition {
    Publish,
    RemoveCreatedSnapshot,
}

fn operation_snapshot_post_create_disposition(
    source_precondition_still_holds: bool,
) -> OperationSnapshotPostCreateDisposition {
    if source_precondition_still_holds {
        OperationSnapshotPostCreateDisposition::Publish
    } else {
        OperationSnapshotPostCreateDisposition::RemoveCreatedSnapshot
    }
}

async fn remove_created_disk_operation_snapshot(
    node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox_id: &str,
    snapshot_name: &pplx_space_common_core::types::SnapshotName,
    failure: &Status,
) -> Result<(), Status> {
    match node_client
        .delete_btrfs_snapshot(
            pplx_space_node_mgr_client::proto::DeleteBtrfsSnapshotRequest {
                sandbox_id: sandbox_id.to_string(),
                snapshot_name: snapshot_name.to_string(),
            },
        )
        .await
    {
        Ok(_) => {}
        Err(status) if status.code() == tonic::Code::NotFound => {}
        Err(cleanup_error) => {
            return Err(Status::internal(format!(
                "{failure}; remove unpublished operation snapshot: {cleanup_error}"
            )));
        }
    }
    Ok(())
}

async fn remove_disk_operation_snapshot_attempt(
    node_client: &mut pplx_space_node_mgr_client::NodeManagerGrpcClient,
    sandbox_id: &str,
    snapshot_name: &pplx_space_common_core::types::SnapshotName,
    paths: &DiskOperationSnapshotPaths,
    failure: &Status,
) -> Result<(), Status> {
    let capture_cleanup =
        remove_created_disk_operation_snapshot(node_client, sandbox_id, snapshot_name, failure)
            .await;
    let artifact_cleanup = node_client
        .delete_cache_artifact(
            pplx_space_node_mgr_client::proto::DeleteCacheArtifactRequest {
                local_path: paths.artifact.clone(),
            },
        )
        .await
        .map(|_| ())
        .map_err(|cleanup_error| {
            Status::internal(format!(
                "{failure}; remove unpublished encoded operation artifact: {cleanup_error}"
            ))
        });
    match (capture_cleanup, artifact_cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(capture_error), Err(artifact_error)) => Err(Status::internal(format!(
            "{capture_error}; {artifact_error}"
        ))),
    }
}

fn operation_snapshot_publish_failure_disposition(
    matching_artifact_was_published: bool,
) -> OperationSnapshotPublishFailureDisposition {
    if matching_artifact_was_published {
        OperationSnapshotPublishFailureDisposition::ReplayConcurrentPublication
    } else {
        OperationSnapshotPublishFailureDisposition::RemoveCreatedSnapshot
    }
}

fn operation_snapshot_create_disposition(
    error_code: Option<tonic::Code>,
    snapshot_path_exists_after_failure: bool,
) -> OperationSnapshotCreateDisposition {
    match error_code {
        None => OperationSnapshotCreateDisposition::Created,
        Some(_) if snapshot_path_exists_after_failure => {
            OperationSnapshotCreateDisposition::RemoveUnboundSnapshot
        }
        Some(_) => OperationSnapshotCreateDisposition::Failed,
    }
}

fn operation_snapshot_next_step(
    artifact_exists: bool,
    snapshot_path_exists: bool,
) -> OperationSnapshotNextStep {
    match (artifact_exists, snapshot_path_exists) {
        (true, true) => OperationSnapshotNextStep::Replay,
        (true, false) => OperationSnapshotNextStep::RejectMissingArtifactPath,
        (false, true) => OperationSnapshotNextStep::RemoveUnboundSnapshot,
        (false, false) => OperationSnapshotNextStep::CreateSnapshot,
    }
}

fn live_sandbox_namespace(
    pid_alive: bool,
    runtime_label: &pplx_space_common_core::types::SandboxRuntimeLabel,
) -> Option<String> {
    pid_alive.then(|| format!("sns-{runtime_label}"))
}

fn sandbox_status_from_runtime_observation(
    state: SandboxState,
    pid_alive: bool,
    vm_state: Option<FirecrackerVmState>,
) -> SandboxState {
    if pid_alive
        && matches!(state, SandboxState::Started | SandboxState::Paused)
        && vm_state == Some(FirecrackerVmState::Paused)
    {
        SandboxState::Paused
    } else {
        state
    }
}

fn snapshot_file_memory_mib(path: &Path) -> Result<u32, Status> {
    let bytes = std::fs::metadata(path)
        .map_err(|error| Status::internal(format!("stat {}: {error}", path.display())))?
        .len();
    let memory_mib = bytes.div_ceil(MIB_BYTES);
    if memory_mib == 0 {
        return Err(Status::failed_precondition(format!(
            "{} is empty",
            path.display()
        )));
    }
    u32::try_from(memory_mib).map_err(|_| {
        Status::failed_precondition(format!(
            "{} exceeds u32 MiB size: {memory_mib} MiB",
            path.display()
        ))
    })
}

fn apply_route_identity_provenance_to_state(
    sandbox: &mut SandboxInfo,
    provenance: crate::db::ActivationRouteProvenance,
) {
    let observed_substrates = [
        provenance.current_substrate_kind,
        provenance.retained_substrate_kind,
    ];
    let substrate_kind = if observed_substrates.contains(&Some(
        pplx_space_common_core::types::NodeManagedBy::Kubernetes,
    )) {
        crate::proto::SandboxSubstrateKind::Kubernetes
    } else if observed_substrates
        .contains(&Some(pplx_space_common_core::types::NodeManagedBy::Space))
    {
        crate::proto::SandboxSubstrateKind::Direct
    } else {
        crate::proto::SandboxSubstrateKind::Unspecified
    };
    sandbox.route_provenance_substrate_kind = substrate_kind as i32;

    let generation = [
        provenance
            .current_route_generation
            .map(|generation| generation.get()),
        provenance
            .retained_route_generation
            .map(|generation| generation.get()),
        provenance
            .current_execution_generation
            .and_then(|generation| u64::try_from(generation.get()).ok()),
        provenance
            .retained_execution_generation
            .and_then(|generation| u64::try_from(generation.get()).ok()),
    ]
    .into_iter()
    .flatten()
    .max();
    let Some(generation) = generation else {
        return;
    };
    let Ok(execution_generation) = i64::try_from(generation) else {
        return;
    };
    sandbox.route_provenance_generation = Some(generation);
    sandbox.route_provenance_execution_generation = Some(execution_generation);
}

impl SandboxManagerService {
    pub(super) async fn state(
        &self,
        request: Request<StateRequest>,
    ) -> Result<Response<StateResponse>, Status> {
        self.authorize_peer(&request, "State", ALLOW_SANDBOX_OBSERVER)?;
        let sandbox_ids = canonical_batched_sandbox_ids(&request.get_ref().sandbox_ids)?;
        if sandbox_ids.is_empty() {
            return Err(Status::invalid_argument("sandbox_ids is required"));
        }

        use pplx_space_common_core::inventory::{MAX_ITEMS, MAX_RAW_BYTES};
        // Release SQLite between batches without resetting the RPC's budgets.
        let mut raw_budget = InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES);
        let mut metadata_budget = InventoryBudget::new(MAX_ITEMS, MAX_RAW_BYTES);
        let mut response = StateResponse::default();
        for batch in sandbox_ids.chunks(pplx_space_common_core::inventory::READ_BATCH_ITEMS) {
            let (batch, remaining_raw, remaining_metadata) = self
                .state_batch(batch.to_vec(), raw_budget, metadata_budget)
                .await?;
            raw_budget = remaining_raw;
            metadata_budget = remaining_metadata;
            response.sandboxes.extend(batch.sandboxes);
            response.missing_ids.extend(batch.missing_ids);
        }
        crate::inventory_budget::response(response)
    }

    async fn state_batch(
        &self,
        sandbox_ids: Vec<String>,
        mut raw_budget: InventoryBudget,
        mut metadata_budget: InventoryBudget,
    ) -> Result<(StateResponse, InventoryBudget, InventoryBudget), Status> {
        let (
            rows,
            missing_ids,
            mut publication_metadata,
            mut recovery_observations,
            mut sandboxes,
            route_identity_provenance,
            mut state_changed_at,
            raw_budget,
            metadata_budget,
        ) = self.storage(move |db| {
                crate::db::inventory_read::check_state_metadata(db, &sandbox_ids, &mut metadata_budget).map_err(|error| error.into_status())?;
                let mut route_identity_provenance =
                    crate::db::activation_route_provenance_for_sandboxes(db, &sandbox_ids).map_err(
                        |error| Status::internal(format!("load route identity provenance: {error}")),
                    )?;
                let mut rows = Vec::with_capacity(sandbox_ids.len());
                let mut missing_ids = Vec::new();
                let mut state_only = Vec::new();
                for sandbox_id in sandbox_ids {
                    match crate::db::inventory_read::get(db, &sandbox_id, &mut raw_budget) {
                        Ok(Some(sandbox)) => rows.push(sandbox),
                        Ok(None) => missing_ids.push(sandbox_id),
                        Err(error @ crate::db::inventory_read::InventoryReadError::Limit(_)) => return Err(error.into_status()),
                        Err(error @ crate::db::inventory_read::InventoryReadError::Ambiguous) => return Err(error.into_status()),
                        Err(crate::db::inventory_read::InventoryReadError::Database(error)) => {
                            let lifecycle = match crate::db::raw_sandbox_lifecycle(db, &sandbox_id) {
                                Ok(Some(state)) => state,
                                Ok(None) => {
                                    missing_ids.push(sandbox_id);
                                    continue;
                                }
                                Err(state_error) => {
                                    return Err(Status::internal(format!(
                                        "read raw sandbox state for {sandbox_id}: {state_error}"
                                    )));
                                }
                            };
                            let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::StateOnly;
                            pplx_space_metrics::sandbox::publication::degraded(
                                pplx_space_metrics::sandbox::publication::StatePublicationComponent::Worker,
                                pplx_space_metrics::sandbox::publication::StatePublicationField::CoreRow,
                                disposition,
                            );
                            tracing::warn!(
                                %sandbox_id,
                                %error,
                                %disposition,
                                "publishing raw lifecycle without malformed sandbox fields"
                            );
                            let route_identity_provenance = match route_identity_provenance
                                .remove(&sandbox_id)
                            {
                                Some(Ok(provenance)) => provenance,
                                Some(Err(provenance_error)) => {
                                    tracing::warn!(
                                        %sandbox_id,
                                        %provenance_error,
                                        "publishing degraded sandbox state without malformed route provenance"
                                    );
                                    crate::db::ActivationRouteProvenance::default()
                                }
                                None => crate::db::ActivationRouteProvenance::default(),
                            };
                            let mut sandbox = SandboxInfo {
                                lifecycle: Some(crate::proto::SandboxLifecycleState::from_label(&lifecycle.state).unwrap_or(crate::proto::SandboxLifecycleState::Unspecified) as i32),
                                sandbox_id,
                                state_changed_at: sqlite_timestamp(&lifecycle.state_changed_at),
                                state_degraded: true,
                                ..SandboxInfo::default()
                            };
                            apply_route_identity_provenance_to_state(
                                &mut sandbox,
                                route_identity_provenance,
                            );
                            state_only.push(sandbox);
                        }
                    }
                }
                let row_ids = rows.iter().map(|row| row.id.clone()).collect::<Vec<_>>();
                let publications = crate::db::sandbox_state_publication_metadata(db, &row_ids)
                    .map_err(|error| {
                        tracing::warn!(%error, "failed to load sandbox publication metadata");
                        Status::internal(format!("load sandbox publication metadata: {error}"))
                    })?;
                let state_changed_at = crate::db::sandbox_state_changed_at(db, &row_ids)
                    .map_err(|error| Status::internal(format!("load state change times: {error}")))?;
                let recovery_observations = crate::db::quarantine_recovery_observations(db, &row_ids)
                    .map_err(|error| {
                        tracing::warn!(%error, "failed to load quarantine recovery observations");
                        Status::internal(format!("load quarantine recovery observations: {error}"))
                    })?;
                Ok((
                    rows,
                    missing_ids,
                    publications,
                    recovery_observations,
                    state_only,
                    route_identity_provenance,
                    state_changed_at,
                    raw_budget,
                    metadata_budget,
                ))
        })
        .await?;
        let mut route_identity_provenance = route_identity_provenance;
        let sandbox_results = stream::iter(rows.into_iter().map(|sandbox| {
            let sandbox_id = sandbox.id.clone();
            let status = sandbox.state.to_string();
            let metadata = publication_metadata.remove(&sandbox.id);
            let state_changed_at = state_changed_at.remove(&sandbox.id);
            let recovery_observation = recovery_observations.remove(&sandbox.id);
            let (route_identity_provenance, route_provenance_degraded) =
                match route_identity_provenance.remove(&sandbox.id) {
                    Some(Ok(provenance)) => (provenance, false),
                    Some(Err(provenance_error)) => {
                        tracing::warn!(
                            sandbox_id = %sandbox.id,
                            %provenance_error,
                            "omitting malformed route provenance from sandbox state"
                        );
                        (crate::db::ActivationRouteProvenance::default(), true)
                    }
                    None => (crate::db::ActivationRouteProvenance::default(), false),
                };
            (
                sandbox_id,
                status,
                sandbox,
                metadata,
                state_changed_at,
                recovery_observation,
                route_identity_provenance,
                route_provenance_degraded,
            )
        }))
        .map(
            |(
                sandbox_id,
                status,
                sandbox,
                metadata,
                state_changed_at,
                recovery_observation,
                route_identity_provenance,
                route_provenance_degraded,
            )| async move {
                (
                    sandbox_id,
                    status,
                    route_identity_provenance,
                    route_provenance_degraded,
                    self.build_sandbox_info(
                        sandbox,
                        metadata,
                        state_changed_at,
                        recovery_observation,
                    )
                    .await,
                )
            },
        )
        .buffered(STATE_INFO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        sandboxes.reserve(sandbox_results.len());
        for (sandbox_id, status, route_identity_provenance, route_provenance_degraded, result) in
            sandbox_results
        {
            match result {
                Ok(mut sandbox) => {
                    sandbox.state_degraded |= route_provenance_degraded;
                    apply_route_identity_provenance_to_state(
                        &mut sandbox,
                        route_identity_provenance,
                    );
                    sandboxes.push(sandbox);
                }
                Err(error) => {
                    let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::StateOnly;
                    pplx_space_metrics::sandbox::publication::degraded(
                        pplx_space_metrics::sandbox::publication::StatePublicationComponent::Worker,
                        pplx_space_metrics::sandbox::publication::StatePublicationField::CoreRow,
                        disposition,
                    );
                    tracing::warn!(%sandbox_id, %error, %disposition, "publishing lifecycle without unpublishable sandbox fields");
                    let mut sandbox = SandboxInfo {
                        lifecycle: Some(
                            crate::proto::SandboxLifecycleState::from_label(&status)
                                .unwrap_or(crate::proto::SandboxLifecycleState::Unspecified)
                                as i32,
                        ),
                        sandbox_id,
                        state_degraded: true,
                        ..SandboxInfo::default()
                    };
                    apply_route_identity_provenance_to_state(
                        &mut sandbox,
                        route_identity_provenance,
                    );
                    sandboxes.push(sandbox);
                }
            }
        }

        Ok((
            StateResponse {
                sandboxes,
                missing_ids,
            },
            raw_budget,
            metadata_budget,
        ))
    }

    pub(super) async fn get_sandbox_health(
        &self,
        request: Request<GetSandboxHealthRequest>,
    ) -> Result<Response<GetSandboxHealthResponse>, Status> {
        self.authorize_peer(&request, "GetSandboxHealth", ALLOW_SANDBOX_OBSERVER)?;
        let ids = canonical_batched_sandbox_ids(&[request.get_ref().sandbox_id.clone()])?;
        let sandbox_id = ids
            .into_iter()
            .next()
            .ok_or_else(|| Status::invalid_argument("sandbox_id is required"))?;
        if let Some(health) = self.watchdog.get_composed_health(&sandbox_id).await {
            return Ok(Response::new(health));
        }
        self.lookup_sandbox(&sandbox_id)?;
        Ok(Response::new(GetSandboxHealthResponse {
            sandbox_id,
            status: crate::proto::SandboxHealthStatus::Unspecified as i32,
            evaluated_at: None,
            degraded_components: Vec::new(),
            components: Vec::new(),
        }))
    }

    pub(super) async fn build_sandbox_info(
        &self,
        sandbox: crate::db::SandboxRow,
        raw_publication_metadata: Option<crate::db::RawSandboxStatePublicationMetadataRead>,
        state_changed_at: Option<String>,
        raw_recovery_observation: Option<crate::db::RawQuarantineRecoveryObservationRead>,
    ) -> Result<SandboxInfo, Status> {
        let id = &sandbox.id;

        // Determine status: check if FC process is alive, fall back to DB state
        let pid_alive = is_sandbox_running(id);
        let pid = if pid_alive { sandbox.pid } else { None };
        let health = self.watchdog.get_health(id).await;
        let observed_state = if pid_alive {
            if sandbox.state.is_transient() || sandbox.state == SandboxState::Quarantined {
                sandbox_status_from_runtime_observation(sandbox.state, pid_alive, None)
            } else {
                sandbox_status_from_runtime_observation(
                    sandbox.state,
                    pid_alive,
                    health
                        .as_ref()
                        .and_then(crate::watchdog::SandboxHealth::cached_firecracker_state),
                )
            }
        } else {
            sandbox_status_from_runtime_observation(sandbox.state, pid_alive, None)
        };

        // Read cached guest uptime from DB
        let uptime = sandbox.guest_uptime_secs.map(format_uptime);

        let firecracker_alive = health
            .as_ref()
            .map(|health| health.firecracker_alive)
            .unwrap_or(pid_alive);
        let spaced_host_alive = health
            .as_ref()
            .map(|health| health.spaced_host_alive)
            .unwrap_or_else(|| sidecar_alive(id, "space-spaced-host-svc"));
        let netgtw_alive = health
            .as_ref()
            .map(|health| health.netgtw_alive)
            .unwrap_or_else(|| sidecar_alive(id, "space-netgtw-svc"));
        let vsock_raw_ping_ok = health
            .as_ref()
            .is_some_and(|health| health.vsock_raw_ping_ok);
        let last_guest_ready_at = health
            .as_ref()
            .and_then(|health| health.last_guest_ready_at.as_ref())
            .and_then(system_time_to_proto);
        let last_guest_heartbeat_at = health
            .as_ref()
            .and_then(|health| health.last_guest_heartbeat_at.as_ref())
            .and_then(system_time_to_proto);
        let network_tx_bytes = health
            .as_ref()
            .map(|health| health.network_tx_bytes)
            .unwrap_or(sandbox.network_tx_bytes);
        let network_rx_bytes = health
            .as_ref()
            .map(|health| health.network_rx_bytes)
            .unwrap_or(sandbox.network_rx_bytes);
        let spaced_version = health
            .as_ref()
            .and_then(|health| health.spaced_version.as_ref().map(ToString::to_string));
        let health_status = health.as_ref().map_or(
            crate::proto::SandboxHealthStatus::Unspecified as i32,
            crate::watchdog::SandboxHealth::status_proto,
        );
        let health_observed_at = health
            .as_ref()
            .and_then(crate::watchdog::SandboxHealth::observed_at_proto);
        // `Some("")` means the gateway answered a stats probe without a sha —
        // a binary predating sha reporting, definitively stale. `None` means
        // no successful probe yet, so staleness stays absent (unknown) rather
        // than guessed.
        let probed_netgtw_git_sha = health
            .as_ref()
            .and_then(|health| health.netgtw_git_sha.clone());
        // Stale = the gateway is running a different build than this
        // sandbox-manager (they ship in the same node image); an empty
        // reported sha never equals a real one, so pre-reporting binaries
        // classify stale.
        let netgtw_stale = probed_netgtw_git_sha
            .as_deref()
            .map(|sha| sha != pplx_space_observability::git_sha());
        // The wire field carries only a real sha; "answered but no sha" is
        // expressed as stale-with-absent-sha.
        let netgtw_git_sha = probed_netgtw_git_sha.filter(|sha| !sha.is_empty());
        let (raw_publication_metadata, metadata_was_present) = match raw_publication_metadata {
            Some(Ok(metadata)) => (Some(metadata), true),
            Some(Err(error)) => {
                let disposition = state_publication_metadata_disposition(sandbox.state);
                pplx_space_metrics::sandbox::publication::degraded(
                    pplx_space_metrics::sandbox::publication::StatePublicationComponent::Worker,
                    pplx_space_metrics::sandbox::publication::StatePublicationField::CoreRow,
                    disposition,
                );
                tracing::warn!(
                    sandbox_id = id,
                    %error,
                    %disposition,
                    "degraded unreadable sandbox state publication metadata"
                );
                (None, true)
            }
            None => (None, false),
        };
        let publication_metadata = raw_publication_metadata
            .map(|metadata| metadata.decode(sandbox.state))
            .transpose()
            .unwrap_or_else(|error| {
                let field = state_publication_metric_field(error.field());
                let disposition = state_publication_metadata_disposition(sandbox.state);
                pplx_space_metrics::sandbox::publication::degraded(
                    pplx_space_metrics::sandbox::publication::StatePublicationComponent::Worker,
                    field,
                    disposition,
                );
                tracing::warn!(
                    sandbox_id = id,
                    field = %error.field(),
                    %error,
                    %disposition,
                    "degraded malformed sandbox state publication metadata"
                );
                None
            });
        if sandbox.state == SandboxState::Quarantined && !metadata_was_present {
            let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::FallbackQuarantined;
            pplx_space_metrics::sandbox::publication::degraded(
                pplx_space_metrics::sandbox::publication::StatePublicationComponent::Worker,
                pplx_space_metrics::sandbox::publication::StatePublicationField::QuarantineReasonCode,
                disposition,
            );
            tracing::warn!(
                sandbox_id = id,
                %disposition,
                "quarantined sandbox is missing usable quarantine metadata; publishing fallback reason"
            );
        }
        let (quarantine_reason_code, transition_receipt) = match publication_metadata {
            Some(crate::db::SandboxStatePublicationMetadata::TransitionReceipt(receipt)) => (
                None,
                Some(crate::proto::TransitionReceipt {
                    id: receipt.id().to_string(),
                    kind: receipt.kind().to_string(),
                    target_state: receipt.target_state().to_string(),
                    execution_generation: receipt.execution_generation().get(),
                    reason_code: receipt
                        .reason_code()
                        .map(|reason| reason.as_str().to_owned())
                        .unwrap_or_default(),
                    activation_attempt_id: receipt.activation_attempt_id().map(|id| id.to_string()),
                }),
            ),
            Some(crate::db::SandboxStatePublicationMetadata::Quarantined { reason_code }) => {
                (Some(reason_code.as_str().to_owned()), None)
            }
            None if sandbox.state == SandboxState::Quarantined => (
                Some(SandboxQuarantineReasonCode::NodePublishedQuarantined.to_string()),
                None,
            ),
            None => (None, None),
        };
        let quarantine_recovery = match raw_recovery_observation {
            None => None,
            Some(observation) => {
                let observation = build_quarantine_recovery_observation(observation);
                match observation {
                    Ok(observation) => Some(observation),
                    Err(error) => {
                        let disposition = pplx_space_metrics::sandbox::publication::StatePublicationDisposition::OmitQuarantineRecoveryObservation;
                        pplx_space_metrics::sandbox::publication::degraded(
                                pplx_space_metrics::sandbox::publication::StatePublicationComponent::Worker,
                                pplx_space_metrics::sandbox::publication::StatePublicationField::QuarantineRecoveryObservation,
                                disposition,
                            );
                        tracing::warn!(
                            sandbox_id = id,
                            %error,
                            %disposition,
                            "omitting malformed quarantine recovery observation from sandbox state"
                        );
                        Some(crate::proto::QuarantineRecoveryObservation {
                            degraded: true,
                            ..Default::default()
                        })
                    }
                }
            }
        };
        Ok(SandboxInfo {
            sandbox_id: sandbox.id.clone(),
            template: Some(sandbox.template.clone()),
            lifecycle: Some(crate::proto::SandboxLifecycleState::from(observed_state) as i32),
            pid,
            namespace: live_sandbox_namespace(pid_alive, &sandbox.uuid8),
            uid: sandbox.uid,
            slot: sandbox.slot,
            guest_cid: sandbox.guest_cid,
            created_at: Some(sandbox.created_at.clone()),
            runtime: Some(sandbox.runtime.clone()),
            owner: Some(sandbox.user.to_string()),
            snapshots_enabled: sandbox.snapshots_enabled,
            pool_template: sandbox.pool_template.clone(),
            auto_lifecycle: Some(policy_to_proto(&sandbox.auto_lifecycle)),
            auto_lifecycle_status: Some(status_to_proto(
                &self.auto_lifecycle_runtime_state(&sandbox).public_status(),
            )),
            failure_reason: sandbox.failure_reason.clone(),
            firecracker_alive,
            spaced_host_alive,
            netgtw_alive,
            vsock_raw_ping_ok,
            last_guest_ready_at,
            last_guest_heartbeat_at,
            network_tx_bytes: Some(network_tx_bytes),
            network_rx_bytes: Some(network_rx_bytes),
            network_policy_generation: sandbox.network_policy_generation,
            applied_network_policy_generation: sandbox.applied_network_policy_generation,
            applied_network_policy_source: sandbox.applied_network_policy_source.as_proto(),
            network_policy_authority: sandbox.network_policy_authority.as_proto_i32(),
            auto_lifecycle_authority: sandbox.auto_lifecycle_authority.as_proto_i32(),
            current_local_artifact_id: sandbox.current_local_artifact_id.map(|id| id.to_string()),
            current_local_artifact_kind: sandbox.current_local_artifact_kind.map(
                |kind| match kind {
                    pplx_space_common_core::types::ArtifactKind::Disk => {
                        crate::proto::ArtifactKind::Disk as i32
                    }
                    pplx_space_common_core::types::ArtifactKind::Full => {
                        crate::proto::ArtifactKind::Full as i32
                    }
                },
            ),
            current_local_artifact_origin: sandbox.current_local_artifact_origin.map(|origin| {
                match origin {
                    pplx_space_common_core::types::ArtifactOrigin::Stop => {
                        crate::proto::ArtifactOrigin::Stop as i32
                    }
                    pplx_space_common_core::types::ArtifactOrigin::Suspend => {
                        crate::proto::ArtifactOrigin::Suspend as i32
                    }
                    pplx_space_common_core::types::ArtifactOrigin::Snapshot => {
                        crate::proto::ArtifactOrigin::Snapshot as i32
                    }
                    pplx_space_common_core::types::ArtifactOrigin::Fork => {
                        crate::proto::ArtifactOrigin::Fork as i32
                    }
                }
            }),
            execution: sandbox
                .execution_metadata
                .as_ref()
                .map(|execution| execution.into()),
            spaced_version,
            netgtw_git_sha,
            netgtw_stale,
            quarantine_reason_code,
            transition_receipt,
            execution_generation: sandbox
                .space_api_route_identity
                .execution_generation
                .map(|generation| generation.get()),
            pod_uid: sandbox
                .space_api_route_identity
                .pod_uid
                .as_ref()
                .map(ToString::to_string),
            activation_attempt_id: sandbox
                .space_api_route_identity
                .activation_attempt_id
                .map(|attempt| attempt.to_string()),
            state_changed_at: state_changed_at.as_deref().and_then(sqlite_timestamp),
            state_degraded: false,
            health_status,
            health_observed_at,
            uptime,
            quarantine_recovery,
            ..Default::default()
        })
    }

    pub(super) async fn get_env(
        &self,
        request: Request<GetEnvRequest>,
    ) -> Result<Response<GetEnvResponse>, Status> {
        self.authorize_peer(&request, "GetEnv", ALLOW_SANDBOX_OPERATOR)?;
        let sandbox_id = &request.get_ref().sandbox_id;
        let sandbox = self.lookup_sandbox(sandbox_id)?;
        Ok(Response::new(GetEnvResponse {
            env: sandbox.process_env.into_iter().collect(),
        }))
    }

    pub(super) async fn create_snapshot(
        &self,
        request: Request<crate::proto::CreateSnapshotRequest>,
    ) -> Result<Response<crate::proto::CreateSnapshotResponse>, Status> {
        self.authorize_peer(&request, "CreateSnapshot", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.into_inner();
        let artifact_kind = match snapshot_type_from_proto(req.r#type)? {
            crate::proto::SnapshotType::Disk => pplx_space_common_core::types::ArtifactKind::Disk,
            _ => pplx_space_common_core::types::ArtifactKind::Full,
        };
        if let Some(crate::proto::create_snapshot_source::Source::DurableArtifact(source)) = req
            .source
            .as_ref()
            .and_then(|source| source.source.as_ref())
        {
            return self
                .create_snapshot_from_durable_artifact(&req, source, artifact_kind)
                .await;
        }

        let sandbox = self.lookup_sandbox(&req.sandbox_id)?;
        let (_wake_guard, _artifact_guard) = self
            .auto_lifecycle
            .artifact_publication_locks(&sandbox.id, artifact_kind)
            .await;
        let sandbox = self.lookup_sandbox(&sandbox.id)?;
        self.ensure_not_pool_sandbox_for_snapshots(&sandbox, "create snapshot")?;
        self.ensure_no_plaintext_pool_cow_for_snapshots(&sandbox, "create snapshot")?;
        let id = &sandbox.id;

        if req.creation_identity.is_some() || req.source.is_some() {
            return self
                .create_operation_snapshot(&req, &sandbox, artifact_kind)
                .await;
        }

        // Determine snapshot type (default to full for backward compat)
        let snap_type = match artifact_kind {
            pplx_space_common_core::types::ArtifactKind::Disk => "disk",
            pplx_space_common_core::types::ArtifactKind::Full => "full",
        };

        // For full snapshots, validate state transition
        if snap_type == "full" {
            self.ensure_snapshot_restore_supported("create_full_snapshot")?;
            sandbox
                .state
                .transition(SandboxState::Checkpointing)
                .map_err(Status::from)?;
        }

        // Generate snapshot name with f-/d- prefix (required by list/rollback/export).
        // Labels are NOT embedded in the name — they go in the sidecar JSON only.
        // This preserves the {prefix}-{tier}-{timestamp} format that node-manager parses.
        let type_prefix = if snap_type == "full" { "f" } else { "d" };
        let now = crate::snapshot_scheduler::snapshot_timestamp(jiff::Timestamp::now());
        let snapshot_name = format!("{type_prefix}-manual-{now}");
        validate_snapshot_name(&snapshot_name)?;
        let typed_snapshot_name =
            pplx_space_common_core::types::SnapshotName::parse(snapshot_name.clone()).map_err(
                |error| Status::internal(format!("generated snapshot name is invalid: {error}")),
            )?;
        let local_artifact_id = LocalArtifactId::new();
        let label = req.label.clone().unwrap_or_default();

        let checkpoint_completion = if snap_type == "full" {
            let ctx = crate::snapshot_scheduler::CheckpointContext {
                db: self.db.clone(),
                auto_lifecycle: self.auto_lifecycle.clone(),
                health_lifecycle: self.watchdog.lifecycle_epochs(),
            };

            let completion = crate::snapshot_scheduler::do_checkpoint(&ctx, id, &snapshot_name)
                .await
                .map_err(Status::internal)?;
            // A skipped capture wrote nothing; publishing below would create
            // a Ready artifact row pointing at files that do not exist.
            if let Some(reason) = completion.skip_reason() {
                return Err(Status::failed_precondition(format!(
                    "snapshot capture skipped: {reason}"
                )));
            }
            Some(completion)
        } else {
            // Disk snapshot: btrfs subvolume snapshot via node-manager
            let mut node_client = pplx_space_node_mgr_client::connect()
                .await
                .map_err(|e| Status::internal(format!("connect to node-manager: {e}")))?;
            node_client
                .create_btrfs_snapshot(
                    pplx_space_node_mgr_client::proto::CreateBtrfsSnapshotRequest {
                        sandbox_id: id.to_string(),
                        snapshot_name: snapshot_name.clone(),
                        writable: false,
                        execution_fence: sandbox.node_execution_fence(),
                    },
                )
                .await
                .map_err(|e| Status::internal(format!("disk snapshot: {e}")))?;
            None
        };

        // Write label to sidecar JSON if provided (full snapshots only — disk snapshots
        // are read-only btrfs subvolumes, sidecar writes would fail silently)
        if !label.is_empty() && snap_type == "full" {
            let snap_dir = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
                id,
                &snapshot_name,
            );
            let sidecar_path = format!("{snap_dir}/snapshot.json");
            // Read existing sidecar (written by do_checkpoint for full snapshots)
            let mut meta: serde_json::Value = std::fs::read_to_string(&sidecar_path)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_else(|| serde_json::json!({}));
            if let Some(meta) = meta.as_object_mut() {
                meta.insert("label".to_string(), serde_json::json!(label));
            }
            let _ = std::fs::write(
                &sidecar_path,
                serde_json::to_string_pretty(&meta).unwrap_or_default(),
            );
        }

        // Return the snapshot name as the ID — this is what delete/rollback/export use
        // Return the actual type created (not the request type, which may be UNSPECIFIED)
        let actual_type = if snap_type == "full" {
            crate::proto::SnapshotType::Full
        } else {
            crate::proto::SnapshotType::Disk
        };
        let local_path =
            pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(id, &snapshot_name);
        let publish_result = {
            let db = self.db_conn()?;
            crate::db::publish_named_local_artifact(
                &db,
                id,
                local_artifact_id,
                artifact_kind,
                &typed_snapshot_name,
                &local_path,
            )
        };
        if let Err(error) = publish_result {
            match pplx_space_node_mgr_client::connect().await {
                Ok(mut node_client) => {
                    if let Err(cleanup_error) = node_client
                        .delete_btrfs_snapshot(
                            pplx_space_node_mgr_client::proto::DeleteBtrfsSnapshotRequest {
                                sandbox_id: id.to_string(),
                                snapshot_name: snapshot_name.clone(),
                            },
                        )
                        .await
                    {
                        tracing::warn!(
                            sandbox_id = %id,
                            snapshot_name,
                            error = %cleanup_error,
                            "failed to remove manual snapshot after registry persist failure"
                        );
                    }
                }
                Err(cleanup_error) => {
                    tracing::warn!(
                        sandbox_id = %id,
                        snapshot_name,
                        error = %cleanup_error,
                        "failed to connect to node-manager to remove manual snapshot after registry persist failure"
                    );
                }
            }
            return Err(Status::internal(format!("persist local artifact: {error}")));
        }
        emit_sandbox_event(
            id,
            SandboxEventType::LocalArtifactPublished,
            SandboxEventSource::SbxManager,
            SandboxEventDetails::default()
                .local_artifact_id(local_artifact_id.to_string())
                .artifact_kind(artifact_kind.as_str())
                .artifact_origin("snapshot")
                .snapshot_id(snapshot_name.clone()),
        );
        if let Some(completion) = checkpoint_completion {
            completion.into_lifecycle_result().map_err(|error| {
                Status::internal(format!(
                    "snapshot {snapshot_name} was published, but lifecycle restoration failed: {error}"
                ))
            })?;
        }
        Ok(Response::new(crate::proto::CreateSnapshotResponse {
            snapshot_name: snapshot_name.clone(),
            r#type: actual_type.into(),
            published_artifact: Some(crate::proto::PublishedArtifact {
                local_artifact_id: local_artifact_id.to_string(),
                artifact_kind: if artifact_kind == pplx_space_common_core::types::ArtifactKind::Full
                {
                    crate::proto::ArtifactKind::Full as i32
                } else {
                    crate::proto::ArtifactKind::Disk as i32
                },
                origin: crate::proto::ArtifactOrigin::Snapshot as i32,
                snapshot_name: Some(snapshot_name),
            }),
            published_durable_artifact: None,
        }))
    }

    async fn create_operation_snapshot(
        &self,
        req: &crate::proto::CreateSnapshotRequest,
        initial_sandbox: &crate::db::SandboxRow,
        artifact_kind: pplx_space_common_core::types::ArtifactKind,
    ) -> Result<Response<crate::proto::CreateSnapshotResponse>, Status> {
        if artifact_kind != pplx_space_common_core::types::ArtifactKind::Disk {
            return Err(Status::unimplemented(
                "operation-safe full snapshots are reserved for rich runtime capture",
            ));
        }
        let identity = req.creation_identity.as_ref().ok_or_else(|| {
            Status::invalid_argument("creation_identity and source must be supplied together")
        })?;
        let precondition = current_snapshot_precondition(req)?;
        let request_id = Uuid::parse_str(&identity.request_id)
            .map_err(|error| Status::invalid_argument(format!("invalid request_id: {error}")))?;
        let expected_generation = pplx_space_common_core::types::ExecutionGeneration::new(
            precondition.execution_generation,
        )
        .map_err(|error| {
            Status::invalid_argument(format!("invalid execution_generation: {error}"))
        })?;
        let expected_state =
            match crate::proto::SnapshotSourceLifecycle::try_from(precondition.lifecycle) {
                Ok(crate::proto::SnapshotSourceLifecycle::Started) => SandboxState::Started,
                Ok(crate::proto::SnapshotSourceLifecycle::Paused) => SandboxState::Paused,
                _ => {
                    return Err(Status::invalid_argument(
                        "current_sandbox.lifecycle must be STARTED or PAUSED",
                    ));
                }
            };
        let snapshot_name = disk_operation_snapshot_name(request_id)?;
        let sandbox_id = initial_sandbox.id.clone();
        let local_artifact_id = disk_operation_snapshot_artifact_id(
            &sandbox_id,
            request_id,
            expected_generation,
            expected_state,
        );
        let paths = disk_operation_snapshot_paths(&sandbox_id, &snapshot_name);

        let existing = {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
            crate::db::get_named_local_artifact(&db, &sandbox_id, &snapshot_name)
                .map_err(|error| Status::internal(format!("read operation snapshot: {error}")))?
        };
        if let Some(artifact) = existing.as_ref()
            && !disk_operation_snapshot_artifact_matches(
                artifact,
                local_artifact_id,
                &sandbox_id,
                &snapshot_name,
                &paths,
            )
        {
            return Err(Status::already_exists(
                "operation snapshot identity conflicts with an existing artifact",
            ));
        }

        let publish_artifact = |local_artifact_id, local_path: &str| -> Result<(), Status> {
            let db = self
                .db
                .lock()
                .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
            crate::db::publish_named_local_artifact(
                &db,
                &sandbox_id,
                local_artifact_id,
                pplx_space_common_core::types::ArtifactKind::Disk,
                &snapshot_name,
                local_path,
            )
            .map_err(|error| Status::internal(format!("record operation snapshot: {error}")))
        };

        let remove_unbound_snapshot = match operation_snapshot_next_step(
            existing.is_some(),
            existing.as_ref().map_or_else(
                || paths.any_exists(),
                |artifact| std::path::Path::new(&artifact.local_path).exists(),
            ),
        ) {
            OperationSnapshotNextStep::Replay => {
                let artifact = existing.ok_or_else(|| {
                    Status::internal("operation snapshot replay has no artifact metadata")
                })?;
                return Ok(Response::new(disk_operation_snapshot_response(
                    artifact.local_artifact_id,
                    &snapshot_name,
                )));
            }
            OperationSnapshotNextStep::CreateSnapshot => false,
            OperationSnapshotNextStep::RejectMissingArtifactPath => {
                return Err(Status::failed_precondition(
                    "operation snapshot metadata exists but its immutable bytes are missing",
                ));
            }
            OperationSnapshotNextStep::RemoveUnboundSnapshot => true,
        };

        let validate_source = || -> Result<OperationSnapshotPlan, Status> {
            let sandbox = self.lookup_sandbox(&sandbox_id)?;
            self.ensure_not_pool_sandbox_for_snapshots(&sandbox, "create operation snapshot")?;
            self.ensure_no_plaintext_pool_cow_for_snapshots(&sandbox, "create operation snapshot")?;
            if sandbox.volume_encryption {
                return Err(Status::failed_precondition(
                    "encrypted sandboxes do not support operation snapshots",
                ));
            }
            if sandbox.space_api_route_identity.execution_generation != Some(expected_generation) {
                return Err(Status::aborted(
                    "source execution generation changed before snapshot creation",
                ));
            }
            if sandbox.state != expected_state {
                return Err(Status::aborted(format!(
                    "source lifecycle changed from {expected_state} to {} before snapshot creation",
                    sandbox.state
                )));
            }
            OperationSnapshotPlan::from_state(sandbox.state).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "source lifecycle {} does not support operation snapshots",
                    sandbox.state
                ))
            })
        };
        let plan = validate_source()?;

        if plan.flush_guest_filesystems() {
            crate::guest_filesystem_flush::flush_guest_filesystems_via_spaced_host(&sandbox_id)
                .await
                .map_err(|error| {
                    Status::unavailable(format!("flush guest filesystems: {error}"))
                })?;
        }

        let mut node_client = pplx_space_node_mgr_client::connect()
            .await
            .map_err(|error| Status::unavailable(format!("connect to node-manager: {error}")))?;
        node_client
            .sync_guest(pplx_space_node_mgr_client::proto::SyncGuestRequest {
                sandbox_id: sandbox_id.clone(),
            })
            .await
            .map_err(|error| Status::unavailable(format!("sync host rootfs: {error}")))?;
        let current_plan = validate_source()?;
        if current_plan != plan {
            return Err(Status::aborted(format!(
                "source lifecycle changed from {} to {} before snapshot creation",
                plan.source_state(),
                current_plan.source_state()
            )));
        }

        if remove_unbound_snapshot {
            let interrupted_attempt = Status::unavailable(
                "remove unbound operation snapshot left by an interrupted creation attempt",
            );
            remove_disk_operation_snapshot_attempt(
                &mut node_client,
                &sandbox_id,
                &snapshot_name,
                &paths,
                &interrupted_attempt,
            )
            .await?;
        }

        for attempt in 0..2 {
            let create_result = node_client
                .create_btrfs_snapshot(
                    pplx_space_node_mgr_client::proto::CreateBtrfsSnapshotRequest {
                        sandbox_id: sandbox_id.clone(),
                        snapshot_name: snapshot_name.to_string(),
                        writable: false,
                        execution_fence: initial_sandbox.node_execution_fence(),
                    },
                )
                .await;
            match operation_snapshot_create_disposition(
                create_result.as_ref().err().map(tonic::Status::code),
                create_result.is_err() && std::path::Path::new(&paths.capture).exists(),
            ) {
                OperationSnapshotCreateDisposition::Created => break,
                OperationSnapshotCreateDisposition::RemoveUnboundSnapshot => {
                    let error = create_result.err().ok_or_else(|| {
                        Status::internal("unbound operation snapshot lacks creation error")
                    })?;
                    let lost_response = Status::unavailable(format!(
                        "operation snapshot creation failed after bytes appeared: {error}"
                    ));
                    remove_disk_operation_snapshot_attempt(
                        &mut node_client,
                        &sandbox_id,
                        &snapshot_name,
                        &paths,
                        &lost_response,
                    )
                    .await?;
                    if attempt == 1 {
                        return Err(lost_response);
                    }
                    validate_source()?;
                }
                OperationSnapshotCreateDisposition::Failed => {
                    let error = create_result.err().ok_or_else(|| {
                        Status::internal("failed operation snapshot lacks creation error")
                    })?;
                    return Err(Status::internal(format!(
                        "create operation snapshot: {error}"
                    )));
                }
            }
        }

        if !std::path::Path::new(&paths.capture).exists() {
            return Err(Status::unavailable(
                "operation snapshot was not visible after creation",
            ));
        }
        let post_create_validation = validate_source();
        match operation_snapshot_post_create_disposition(post_create_validation.is_ok()) {
            OperationSnapshotPostCreateDisposition::Publish => {}
            OperationSnapshotPostCreateDisposition::RemoveCreatedSnapshot => {
                let validation_error = post_create_validation.err().ok_or_else(|| {
                    Status::internal("operation snapshot cleanup lacks validation error")
                })?;
                remove_disk_operation_snapshot_attempt(
                    &mut node_client,
                    &sandbox_id,
                    &snapshot_name,
                    &paths,
                    &validation_error,
                )
                .await?;
                return Err(validation_error);
            }
        }

        let encode_result = match initial_sandbox.template_hash.as_deref() {
            Some(content_hash) => crate::btrfs_via_nodemgr::encode_snapshot_rootfs_send(
                &sandbox_id,
                &initial_sandbox.template,
                content_hash,
                snapshot_name.as_str(),
                &paths.artifact,
            )
            .await
            .map_err(crate::btrfs_via_nodemgr::BtrfsViaNodemgrError::into_status),
            None => Err(Status::failed_precondition(
                "btrfs-send operation snapshot requires sandbox.template_hash",
            )),
        };
        let local_path = match encode_result {
            Ok(stream_bytes) => {
                tracing::info!(
                    sandbox_id,
                    snapshot_name = %snapshot_name,
                    stream_bytes,
                    "operation snapshot encoded as parent-relative btrfs stream"
                );
                let encoded_capture = Status::internal(
                    "remove exact operation capture after successful btrfs encoding",
                );
                if let Err(cleanup_error) = remove_created_disk_operation_snapshot(
                    &mut node_client,
                    &sandbox_id,
                    &snapshot_name,
                    &encoded_capture,
                )
                .await
                {
                    return match remove_disk_operation_snapshot_attempt(
                        &mut node_client,
                        &sandbox_id,
                        &snapshot_name,
                        &paths,
                        &cleanup_error,
                    )
                    .await
                    {
                        Ok(()) => Err(cleanup_error),
                        Err(combined_error) => Err(combined_error),
                    };
                }
                paths.artifact.as_str()
            }
            Err(encode_error) => {
                return match remove_disk_operation_snapshot_attempt(
                    &mut node_client,
                    &sandbox_id,
                    &snapshot_name,
                    &paths,
                    &encode_error,
                )
                .await
                {
                    Ok(()) => Err(encode_error),
                    Err(cleanup_error) => Err(cleanup_error),
                };
            }
        };

        if let Err(publish_error) = publish_artifact(local_artifact_id, local_path) {
            let existing = {
                let db = self
                    .db
                    .lock()
                    .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
                crate::db::get_named_local_artifact(&db, &sandbox_id, &snapshot_name).map_err(
                    |error| {
                        Status::internal(format!("read concurrent operation snapshot: {error}"))
                    },
                )?
            };
            let matching_artifact_was_published = existing.as_ref().is_some_and(|artifact| {
                disk_operation_snapshot_artifact_matches(
                    artifact,
                    local_artifact_id,
                    &sandbox_id,
                    &snapshot_name,
                    &paths,
                ) && artifact.local_path == local_path
            });
            match operation_snapshot_publish_failure_disposition(matching_artifact_was_published) {
                OperationSnapshotPublishFailureDisposition::ReplayConcurrentPublication => {}
                OperationSnapshotPublishFailureDisposition::RemoveCreatedSnapshot => {
                    remove_disk_operation_snapshot_attempt(
                        &mut node_client,
                        &sandbox_id,
                        &snapshot_name,
                        &paths,
                        &publish_error,
                    )
                    .await?;
                    return Err(publish_error);
                }
            }
        }
        emit_disk_operation_snapshot_published(&sandbox_id, local_artifact_id, &snapshot_name);

        Ok(Response::new(disk_operation_snapshot_response(
            local_artifact_id,
            &snapshot_name,
        )))
    }

    async fn create_snapshot_from_durable_artifact(
        &self,
        req: &crate::proto::CreateSnapshotRequest,
        source: &crate::proto::DurableArtifactSnapshotSource,
        artifact_kind: ArtifactKind,
    ) -> Result<Response<crate::proto::CreateSnapshotResponse>, Status> {
        if artifact_kind != ArtifactKind::Disk {
            return Err(Status::unimplemented(
                "durable full snapshot publication is reserved for rich runtime capture",
            ));
        }
        let identity = req.creation_identity.as_ref().ok_or_else(|| {
            Status::invalid_argument(
                "creation_identity is required for a durable artifact snapshot source",
            )
        })?;
        let operation_id = Uuid::parse_str(&identity.request_id)
            .map_err(|error| Status::invalid_argument(format!("invalid request_id: {error}")))?;
        let source_sandbox_id = req
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox_id: {error}")))?;
        let destination_sandbox_id =
            source
                .destination_sandbox_id
                .parse::<SandboxId>()
                .map_err(|error| {
                    Status::invalid_argument(format!("invalid destination_sandbox_id: {error}"))
                })?;
        let source_storage =
            crate::snapshot_upload::SnapshotUploadConfig::from_uri(&source.source_storage_uri)
                .map_err(|_| Status::invalid_argument("invalid source_storage_uri"))?;
        let destination_storage =
            crate::snapshot_upload::SnapshotUploadConfig::from_uri(&source.destination_storage_uri)
                .map_err(|_| Status::invalid_argument("invalid destination_storage_uri"))?;
        let destination_origin = durable_copy_destination_origin(source.destination_origin)?;
        if !source_storage.same_store(&destination_storage) {
            return Err(Status::invalid_argument(
                "snapshot source and destination must use the same object store",
            ));
        }
        if source_storage.key_prefix.trim_matches('/')
            == destination_storage.key_prefix.trim_matches('/')
        {
            return Err(Status::invalid_argument(
                "snapshot source and destination prefixes must differ",
            ));
        }

        crate::service::fork_artifact::copy_objects(
            self,
            crate::service::fork_artifact::CopyObjectsRequest {
                operation_id,
                source_sandbox_id,
                target_sandbox_id: destination_sandbox_id,
                source: &source_storage,
                destination: &destination_storage,
                destination_storage_uri: source.destination_storage_uri.clone(),
                destination_origin,
            },
        )
        .await
    }

    pub(super) async fn create_template_base_snapshot(
        &self,
        request: Request<CreateTemplateBaseSnapshotRequest>,
    ) -> Result<Response<CreateTemplateBaseSnapshotResponse>, Status> {
        self.authorize_peer(
            &request,
            "CreateTemplateBaseSnapshot",
            ALLOW_SANDBOX_ADMIN_OR_TMPLMGR,
        )?;
        let req = request.get_ref();
        if req.vcpu == 0 {
            return Err(Status::invalid_argument("vcpu must be greater than 0"));
        }
        if req.memory_mib == 0 {
            return Err(Status::invalid_argument(
                "memory_mib must be greater than 0",
            ));
        }

        let sandbox_id = self.lookup_sandbox(&req.sandbox_id)?.id;
        let wake_lock = self.auto_lifecycle.wake_lock(&sandbox_id);
        let _guard = wake_lock.lock().await;

        let (sandbox, content_hash) = {
            let db = self.db_conn()?;
            let sandbox = crate::db::get_sandbox(&db, &sandbox_id)
                .map_err(|e| Status::internal(format!("db error: {e}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox not found: {sandbox_id}")))?;
            if !Self::sandbox_owned_by_template_manager(&sandbox) {
                return Err(Status::permission_denied(
                    "template base snapshots require a template-owned sandbox",
                ));
            }
            if sandbox.state != SandboxState::Paused {
                return Err(Status::failed_precondition(format!(
                    "template base snapshot requires paused sandbox, got {}",
                    sandbox.state.as_str()
                )));
            }
            let content_hash = sandbox.template_hash.clone().ok_or_else(|| {
                Status::failed_precondition("template-owned sandbox is missing content_hash")
            })?;
            validate_path_component("template", &sandbox.template)?;
            validate_path_component("content_hash", &content_hash)?;
            sandbox
                .state
                .transition(SandboxState::Checkpointing)
                .map_err(Status::from)?;
            require_state_write(
                crate::db::update_state(&db, &sandbox.id, SandboxState::Checkpointing),
                "begin template checkpoint",
            )?;
            (sandbox, content_hash)
        };

        let staging_dir = format!(
            "{CACHE_DIR}/{}/template-snapshot-staging/{}vcpu-{}mib",
            sandbox.id, req.vcpu, req.memory_mib
        );
        let checkpoint_started = Instant::now();
        tracing::debug!(
            sandbox_id = %sandbox.id,
            template = %sandbox.template,
            content_hash = %content_hash,
            vcpu = req.vcpu,
            requested_memory_mib = req.memory_mib,
            staging_dir = %staging_dir,
            "template base snapshot checkpoint started"
        );

        let snapshot_result: Result<(String, u32), Status> = async {
            let staging_dir_path = PathBuf::from(&staging_dir);
            let _ = std::fs::remove_dir_all(&staging_dir_path);
            std::fs::create_dir_all(&staging_dir_path).map_err(|e| {
                Status::internal(format!("create template snapshot staging dir: {e}"))
            })?;

            let mut node_client = connect_node()
                .await
                .map_err(|e| Status::internal(format!("cannot connect to node-manager: {e}")))?;
            let phase = Instant::now();
            node_client
                .create_snapshot(CreateSnapshotRequest {
                    execution_fence: sandbox.node_execution_fence(),
                    sandbox_id: sandbox.id.clone(),
                    snapshot_path: "vmstate".to_string(),
                    mem_file_path: "memory".to_string(),
                    dest_dir: Some(staging_dir.clone()),
                    snapshot_volume_mount_dir: None,
                })
                .await
                .map_err(|error| {
                    Status::internal(format!("create template base snapshot: {error}"))
                })?;
            tracing::debug!(
                sandbox_id = %sandbox.id,
                template = %sandbox.template,
                elapsed_ms = phase.elapsed().as_millis(),
                "template base snapshot node export complete"
            );

            std::fs::rename(
                staging_dir_path.join("memory"),
                staging_dir_path.join("snapshot.base"),
            )
            .map_err(|e| Status::internal(format!("rename memory -> snapshot.base: {e}")))?;
            std::fs::rename(
                staging_dir_path.join("vmstate"),
                staging_dir_path.join("vmstate.base"),
            )
            .map_err(|e| Status::internal(format!("rename vmstate -> vmstate.base: {e}")))?;

            let snapshot_execution = sandbox_snapshot_execution_metadata(&sandbox.id)
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            let runtime_metadata =
                serde_json::to_vec_pretty(&snapshot_execution).map_err(|error| {
                    Status::internal(format!(
                        "serialize template snapshot runtime metadata: {error}"
                    ))
                })?;
            std::fs::write(staging_dir_path.join("metadata.json"), runtime_metadata).map_err(
                |error| {
                    Status::internal(format!("write template snapshot runtime metadata: {error}"))
                },
            )?;

            let snapshot_memory_mib =
                snapshot_file_memory_mib(&staging_dir_path.join("snapshot.base"))?;
            let snapshot_dir = pplx_space_common_core::host::linux::paths::template_snapshot_dir(
                &sandbox.template,
                &content_hash,
                req.vcpu,
                snapshot_memory_mib,
            );
            let snapshot_dir_path = PathBuf::from(&snapshot_dir);

            let hash_dir = staging_dir_path.clone();
            let phase = Instant::now();
            let hash_result =
                tokio::task::spawn_blocking(move || compute_template_base_snapshot_hash(&hash_dir))
                    .await
                    .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
                    .map_err(|e| {
                        Status::internal(format!("compute template snapshot hash: {e}"))
                    })?;
            let snapshot_hash = hash_result.hash;
            tracing::debug!(
                sandbox_id = %sandbox.id,
                template = %sandbox.template,
                bytes_hashed = hash_result.bytes_hashed,
                elapsed_ms = phase.elapsed().as_millis(),
                "template base snapshot hash phase complete"
            );
            std::fs::write(staging_dir_path.join("snapshot_hash"), &snapshot_hash)
                .map_err(|e| Status::internal(format!("write snapshot_hash: {e}")))?;

            publish_template_base_snapshot_dir(&staging_dir_path, &snapshot_dir_path)?;
            Ok((snapshot_hash, snapshot_memory_mib))
        }
        .await;

        match snapshot_result {
            Ok((snapshot_hash, snapshot_memory_mib)) => {
                {
                    let db = self.db_conn()?;
                    require_state_write(
                        crate::db::update_state(&db, &sandbox.id, SandboxState::Paused),
                        "complete template checkpoint",
                    )?;
                }
                tracing::debug!(
                    sandbox_id = %sandbox.id,
                    template = %sandbox.template,
                    content_hash = %content_hash,
                    vcpu = req.vcpu,
                    requested_memory_mib = req.memory_mib,
                    snapshot_memory_mib,
                    snapshot_hash = %snapshot_hash,
                    elapsed_ms = checkpoint_started.elapsed().as_millis(),
                    "template base snapshot checkpoint complete"
                );
                Ok(Response::new(CreateTemplateBaseSnapshotResponse {
                    snapshot_hash,
                    memory_mib: snapshot_memory_mib,
                }))
            }
            Err(status) => {
                tracing::warn!(
                    sandbox_id = %sandbox.id,
                    template = %sandbox.template,
                    content_hash = %content_hash,
                    vcpu = req.vcpu,
                    memory_mib = req.memory_mib,
                    code = ?status.code(),
                    error = %status.message(),
                    elapsed_ms = checkpoint_started.elapsed().as_millis(),
                    "template base snapshot checkpoint failed"
                );
                let _ = std::fs::remove_dir_all(&staging_dir);
                update_state_best_effort(&self.db, &sandbox.id, SandboxState::Paused);
                Err(status)
            }
        }
    }
}

fn durable_copy_destination_origin(value: i32) -> Result<ArtifactOrigin, Status> {
    match crate::proto::ArtifactOrigin::try_from(value) {
        Ok(crate::proto::ArtifactOrigin::Fork) => Ok(ArtifactOrigin::Fork),
        Ok(crate::proto::ArtifactOrigin::Snapshot) => Err(Status::invalid_argument(
            "durable artifact copies cannot publish snapshot origin without a snapshot name",
        )),
        _ => Err(Status::invalid_argument("destination_origin must be FORK")),
    }
}

#[cfg(test)]
mod operation_snapshot_tests;

#[cfg(test)]
mod tests;

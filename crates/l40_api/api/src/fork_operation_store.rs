use pplx_space_accounts::{OrgId, UserId};
use pplx_space_snapshot_model::SnapshotId;

use std::time::Duration;

use pplx_space_public_api::{OperationId, OperationState};
use pplx_space_public_operation_store as operation_store;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::LocalArtifactId;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    fork_operations::{
        ForkPayloadDecodeError, ForkSandboxPayload, ForkSandboxPayloadV1, ForkSourceLifecycle,
        decode_fork_payload,
    },
    models::User,
    public_operations::{
        ClaimedPublicOperation, PublicOperationStore, UpdateClaimedPublicOperationPayload,
    },
};

#[derive(Clone)]
pub struct ForkOperationStore {
    public_operations: PublicOperationStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableForkArtifact {
    pub snapshot_id: SnapshotId,
    pub storage_location_id: Uuid,
    pub storage_uri: String,
}

impl From<pplx_space_snapshot_store::disk_artifact::ReadyDiskArtifact> for DurableForkArtifact {
    fn from(artifact: pplx_space_snapshot_store::disk_artifact::ReadyDiskArtifact) -> Self {
        Self {
            snapshot_id: artifact.snapshot_id,
            storage_location_id: artifact.storage_location_id,
            storage_uri: artifact.storage_connection_string,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureLeaderObservation {
    Missing,
    IncompatibleSource,
    Pending,
    TerminalWithoutArtifact,
    ArtifactReady {
        snapshot_id: SnapshotId,
        storage_location_id: Uuid,
    },
}

pub struct PrepareCopiedArtifact<'a> {
    pub snapshot_id: SnapshotId,
    pub storage_location_id: Uuid,
    pub owner_sandbox_id: SandboxId,
    pub org_id: OrgId,
    pub destination_storage_uri: &'a str,
}

#[derive(Debug, thiserror::Error)]
pub enum ForkOperationStoreError {
    #[error("fork operation storage failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("fork workflow serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("public operation storage failed: {0}")]
    PublicOperation(#[from] crate::public_operations::PublicOperationStoreError),
    #[error("fork operation claim was superseded")]
    ClaimSuperseded,
    #[error("capture leader selection requires a frozen source")]
    InvalidCaptureWorkflow,
    #[error("capture leader has an invalid workflow payload: {0}")]
    InvalidLeaderPayload(#[from] ForkPayloadDecodeError),
    #[error("capture leader has an invalid operation state: {0}")]
    InvalidLeaderState(String),
}

impl ForkOperationStore {
    pub fn new(pool: PgPool) -> Self {
        Self {
            public_operations: PublicOperationStore::new(pool),
        }
    }

    pub(crate) fn pool(&self) -> &PgPool {
        self.public_operations.pool()
    }

    pub async fn persist_payload(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &ForkSandboxPayloadV1,
    ) -> Result<(), ForkOperationStoreError> {
        let payload_json = serde_json::to_value(ForkSandboxPayload::v1(payload.clone()))?;
        let updated = self
            .public_operations
            .update_claimed_payload(UpdateClaimedPublicOperationPayload {
                claim_token: claim_token.to_string(),
                operation_id: operation.id,
                claim_generation: operation.claim_generation,
                payload_json,
            })
            .await?;
        if updated {
            Ok(())
        } else {
            Err(ForkOperationStoreError::ClaimSuperseded)
        }
    }

    pub async fn renew_claimed_operation(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> Result<bool, ForkOperationStoreError> {
        self.public_operations
            .renew_claimed_operation(claim_token, operation)
            .await
            .map_err(Into::into)
    }

    pub async fn scrub_claimed_fork_federated_service_grant(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
    ) -> Result<bool, ForkOperationStoreError> {
        self.public_operations
            .scrub_claimed_fork_federated_service_grant(claim_token, operation)
            .await
            .map_err(Into::into)
    }

    pub async fn suspended_source_artifact(
        &self,
        source_sandbox_id: SandboxId,
    ) -> Result<Option<(SnapshotId, Uuid)>, ForkOperationStoreError> {
        let artifact = pplx_space_snapshot_store::fork_source::suspended_artifact(
            self.pool(),
            source_sandbox_id,
        )
        .await?;
        Ok(artifact.map(|artifact| (artifact.snapshot_id, artifact.storage_location_id)))
    }

    pub async fn select_capture_leader(
        &self,
        claim_token: &str,
        operation: &ClaimedPublicOperation,
        payload: &mut ForkSandboxPayloadV1,
    ) -> Result<OperationId, ForkOperationStoreError> {
        let generation = payload
            .source_execution_generation()
            .ok_or(ForkOperationStoreError::InvalidCaptureWorkflow)?;
        let lifecycle = payload
            .source_lifecycle()
            .ok_or(ForkOperationStoreError::InvalidCaptureWorkflow)?;
        let mut tx = self.pool().begin().await?;
        operation_store::operations::lock_fork_capture_leader(
            &mut tx,
            &payload.source_sandbox_id().to_string(),
            generation,
        )
        .await?;
        // Forks of one source name it by either id form; see
        // `fork_operations::fork_operation_target_id`.
        let rows = operation_store::operations::active_forks_of_source(
            &mut *tx,
            operation.org_id,
            payload.source_sandbox_id(),
        )
        .await?;
        let mut candidates = Vec::new();
        for operation_store::operations::ActiveForkRow {
            operation_id: candidate_id,
            payload_json: candidate_json,
        } in rows
        {
            let Ok(candidate) = decode_fork_payload(candidate_json.0) else {
                continue;
            };
            let candidate_id = OperationId::from_uuid(candidate_id);
            if is_capture_candidate(&candidate, generation, lifecycle) {
                candidates.push((candidate_id, candidate));
            }
        }
        let leader = choose_capture_leader(
            candidates
                .iter()
                .map(|(candidate_id, candidate)| (*candidate_id, candidate.capture_operation_id())),
            operation.id,
        );
        if leader != operation.id
            && let Some((_, leader_payload)) = candidates
                .iter_mut()
                .find(|(candidate_id, _)| *candidate_id == leader)
            && leader_payload.capture_operation_id().is_none()
        {
            leader_payload.set_capture_operation_id(leader);
            let leader_payload_json =
                serde_json::to_value(ForkSandboxPayload::v1(leader_payload.clone()))?;
            let updated = operation_store::operations::set_active_fork_payload(
                &mut *tx,
                operation.org_id,
                leader,
                leader_payload_json,
            )
            .await?;
            if !updated {
                return Err(ForkOperationStoreError::ClaimSuperseded);
            }
        }
        payload.set_capture_operation_id(leader);
        let payload_json = serde_json::to_value(ForkSandboxPayload::v1(payload.clone()))?;
        let updated = self
            .public_operations
            .update_claimed_payload_in_transaction(
                &mut tx,
                UpdateClaimedPublicOperationPayload {
                    claim_token: claim_token.to_string(),
                    operation_id: operation.id,
                    claim_generation: operation.claim_generation,
                    payload_json,
                },
            )
            .await?;
        if !updated {
            return Err(ForkOperationStoreError::ClaimSuperseded);
        }
        tx.commit().await?;
        Ok(leader)
    }

    pub async fn observe_capture_leader(
        &self,
        org_id: OrgId,
        leader_id: OperationId,
        expected_generation: i64,
        expected_lifecycle: ForkSourceLifecycle,
    ) -> Result<CaptureLeaderObservation, ForkOperationStoreError> {
        let row = operation_store::operations::fork_state(self.pool(), org_id, leader_id).await?;
        let Some(operation_store::operations::ForkStateRow {
            state,
            payload_json: leader_json,
        }) = row
        else {
            return Ok(CaptureLeaderObservation::Missing);
        };
        let leader = decode_fork_payload(leader_json.0)?;
        if !capture_leader_matches_source(&leader, expected_generation, expected_lifecycle) {
            return Ok(CaptureLeaderObservation::IncompatibleSource);
        }
        if let (Some(snapshot_id), Some(storage_location_id)) = (
            leader.fork_snapshot_id(),
            leader.fork_snapshot_storage_location_id(),
        ) {
            return Ok(CaptureLeaderObservation::ArtifactReady {
                snapshot_id,
                storage_location_id,
            });
        }
        capture_leader_state_observation(state)
    }

    pub async fn source_storage_uri(
        &self,
        snapshot_id: SnapshotId,
        storage_location_id: Uuid,
    ) -> Result<Option<String>, ForkOperationStoreError> {
        pplx_space_snapshot_store::storage_location::live_connection_string(
            self.pool(),
            snapshot_id,
            storage_location_id,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn prepare_copied_artifact(
        &self,
        input: PrepareCopiedArtifact<'_>,
    ) -> Result<(), ForkOperationStoreError> {
        let mut tx = self.pool().begin().await?;
        pplx_space_snapshot_store::disk_artifact::insert_creating_fork_copy(
            &mut *tx,
            input.snapshot_id,
            input.owner_sandbox_id,
            input.org_id,
        )
        .await?;
        pplx_space_snapshot_store::disk_artifact::attach_fork_copy_location(
            &mut *tx,
            input.storage_location_id,
            input.snapshot_id,
            input.destination_storage_uri,
            input.org_id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_copied_artifact_ready(
        &self,
        snapshot_id: SnapshotId,
        owner_sandbox_id: SandboxId,
        size_bytes: u64,
    ) -> Result<bool, ForkOperationStoreError> {
        pplx_space_snapshot_store::disk_artifact::mark_fork_copy_ready(
            self.pool(),
            snapshot_id,
            owner_sandbox_id,
            i64::try_from(size_bytes).unwrap_or(i64::MAX),
        )
        .await
        .map_err(Into::into)
    }

    pub async fn rebind_copied_artifact_owner(
        &self,
        snapshot_id: SnapshotId,
        source_sandbox_id: SandboxId,
        target_sandbox_id: SandboxId,
    ) -> Result<bool, ForkOperationStoreError> {
        pplx_space_snapshot_store::disk_artifact::rebind_fork_copy_owner(
            self.pool(),
            snapshot_id,
            source_sandbox_id,
            target_sandbox_id,
        )
        .await
        .map_err(Into::into)
    }

    pub async fn retire_copied_artifact(
        &self,
        snapshot_id: SnapshotId,
        storage_location_id: Uuid,
    ) -> Result<(), ForkOperationStoreError> {
        let mut tx = self.pool().begin().await?;
        pplx_space_snapshot_store::disk_artifact::orphan_fork_copy_location(
            &mut *tx,
            snapshot_id,
            storage_location_id,
        )
        .await?;
        pplx_space_snapshot_store::disk_artifact::delete_unlocated_fork_copy(&mut *tx, snapshot_id)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn user(&self, user_id: UserId) -> Result<Option<User>, ForkOperationStoreError> {
        pplx_space_accounts_store::users::get_user(self.pool(), user_id)
            .await
            .map_err(Into::into)
    }

    pub async fn remaining_deadline(
        &self,
        operation_id: OperationId,
        hard_deadline: Duration,
    ) -> Result<Option<Duration>, ForkOperationStoreError> {
        self.public_operations
            .remaining_deadline(operation_id, hard_deadline)
            .await
            .map_err(Into::into)
    }

    pub async fn ready_artifact_for_local_id(
        &self,
        sandbox_id: SandboxId,
        local_artifact_id: LocalArtifactId,
    ) -> Result<Option<DurableForkArtifact>, ForkOperationStoreError> {
        let artifact = pplx_space_snapshot_store::disk_artifact::ready_for_local_id(
            self.pool(),
            sandbox_id,
            local_artifact_id,
        )
        .await?;
        Ok(artifact.map(DurableForkArtifact::from))
    }

    pub async fn ready_artifact(
        &self,
        snapshot_id: SnapshotId,
        storage_location_id: Uuid,
    ) -> Result<Option<DurableForkArtifact>, ForkOperationStoreError> {
        let artifact = pplx_space_snapshot_store::disk_artifact::ready_at_location(
            self.pool(),
            snapshot_id,
            storage_location_id,
        )
        .await?;
        Ok(artifact.map(DurableForkArtifact::from))
    }
}

fn choose_capture_leader(
    candidates: impl IntoIterator<Item = (OperationId, Option<OperationId>)>,
    fallback: OperationId,
) -> OperationId {
    let candidates = candidates.into_iter().collect::<Vec<_>>();
    candidates
        .iter()
        .find_map(|(candidate_id, selected)| {
            (*selected == Some(*candidate_id)).then_some(*candidate_id)
        })
        .or_else(|| {
            candidates
                .iter()
                .find_map(|(candidate_id, selected)| selected.is_none().then_some(*candidate_id))
        })
        .unwrap_or(fallback)
}

fn is_capture_candidate(
    candidate: &ForkSandboxPayloadV1,
    generation: i64,
    lifecycle: ForkSourceLifecycle,
) -> bool {
    candidate.source_execution_generation() == Some(generation)
        && candidate.source_lifecycle() == Some(lifecycle)
        && lifecycle != ForkSourceLifecycle::Suspended
        && candidate.fork_snapshot_id().is_none()
        && candidate.fork_snapshot_storage_location_id().is_none()
}

fn capture_leader_matches_source(
    leader: &ForkSandboxPayloadV1,
    generation: i64,
    lifecycle: ForkSourceLifecycle,
) -> bool {
    leader.source_execution_generation() == Some(generation)
        && leader.source_lifecycle() == Some(lifecycle)
}

fn is_capture_pending_state(state: OperationState) -> bool {
    matches!(state, OperationState::Queued | OperationState::Running)
}

/// Classify a capture leader that has not published an artifact by its stored
/// `state`.
///
/// Strict on purpose: `OperationState` is an open vocabulary whose lenient
/// `FromStr` decodes an unknown persisted value as `Other(_)`. Such a state is
/// neither pending nor terminal *to this build*; reading it as terminal would
/// let a follower clear a live leader's claim and start a duplicate capture,
/// so it is an error and the follower backs off.
fn capture_leader_state_observation(
    state: String,
) -> Result<CaptureLeaderObservation, ForkOperationStoreError> {
    let state = OperationState::parse_known(&state)
        .map_err(|_| ForkOperationStoreError::InvalidLeaderState(state))?;
    Ok(if is_capture_pending_state(state) {
        CaptureLeaderObservation::Pending
    } else {
        CaptureLeaderObservation::TerminalWithoutArtifact
    })
}

#[cfg(test)]
mod tests;

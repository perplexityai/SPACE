use pplx_space_common_core::types::{OrgId, SnapshotId, UserId};

use std::time::Duration;

use pplx_space_common_core::{
    sql,
    types::{LocalArtifactId, SandboxId},
};
use pplx_space_public_api::{OperationId, OperationState};
use pplx_space_sandbox_portability::no_newer_full_snapshot;
use sqlx::{PgPool, types::Json};
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
    #[error("read fork artifact identities: {0}")]
    ReadDigests(#[from] pplx_space_snapshot_store::GetArtifactDigestsError),
    #[error("record fork artifact identities: {0}")]
    RecordDigests(#[from] pplx_space_snapshot_store::RecordArtifactDigestsError),
    #[error("a legacy replay cannot replace an authenticated fork manifest")]
    LegacyReplay,
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
        let no_newer = no_newer_full_snapshot("s", "sn");
        let query = format!(
            "SELECT sn.id, ssl.id
             FROM sandboxes s
             JOIN snapshots sn
               ON sn.sandbox_id = s.id
              AND sn.artifact_kind = 'full'
              AND sn.type = 'full'
              AND sn.state = 'ready'
              AND (sn.artifact_origin IS NULL OR sn.artifact_origin = 'suspend')
             JOIN LATERAL (
                 SELECT id
                 FROM snapshot_storage_locations
                 WHERE snapshot_id = sn.id AND orphaned_at IS NULL
                 ORDER BY created_at DESC, id DESC
                 LIMIT 1
             ) ssl ON TRUE
             WHERE s.id = $1
               AND s.lifecycle = 'suspended'
               AND (
                   (s.current_local_artifact_id IS NOT NULL
                    AND s.current_local_artifact_origin = 'suspend'
                    AND sn.local_artifact_id = s.current_local_artifact_id)
                   OR (s.current_local_artifact_id IS NULL
                       AND (s.placement_changed_at IS NULL
                            OR sn.timestamp >= s.placement_changed_at))
               )
               AND {no_newer}
             ORDER BY sn.timestamp DESC, sn.id DESC
             LIMIT 1"
        );
        sqlx::query_as(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(source_sandbox_id.as_uuid())
            .fetch_optional(self.pool())
            .await
            .map_err(Into::into)
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
        sqlx::query(sql!(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, $2))"
        ))
        .bind(payload.source_sandbox_id().to_string())
        .bind(generation)
        .execute(&mut *tx)
        .await?;
        let rows = sqlx::query_as::<_, (Uuid, Json<serde_json::Value>)>(sql!(
            "SELECT operation_id, payload_json
             FROM public_operations
             WHERE org_id = $1
               AND kind = 'fork_sandbox'
               AND target_id = $2
               AND state IN ($3, $4)
             ORDER BY created_at, operation_id"
        ))
        .bind(operation.org_id)
        .bind(payload.source_sandbox_id().to_string())
        .bind(OperationState::Queued.as_str())
        .bind(OperationState::Running.as_str())
        .fetch_all(&mut *tx)
        .await?;
        let mut candidates = Vec::new();
        for (candidate_id, candidate_json) in rows {
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
            let updated = sqlx::query(sql!(
                "UPDATE public_operations
                 SET payload_json = $3, updated_at = NOW()
                 WHERE org_id = $1
                   AND operation_id = $2
                   AND kind = 'fork_sandbox'
                   AND state IN ('queued', 'running')"
            ))
            .bind(operation.org_id)
            .bind(leader.as_uuid())
            .bind(Json(leader_payload_json))
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if updated != 1 {
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
        let row = sqlx::query_as::<_, (String, Json<serde_json::Value>)>(sql!(
            "SELECT state, payload_json
             FROM public_operations
             WHERE org_id = $1 AND operation_id = $2 AND kind = 'fork_sandbox'"
        ))
        .bind(org_id)
        .bind(leader_id.as_uuid())
        .fetch_optional(self.pool())
        .await?;
        let Some((state, leader_json)) = row else {
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
        sqlx::query_scalar(sql!(
            "SELECT storage_connection_string
             FROM snapshot_storage_locations
             WHERE id = $1 AND snapshot_id = $2 AND orphaned_at IS NULL"
        ))
        .bind(storage_location_id)
        .bind(snapshot_id)
        .fetch_optional(self.pool())
        .await
        .map_err(Into::into)
    }

    pub async fn prepare_copied_artifact(
        &self,
        input: PrepareCopiedArtifact<'_>,
    ) -> Result<(), ForkOperationStoreError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query(sql!(
            "INSERT INTO snapshots (
                 id, sandbox_id, type, state, timestamp, artifact_kind, artifact_origin, org_id
             )
             VALUES ($1, $2, 'disk', 'creating', NOW(), 'disk', 'fork', $3)
             ON CONFLICT (id) DO NOTHING"
        ))
        .bind(input.snapshot_id)
        .bind(input.owner_sandbox_id.as_uuid())
        .bind(input.org_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (
                 id, snapshot_id, storage_connection_string, org_id
             ) VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE
             SET snapshot_id = EXCLUDED.snapshot_id,
                 storage_connection_string = EXCLUDED.storage_connection_string,
                 org_id = EXCLUDED.org_id,
                 orphaned_at = NULL
             WHERE snapshot_storage_locations.snapshot_id IS NULL
               AND snapshot_storage_locations.orphaned_at IS NOT NULL"
        ))
        .bind(input.storage_location_id)
        .bind(input.snapshot_id)
        .bind(input.destination_storage_uri)
        .bind(input.org_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn mark_copied_artifact_ready(
        &self,
        snapshot_id: SnapshotId,
        owner_sandbox_id: SandboxId,
        size_bytes: u64,
        digests: &[pplx_space_snapshot_store::SnapshotArtifactDigest],
    ) -> Result<bool, ForkOperationStoreError> {
        let mut tx = self.pool().begin().await?;
        let updated = sqlx::query(sql!(
            "UPDATE snapshots
             SET state = 'ready', size_bytes = $2
             WHERE id = $1
               AND sandbox_id = $3
               AND type = 'disk'
               AND artifact_kind = 'disk'
               AND artifact_origin = 'fork'
               AND state IN ('creating', 'ready')"
        ))
        .bind(snapshot_id)
        .bind(i64::try_from(size_bytes).unwrap_or(i64::MAX))
        .bind(owner_sandbox_id.as_uuid())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated == 0 {
            return Ok(false);
        }
        if digests.is_empty()
            && !pplx_space_snapshot_store::artifact_digests(&mut *tx, snapshot_id)
                .await?
                .is_empty()
        {
            return Err(ForkOperationStoreError::LegacyReplay);
        }
        pplx_space_snapshot_store::record_artifact_digests(&mut tx, snapshot_id, digests).await?;
        tx.commit().await?;
        Ok(true)
    }

    pub async fn rebind_copied_artifact_owner(
        &self,
        snapshot_id: SnapshotId,
        source_sandbox_id: SandboxId,
        target_sandbox_id: SandboxId,
    ) -> Result<bool, ForkOperationStoreError> {
        let updated = sqlx::query(sql!(
            "UPDATE snapshots
             SET sandbox_id = $3
             WHERE id = $1
               AND sandbox_id IN ($2, $3)
               AND type = 'disk'
               AND artifact_kind = 'disk'
               AND artifact_origin = 'fork'
               AND state = 'ready'"
        ))
        .bind(snapshot_id)
        .bind(source_sandbox_id.as_uuid())
        .bind(target_sandbox_id.as_uuid())
        .execute(self.pool())
        .await?
        .rows_affected();
        Ok(updated == 1)
    }

    pub async fn retire_copied_artifact(
        &self,
        snapshot_id: SnapshotId,
        storage_location_id: Uuid,
    ) -> Result<(), ForkOperationStoreError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query(sql!(
            "UPDATE snapshot_storage_locations
             SET snapshot_id = NULL, orphaned_at = NOW()
             WHERE id = $1 AND snapshot_id = $2 AND orphaned_at IS NULL"
        ))
        .bind(storage_location_id)
        .bind(snapshot_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(sql!(
            "DELETE FROM snapshots s
             WHERE s.id = $1
               AND s.artifact_origin = 'fork'
               AND NOT EXISTS (
                   SELECT 1 FROM snapshot_storage_locations ssl
                   WHERE ssl.snapshot_id = s.id AND ssl.orphaned_at IS NULL
               )"
        ))
        .bind(snapshot_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn user(&self, user_id: UserId) -> Result<Option<User>, ForkOperationStoreError> {
        sqlx::query_as(sql!("SELECT * FROM users WHERE id = $1"))
            .bind(user_id)
            .fetch_optional(self.pool())
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
        self.ready_artifact_query(
            sqlx::query_as(sql!(
                "SELECT sn.id, ssl.id, ssl.storage_connection_string
             FROM snapshots sn
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
             WHERE sn.sandbox_id = $1
               AND sn.local_artifact_id = $2
               AND sn.type = 'disk'
               AND sn.artifact_kind = 'disk'
               AND sn.state = 'ready'
               AND ssl.orphaned_at IS NULL
             ORDER BY ssl.created_at DESC
             LIMIT 1"
            ))
            .bind(sandbox_id.as_uuid())
            .bind(local_artifact_id.as_uuid()),
        )
        .await
    }

    pub async fn ready_artifact(
        &self,
        snapshot_id: SnapshotId,
        storage_location_id: Uuid,
    ) -> Result<Option<DurableForkArtifact>, ForkOperationStoreError> {
        self.ready_artifact_query(
            sqlx::query_as(sql!(
                "SELECT sn.id, ssl.id, ssl.storage_connection_string
             FROM snapshots sn
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
             WHERE sn.id = $1
               AND ssl.id = $2
               AND sn.type = 'disk'
               AND sn.artifact_kind = 'disk'
               AND sn.artifact_origin IN ('snapshot', 'fork')
               AND sn.state = 'ready'
               AND ssl.orphaned_at IS NULL"
            ))
            .bind(snapshot_id)
            .bind(storage_location_id),
        )
        .await
    }

    async fn ready_artifact_query<'q>(
        &self,
        query: sqlx::query::QueryAs<
            'q,
            sqlx::Postgres,
            (SnapshotId, Uuid, String),
            sqlx::postgres::PgArguments,
        >,
    ) -> Result<Option<DurableForkArtifact>, ForkOperationStoreError> {
        let row = query.fetch_optional(self.pool()).await?;
        Ok(row.map(
            |(snapshot_id, storage_location_id, storage_uri)| DurableForkArtifact {
                snapshot_id,
                storage_location_id,
                storage_uri,
            },
        ))
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

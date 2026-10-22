//! Explicit, bounded recovery authority. A grant proves an administrator's
//! approval of retained bytes, never their historical machine ownership.
use super::*;
use pplx_space_common_core::types::{SubstrateKind, UserId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type, Serialize, Deserialize)]
#[sqlx(transparent)]
#[serde(transparent)]
pub struct RecoveryGrantId(Uuid);
impl RecoveryGrantId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}
impl Default for RecoveryGrantId {
    fn default() -> Self {
        Self::new()
    }
}
impl std::fmt::Display for RecoveryGrantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for RecoveryGrantId {
    type Err = uuid::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Stable metadata proof excludes generated timestamps and measured sizes.
/// File proofs cover logical bytes read by volume-manager, not transport objects.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryManifest {
    /// The administrator has inspected a sealed full suspend and validated its
    /// files against this manifest. Issuance does not seal or repair a capture.
    pub validation: SealedValidation,
    pub execution: SnapshotExecutionMetadata,
    pub metadata_binding_digest: Digest,
    pub vcpu: i32,
    pub memory_mib: i32,
    pub template_id: TemplateName,
    pub content_hash: Option<String>,
    pub artifacts: Vec<SnapshotArtifactDigest>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedValidation {
    pub sealed_full_suspend: bool,
    pub evidence_reference: String,
}

impl RecoveryManifest {
    pub fn validate(&self) -> Result<(), RecoveryError> {
        if !self.validation.sealed_full_suspend
            || self.validation.evidence_reference.trim().is_empty()
            || self.validation.evidence_reference.len() > 2048
        {
            return Err(RecoveryError::Refused(
                "recovery requires an operator-validated, already sealed full suspend",
            ));
        }
        use SnapshotArtifactFile::*;
        let mut files: Vec<_> = self.artifacts.iter().map(|p| p.artifact).collect();
        files.sort();
        let valid = [
            vec![Rootfs, Memory, VmState],
            vec![EncryptedSnapshotVolume, EncryptedRootfsCow],
        ]
        .into_iter()
        .any(|mut set| {
            set.sort();
            set == files
        });
        if !valid
            || self
                .artifacts
                .iter()
                .any(|p| p.size_bytes == 0 || p.size_bytes > i64::MAX as u64)
        {
            return Err(RecoveryError::Refused(
                "recovery requires an exact, nonempty full-artifact file manifest",
            ));
        }
        if self.vcpu <= 0 || self.memory_mib <= 0 {
            return Err(RecoveryError::Refused(
                "recovery requires the captured VM resource shape",
            ));
        }
        self.total_bytes()?;
        Ok(())
    }
    pub fn total_bytes(&self) -> Result<i64, RecoveryError> {
        self.artifacts
            .iter()
            .try_fold(0_i64, |sum, p| {
                i64::try_from(p.size_bytes)
                    .ok()
                    .and_then(|n| sum.checked_add(n))
            })
            .ok_or(RecoveryError::Refused(
                "recovery manifest size exceeds catalog range",
            ))
    }
    pub fn matches(&self, proofs: &[SnapshotArtifactDigest], metadata: Option<Digest>) -> bool {
        let mut expected = self.artifacts.clone();
        expected.sort_by_key(|p| p.artifact);
        let mut actual = proofs.to_vec();
        actual.sort_by_key(|p| p.artifact);
        expected == actual && metadata == Some(self.metadata_binding_digest)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Insert(#[from] InsertSnapshotError),
    #[error(transparent)]
    Read(#[from] GetSnapshotError),
    #[error("{0}")]
    Refused(&'static str),
}

/// The administrator attests capture freshness, not historical node ownership.
/// The database additionally rejects times outside the known execution lifetime.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CaptureTimeEvidence {
    RecordedCapture { reference: String },
    OperatorAttestsNoNewerState { explanation: String },
}
impl CaptureTimeEvidence {
    fn validate(&self) -> Result<(), RecoveryError> {
        let detail = match self {
            Self::RecordedCapture { reference } => reference,
            Self::OperatorAttestsNoNewerState { explanation } => explanation,
        };
        if detail.trim().is_empty() || detail.len() > 2048 {
            return Err(RecoveryError::Refused(
                "capture evidence must contain 1 to 2048 bytes of explanation or reference",
            ));
        }
        Ok(())
    }
}

pub struct ApproveRecovery<'a> {
    pub id: RecoveryGrantId,
    pub sandbox_id: SandboxId,
    pub artifact_id: LocalArtifactId,
    pub generation: ExecutionGeneration,
    pub uploader_node_id: NodeId,
    pub uploader_node_uid: &'a K8sNodeUid,
    pub captured_at: OffsetDateTime,
    pub capture_evidence: &'a CaptureTimeEvidence,
    pub expires_at: OffsetDateTime,
    pub actor: UserId,
    pub reason: &'a str,
    pub manifest: &'a RecoveryManifest,
}

#[derive(Debug, FromRow)]
pub struct RecoveryGrant {
    pub id: RecoveryGrantId,
    pub sandbox_id: SandboxId,
    pub local_artifact_id: LocalArtifactId,
    pub execution_generation: i64,
    pub captured_at: OffsetDateTime,
    pub uploader_node_id: NodeId,
    pub uploader_node_uid: K8sNodeUid,
    pub manifest: Json<RecoveryManifest>,
    pub expected_state: Json<serde_json::Value>,
    pub active_attempt_id: Option<SnapshotId>,
    pub consumed_attempt_id: Option<SnapshotId>,
    pub usable: bool,
}

/// Caller must hold the sandbox row lock before acquiring grant or node locks.
/// The predicate is also checked at finalization, so approval is not a stale
/// authorization to restore across a successor execution.
async fn check_retained_state(
    conn: &mut sqlx::PgConnection,
    sandbox: SandboxId,
    generation: i64,
    artifact: LocalArtifactId,
    expected_state: Option<&serde_json::Value>,
) -> Result<serde_json::Value, RecoveryError> {
    let _sandbox: Option<SandboxId> =
        sqlx::query_scalar(sql!("SELECT id FROM sandboxes WHERE id=$1 FOR UPDATE"))
            .bind(sandbox)
            .fetch_optional(&mut *conn)
            .await?;
    let _executions:Vec<i64> = sqlx::query_scalar(sql!("SELECT generation FROM sandbox_k8s_executions WHERE sandbox_id=$1 AND generation>=$2 FOR SHARE"))
        .bind(sandbox).bind(generation).fetch_all(&mut *conn).await?;
    let row: Option<(Option<LocalArtifactId>,bool,serde_json::Value)> = sqlx::query_as(sql!(
        "SELECT current_local_artifact_id,
          active_node_id IS NULL AND lifecycle IN ('suspended','quarantined','stopped','failed')
          AND EXISTS(SELECT 1 FROM sandbox_k8s_executions e WHERE e.sandbox_id=s.id AND e.generation=$2)
          AND NOT EXISTS(SELECT 1 FROM sandbox_k8s_executions e WHERE e.sandbox_id=s.id AND
            (e.active OR (e.generation>$2 AND (e.pod_materialized_at IS NOT NULL OR NULLIF(BTRIM(e.pod_uid),'') IS NOT NULL))
             OR (e.generation=$2 AND e.state='replaced'))),
          jsonb_build_object('lifecycle',lifecycle,'placement',placement,'active_node_id',active_node_id,
            'target_node_id',target_node_id,'artifact_id',current_local_artifact_id,
            'artifact_kind',current_local_artifact_kind,'artifact_origin',current_local_artifact_origin,
            'placement_changed_at',placement_changed_at)
         FROM sandboxes s WHERE id=$1 FOR UPDATE"
    )).bind(sandbox).bind(generation).fetch_optional(&mut *conn).await?;
    let (pointer, eligible, witness) =
        row.ok_or(RecoveryError::Refused("recovery sandbox is absent"))?;
    if !eligible
        || pointer.is_some_and(|id| id != artifact)
        || expected_state.is_some_and(|expected| expected != &witness)
    {
        return Err(RecoveryError::Refused(
            "recovery state changed or its execution was superseded",
        ));
    }
    Ok(witness)
}

async fn check_uploader(
    conn: &mut sqlx::PgConnection,
    node: NodeId,
    uid: &K8sNodeUid,
) -> Result<(), RecoveryError> {
    let eligible: Option<bool> = sqlx::query_scalar(sql!(
        "SELECT n.role='worker' AND n.managed_by=$3 AND n.state<>'deleted' AND k.k8s_node_uid=$2
         FROM nodes n JOIN node_k8s k ON k.node_id=n.id WHERE n.id=$1 FOR SHARE OF n,k"
    ))
    .bind(node)
    .bind(uid)
    .bind(SubstrateKind::Kubernetes)
    .fetch_optional(conn)
    .await?;
    if eligible != Some(true) {
        return Err(RecoveryError::Refused(
            "recovery uploader is not the approved live node incarnation",
        ));
    }
    Ok(())
}

/// Admin route owns authentication. Actor and exact approval are durable in the
/// same transaction; no worker-facing RPC can create or extend this authority.
pub async fn approve(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: ApproveRecovery<'_>,
) -> Result<(), RecoveryError> {
    request.manifest.validate()?;
    request.capture_evidence.validate()?;
    if request.reason.trim().is_empty() || request.reason.len() > 2048 {
        return Err(RecoveryError::Refused(
            "an approval reason of at most 2048 bytes is required",
        ));
    }
    let witness = check_retained_state(
        tx,
        request.sandbox_id,
        request.generation.get(),
        request.artifact_id,
        None,
    )
    .await?;
    let within_execution: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS(SELECT 1 FROM sandbox_k8s_executions WHERE sandbox_id=$1 AND generation=$2
         AND created_at <= $3 AND $3 <= COALESCE(terminated_at,NOW()))"
    ))
    .bind(request.sandbox_id)
    .bind(request.generation.get())
    .bind(request.captured_at)
    .fetch_one(&mut **tx)
    .await?;
    if !within_execution {
        return Err(RecoveryError::Refused(
            "capture time is outside the approved execution lifetime",
        ));
    }
    // Serialize grant replacement through the sandbox, including expired grants
    // whose accepted uploads may still be running. Only one approval is live.
    let occupied: bool = sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM backup_recovery_grants WHERE sandbox_id=$1 AND local_artifact_id=$2 AND revoked_at IS NULL AND consumed_attempt_id IS NULL AND expires_at>clock_timestamp())"))
        .bind(request.sandbox_id).bind(request.artifact_id).fetch_one(&mut **tx).await?;
    if occupied {
        return Err(RecoveryError::Refused(
            "an unexpired recovery approval already exists; revoke it before replacing it",
        ));
    }
    check_uploader(tx, request.uploader_node_id, request.uploader_node_uid).await?;
    let inserted = sqlx::query(sql!(
        "INSERT INTO backup_recovery_grants(id,sandbox_id,local_artifact_id,execution_generation,uploader_node_id,uploader_node_uid,captured_at,expires_at,approved_by,approval_reason,manifest,expected_state,capture_evidence)
         SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
         WHERE $7<=NOW() AND $8>NOW() AND $8<=NOW()+INTERVAL '24 hours'"
    )).bind(request.id).bind(request.sandbox_id).bind(request.artifact_id).bind(request.generation.get())
        .bind(request.uploader_node_id).bind(request.uploader_node_uid).bind(request.captured_at).bind(request.expires_at)
        .bind(request.actor).bind(request.reason.trim()).bind(Json(request.manifest)).bind(Json(witness)).bind(Json(request.capture_evidence)).execute(&mut **tx).await?;
    if inserted.rows_affected() != 1 {
        return Err(RecoveryError::Refused(
            "capture must be in the past and approval must expire within 24 hours",
        ));
    }
    Ok(())
}

pub async fn revoke(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sandbox: SandboxId,
    id: RecoveryGrantId,
    actor: UserId,
    reason: &str,
) -> Result<bool, RecoveryError> {
    if reason.trim().is_empty() || reason.len() > 2048 {
        return Err(RecoveryError::Refused(
            "a revocation reason of at most 2048 bytes is required",
        ));
    }
    sqlx::query(sql!("SELECT id FROM sandboxes WHERE id=$1 FOR UPDATE"))
        .bind(sandbox)
        .fetch_optional(&mut **tx)
        .await?;
    let result = sqlx::query(sql!("UPDATE backup_recovery_grants SET revoked_at=NOW(),revoked_by=$3,revoke_reason=$4 WHERE id=$1 AND sandbox_id=$2 AND revoked_at IS NULL AND consumed_attempt_id IS NULL"))
        .bind(id).bind(sandbox).bind(actor).bind(reason.trim()).execute(&mut **tx).await?;
    Ok(result.rows_affected() == 1)
}

pub async fn for_artifact(
    conn: &mut sqlx::PgConnection,
    sandbox: SandboxId,
    artifact: LocalArtifactId,
) -> Result<Option<RecoveryGrant>, RecoveryError> {
    Ok(sqlx::query_as(sql!("SELECT id,sandbox_id,local_artifact_id,execution_generation,captured_at,uploader_node_id,uploader_node_uid,manifest,expected_state,active_attempt_id,consumed_attempt_id,
        revoked_at IS NULL AND expires_at>clock_timestamp() AND consumed_attempt_id IS NULL AS usable
        FROM backup_recovery_grants WHERE sandbox_id=$1 AND local_artifact_id=$2 AND revoked_at IS NULL AND consumed_attempt_id IS NULL AND expires_at>clock_timestamp() ORDER BY approved_at DESC,id DESC LIMIT 1 FOR UPDATE"))
        .bind(sandbox).bind(artifact).fetch_optional(conn).await?)
}

pub async fn for_attempt(
    conn: &mut sqlx::PgConnection,
    attempt: SnapshotId,
) -> Result<Option<RecoveryGrant>, RecoveryError> {
    Ok(sqlx::query_as(sql!("SELECT g.id,g.sandbox_id,g.local_artifact_id,g.execution_generation,g.captured_at,g.uploader_node_id,g.uploader_node_uid,g.manifest,g.expected_state,g.active_attempt_id,g.consumed_attempt_id,
        g.revoked_at IS NULL AND g.expires_at>clock_timestamp() AND g.consumed_attempt_id IS NULL AS usable
        FROM backup_recovery_grants g JOIN snapshots s ON s.recovery_grant_id=g.id WHERE s.id=$1 FOR UPDATE OF g"))
        .bind(attempt).fetch_optional(conn).await?)
}

impl RecoveryGrant {
    pub async fn validate_live(
        &self,
        conn: &mut sqlx::PgConnection,
        caller_uid: &K8sNodeUid,
    ) -> Result<(), RecoveryError> {
        if !self.usable {
            return Err(RecoveryError::Refused(
                "recovery approval is expired, revoked, or consumed",
            ));
        }
        if caller_uid != &self.uploader_node_uid {
            return Err(RecoveryError::Refused(
                "caller is not the approved recovery uploader",
            ));
        }
        check_retained_state(
            conn,
            self.sandbox_id,
            self.execution_generation,
            self.local_artifact_id,
            Some(&self.expected_state.0),
        )
        .await?;
        let newer: bool = sqlx::query_scalar(sql!("SELECT EXISTS(SELECT 1 FROM snapshots WHERE sandbox_id=$1 AND local_artifact_id IS DISTINCT FROM $2 AND timestamp>$3 AND state<>'deleting')"))
            .bind(self.sandbox_id).bind(self.local_artifact_id).bind(self.captured_at).fetch_one(&mut *conn).await?;
        if newer {
            return Err(RecoveryError::Refused(
                "newer captured state supersedes this recovery",
            ));
        }
        check_uploader(conn, self.uploader_node_id, caller_uid).await
    }
}

/// Only this entry point can attach grant provenance to a fresh attempt.
/// Ordinary retry admission and store retry validation exclude all such rows.
pub async fn register(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    grant: &RecoveryGrant,
    mut snapshot: NewSnapshot,
    caller_uid: &K8sNodeUid,
) -> Result<SnapshotRecord, RecoveryError> {
    if caller_uid != &grant.uploader_node_uid {
        return Err(RecoveryError::Refused(
            "caller is not the approved recovery uploader",
        ));
    }
    let artifact = snapshot
        .artifact
        .as_ref()
        .ok_or(RecoveryError::Refused("recovery attempt has no artifact"))?;
    if snapshot.sandbox_id != grant.sandbox_id
        || artifact.local_artifact_id() != Some(grant.local_artifact_id)
        || artifact.kind() != ArtifactKind::Full
        || artifact.origin() != ArtifactOrigin::Suspend
        || artifact.snapshot_name().is_some()
        || snapshot.execution_metadata.as_ref() != Some(&grant.manifest.execution)
    {
        return Err(RecoveryError::Refused(
            "recovery attempt differs from the approved artifact or execution metadata",
        ));
    }
    if let Some(active) = grant.active_attempt_id.filter(|id| *id != snapshot.id) {
        let failed: Option<bool> = sqlx::query_scalar(sql!(
            "SELECT state='failed' FROM snapshots WHERE id=$1 FOR SHARE"
        ))
        .bind(active)
        .fetch_optional(&mut **tx)
        .await?;
        if failed != Some(true) {
            return Err(RecoveryError::Refused(
                "recovery already has an active attempt",
            ));
        }
    }
    snapshot.execution_metadata = Some(grant.manifest.execution.clone());
    snapshot.vcpu = Some(grant.manifest.vcpu);
    snapshot.memory_mib = Some(grant.manifest.memory_mib);
    snapshot.template_id = Some(grant.manifest.template_id.clone());
    snapshot.content_hash = grant
        .manifest
        .content_hash
        .clone()
        .map(SnapshotContentHash::from_stored);
    snapshot.source_node_id = Some(grant.uploader_node_id);
    if grant.consumed_attempt_id == Some(snapshot.id) {
        let persisted = get(&mut **tx, snapshot.id)
            .await?
            .ok_or(RecoveryError::Refused(
                "consumed recovery attempt is absent",
            ))?;
        if persisted.state().is_ready()
            && persisted.recovery_grant_id() == Some(grant.id)
            && same_snapshot_identity(&persisted, &snapshot)
            && persisted.source_node_uid() == Some(caller_uid)
            && persisted.storage_location().is_some_and(|location| {
                location.storage_connection_string() == snapshot.storage_uri.as_wire()
            })
        {
            return Ok(persisted);
        }
        return Err(RecoveryError::Refused(
            "consumed recovery identity conflicts with replay",
        ));
    }
    grant.validate_live(tx, caller_uid).await?;
    let (persisted, outcome) = insert(tx, &snapshot).await?;
    if outcome == InsertOutcome::Created {
        let changed=sqlx::query(sql!("UPDATE snapshots SET generation=$2,recovery_grant_id=$3,source_node_uid=$4,timestamp=$5,last_attempt_at=NOW()
            WHERE id=$1 AND recovery_grant_id IS NULL AND source_node_uid IS NULL AND state='creating' AND source_node_id=$6"))
            .bind(snapshot.id).bind(grant.execution_generation).bind(grant.id).bind(&grant.uploader_node_uid).bind(grant.captured_at).bind(grant.uploader_node_id).execute(&mut **tx).await?;
        if changed.rows_affected() != 1 {
            return Err(RecoveryError::Refused("recovery registration changed"));
        }
    } else if persisted.recovery_grant_id() != Some(grant.id)
        || persisted.source_node_uid() != Some(caller_uid)
    {
        return Err(RecoveryError::Refused(
            "attempt is bound to another authority",
        ));
    }
    sqlx::query(sql!(
        "UPDATE backup_recovery_grants SET active_attempt_id=$2 WHERE id=$1"
    ))
    .bind(grant.id)
    .bind(snapshot.id)
    .execute(&mut **tx)
    .await?;
    get(&mut **tx, snapshot.id)
        .await?
        .ok_or(RecoveryError::Refused("recovery attempt disappeared"))
}

/// Must commit atomically with Ready. Accepted upload work may finish after a
/// revoke, but it can never publish using revoked authority.
pub async fn consume(
    conn: &mut sqlx::PgConnection,
    grant: &RecoveryGrant,
    attempt: SnapshotId,
) -> Result<(), RecoveryError> {
    let result=sqlx::query(sql!("UPDATE backup_recovery_grants SET consumed_attempt_id=$2 WHERE id=$1 AND active_attempt_id=$2 AND consumed_attempt_id IS NULL AND revoked_at IS NULL AND expires_at>clock_timestamp()"))
        .bind(grant.id).bind(attempt).execute(conn).await?;
    if result.rows_affected() != 1 {
        return Err(RecoveryError::Refused(
            "recovery approval changed before finalization",
        ));
    }
    Ok(())
}

/// Read-only discovery after a lost admin response; audit records outlive attempts.
#[derive(Debug, FromRow, Serialize)]
pub struct RecoveryApprovalSummary {
    pub id: RecoveryGrantId,
    pub local_artifact_id: LocalArtifactId,
    pub execution_generation: i64,
    pub uploader_node_id: NodeId,
    pub uploader_node_uid: K8sNodeUid,
    pub approved_at: String,
    pub captured_at: String,
    pub capture_evidence: Json<CaptureTimeEvidence>,
    pub expires_at: String,
    pub active_attempt_id: Option<SnapshotId>,
    pub consumed_attempt_id: Option<SnapshotId>,
    pub revoked: bool,
}
pub async fn list_approvals(
    conn: impl sqlx::PgExecutor<'_>,
    sandbox: SandboxId,
) -> Result<Vec<RecoveryApprovalSummary>, sqlx::Error> {
    sqlx::query_as(sql!("SELECT id,local_artifact_id,execution_generation,uploader_node_id,uploader_node_uid,approved_at::TEXT,captured_at::TEXT,capture_evidence,expires_at::TEXT,active_attempt_id,consumed_attempt_id,revoked_at IS NOT NULL AS revoked FROM backup_recovery_grants WHERE sandbox_id=$1 ORDER BY approved_at DESC,id DESC LIMIT 100"))
        .bind(sandbox).fetch_all(conn).await
}

/// Verify exact bytes and object bindings before Ready. A consumed replay needs
/// identical proof but may occur after restore changes the sandbox's placement.
pub async fn validate_ready(
    conn: &mut sqlx::PgConnection,
    grant: &RecoveryGrant,
    attempt: &SnapshotRecord,
    proofs: &[SnapshotArtifactDigest],
    evidence: &[upload_evidence::ArtifactUploadEvidence],
    bytes: i64,
) -> Result<bool, RecoveryError> {
    let vm_proofs: Vec<_> = proofs
        .iter()
        .copied()
        .filter(|proof| proof.artifact != SnapshotArtifactFile::Metadata)
        .collect();
    let metadata = evidence
        .iter()
        .find(|entry| entry.artifact == SnapshotArtifactFile::Metadata)
        .and_then(|entry| entry.metadata_binding_digest);
    // Catalog bytes describe compressed/encrypted objects, while the manifest
    // describes logical files. Compare each count in its own representation.
    let wire_bytes = evidence
        .iter()
        .filter(|entry| entry.artifact != SnapshotArtifactFile::Metadata)
        .try_fold(0_i64, |sum, entry| {
            i64::try_from(entry.wire_size_bytes)
                .ok()
                .and_then(|size| sum.checked_add(size))
        });
    if wire_bytes != Some(bytes)
        || proofs.len() != vm_proofs.len() + 1
        || proofs.iter().any(|proof| proof.size_bytes == 0)
        || !grant.manifest.matches(&vm_proofs, metadata)
        || evidence.len() != proofs.len()
    {
        return Err(RecoveryError::Refused(
            "uploaded files differ from the approved manifest or lack exact evidence",
        ));
    }
    let location = attempt.storage_location().ok_or(RecoveryError::Refused(
        "recovery attempt has no storage location",
    ))?;
    let uri = ExactStorageUri::parse(location.storage_connection_string())
        .map_err(|_| RecoveryError::Refused("recovery storage location is invalid"))?;
    for proof in proofs {
        let Some(object) = evidence
            .iter()
            .find(|entry| entry.artifact == proof.artifact)
        else {
            return Err(RecoveryError::Refused(
                "uploaded file has no object evidence",
            ));
        };
        let expected = format!(
            "{}/{}",
            uri.parsed().key_prefix.trim_end_matches('/'),
            proof.artifact.file_name()
        );
        if object.object_key != expected || object.wire_size_bytes == 0 {
            return Err(RecoveryError::Refused(
                "uploaded object does not belong to this attempt",
            ));
        }
    }
    if grant.consumed_attempt_id == Some(attempt.id()) && attempt.state().is_ready() {
        return Ok(false);
    }
    if grant.active_attempt_id != Some(attempt.id()) {
        return Err(RecoveryError::Refused(
            "recovery attempt no longer owns its approval",
        ));
    }
    grant
        .validate_live(
            conn,
            attempt.source_node_uid().ok_or(RecoveryError::Refused(
                "recovery attempt has no uploader identity",
            ))?,
        )
        .await?;
    Ok(true)
}

#[cfg(test)]
mod tests;

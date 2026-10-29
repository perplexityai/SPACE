use pplx_space_common_core::{
    sql,
    types::{
        NodeId, SandboxId, SandboxQuarantineReasonCode, SandboxQuarantineSource,
        SandboxQuarantineVocabularyParseError,
    },
};
use sqlx::PgConnection;
use uuid::Uuid;

use super::quarantine_persistence::{QuarantineHistoryRecord, QuarantineStateRecord};
use crate::node_publish::validate::quarantine_claim::QuarantineClaim;

/// Episode identifiers are storage-owned, not claims made by the node.
#[derive(Debug)]
pub(crate) struct QuarantineSelectionClaim<'a> {
    sandbox_id: SandboxId,
    failure_reason: Option<&'a str>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    quarantine_id: Uuid,
    history_id: Uuid,
}

impl<'a> QuarantineSelectionClaim<'a> {
    pub(crate) fn materialize(claim: &'a QuarantineClaim) -> Self {
        Self {
            sandbox_id: claim.sandbox_id(),
            failure_reason: claim.failure_reason(),
            source: claim.source(),
            reason_code: claim.reason_code(),
            quarantine_id: Uuid::now_v7(),
            history_id: Uuid::now_v7(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClaimedQuarantineProjection {
    pub(crate) sandbox_id: SandboxId,
    pub(crate) from_lifecycle: String,
    failure_reason: Option<String>,
    source: SandboxQuarantineSource,
    reason_code: SandboxQuarantineReasonCode,
    quarantine_id: Uuid,
    history_id: Uuid,
}

/// The stored lifecycle text must match literally, without normalization.
#[derive(Debug)]
pub(crate) struct QuarantineLifecycleGuard<'a> {
    sandbox_id: SandboxId,
    from_lifecycle: &'a str,
}

impl ClaimedQuarantineProjection {
    pub(crate) fn lifecycle_guard(&self) -> QuarantineLifecycleGuard<'_> {
        QuarantineLifecycleGuard {
            sandbox_id: self.sandbox_id,
            from_lifecycle: &self.from_lifecycle,
        }
    }

    pub(crate) fn state_record(&self) -> QuarantineStateRecord<'_> {
        QuarantineStateRecord {
            sandbox_id: self.sandbox_id,
            from_lifecycle: &self.from_lifecycle,
            failure_reason: self.failure_reason.as_deref(),
            source: self.source,
            reason_code: self.reason_code,
            quarantine_id: self.quarantine_id,
        }
    }

    pub(crate) fn history_record(&self) -> QuarantineHistoryRecord<'_> {
        QuarantineHistoryRecord {
            sandbox_id: self.sandbox_id,
            failure_reason: self.failure_reason.as_deref(),
            source: self.source,
            reason_code: self.reason_code,
            quarantine_id: self.quarantine_id,
            history_id: self.history_id,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SelectQuarantineClaimsError {
    #[error("claim quarantined sandboxes: {0}")]
    Select(#[source] sqlx::Error),
    #[error("parse claimed quarantine source: {0}")]
    Source(#[source] SandboxQuarantineVocabularyParseError),
    #[error("parse claimed quarantine reason code: {0}")]
    Reason(#[source] SandboxQuarantineVocabularyParseError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApplyQuarantineClaimsError {
    #[error("mark quarantined sandbox lifecycle: {0}")]
    Update(#[source] sqlx::Error),
}

/// Read this node's eligible quarantine candidates without row locks. Selection
/// excludes quarantined/deleted rows but is not authority to mutate them; the
/// subsequent CAS must still match the exact stored lifecycle and owner.
/// Empty means no row passed the selection guard.
pub(crate) async fn select(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[QuarantineSelectionClaim<'_>],
) -> Result<Vec<ClaimedQuarantineProjection>, SelectQuarantineClaimsError> {
    if claims.is_empty() {
        return Ok(Vec::new());
    }
    // Unlike failed projection, quarantine is a node-authored hold that
    // preserves local evidence and hot-node affiliation across substrates.
    // Quarantine is a stronger hold than cleanup/delete intent: it stops
    // automated cleanup until a healer or operator decides the next step.
    // Deleted still wins, but direct-managed rows are allowed to publish
    // the same hold state as Kubernetes-managed rows.
    let claimed_rows =
        sqlx::query_as::<_, (Uuid, String, Option<String>, String, String, Uuid, Uuid)>(sql!(
            "SELECT
                     s.id,
                     s.lifecycle,
                     input.failure_reason,
                     input.source,
                     input.reason_code,
                     input.quarantine_id,
                     input.history_id
                 FROM sandboxes s
                 JOIN UNNEST(
                     $2::UUID[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::UUID[], $7::UUID[]
                 ) AS input(
                     sandbox_id, failure_reason, source, reason_code, quarantine_id, history_id
                 ) ON input.sandbox_id = s.id
                 WHERE s.active_node_id = $1
                   AND LOWER(TRIM(s.lifecycle)) NOT IN ('quarantined', 'deleted')"
        ))
        .bind(node_id)
        .bind(
            claims
                .iter()
                .map(|claim| claim.sandbox_id)
                .collect::<Vec<_>>(),
        )
        .bind(
            claims
                .iter()
                .map(|claim| claim.failure_reason)
                .collect::<Vec<_>>(),
        )
        .bind(
            claims
                .iter()
                .map(|claim| claim.source.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            claims
                .iter()
                .map(|claim| claim.reason_code.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            claims
                .iter()
                .map(|claim| claim.quarantine_id)
                .collect::<Vec<_>>(),
        )
        .bind(
            claims
                .iter()
                .map(|claim| claim.history_id)
                .collect::<Vec<_>>(),
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(SelectQuarantineClaimsError::Select)?;
    claimed_rows
        .into_iter()
        .map(
            |(
                sandbox_id,
                from_lifecycle,
                failure_reason,
                source,
                reason_code,
                quarantine_id,
                history_id,
            )| {
                Ok(ClaimedQuarantineProjection {
                    sandbox_id: SandboxId::from_uuid(sandbox_id),
                    from_lifecycle,
                    failure_reason,
                    source: source
                        .parse()
                        .map_err(SelectQuarantineClaimsError::Source)?,
                    reason_code: reason_code
                        .parse()
                        .map_err(SelectQuarantineClaimsError::Reason)?,
                    quarantine_id,
                    history_id,
                })
            },
        )
        .collect()
}

/// Apply the selected claims only while owner and exact lifecycle still match.
/// The update takes sandbox row locks; empty is a normal lost-race or
/// no-match outcome, and must not cause quarantine state/history to be written.
pub(crate) async fn apply(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[QuarantineLifecycleGuard<'_>],
) -> Result<Vec<SandboxId>, ApplyQuarantineClaimsError> {
    if claims.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "UPDATE sandboxes s
                 SET lifecycle = 'quarantined'
                 FROM UNNEST($1::UUID[], $3::TEXT[]) AS input(sandbox_id, from_lifecycle)
                 WHERE s.id = input.sandbox_id
                   AND s.active_node_id = $2
                   AND s.lifecycle = input.from_lifecycle
                 RETURNING s.id"
    ))
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.from_lifecycle)
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(ApplyQuarantineClaimsError::Update)
}

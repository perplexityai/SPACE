use pplx_space_common_core::{
    sql,
    types::{
        NodeId, SandboxId, SandboxQuarantineReasonCode, SandboxQuarantineSource,
        SandboxQuarantineVocabularyParseError,
    },
};
use sqlx::{PgConnection, types::Json};
use uuid::Uuid;

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

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ClaimedQuarantineSqlProjection<'a> {
    sandbox_id: SandboxId,
    from_lifecycle: &'a str,
    failure_reason: &'a Option<String>,
    source: &'a str,
    reason_code: &'a str,
    quarantine_id: Uuid,
    history_id: Uuid,
}

impl ClaimedQuarantineProjection {
    pub(crate) fn sql_projection(&self) -> ClaimedQuarantineSqlProjection<'_> {
        ClaimedQuarantineSqlProjection {
            sandbox_id: self.sandbox_id,
            from_lifecycle: &self.from_lifecycle,
            failure_reason: &self.failure_reason,
            source: self.source.as_str(),
            reason_code: self.reason_code.as_str(),
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
    quarantine_rows_json: Json<serde_json::Value>,
) -> Result<Vec<ClaimedQuarantineProjection>, SelectQuarantineClaimsError> {
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
                 JOIN jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     failure_reason TEXT,
                     source TEXT,
                     reason_code TEXT,
                     quarantine_id UUID,
                     history_id UUID
                 ) ON input.sandbox_id = s.id
                 WHERE s.active_node_id = $1
                   AND LOWER(TRIM(s.lifecycle)) NOT IN ('quarantined', 'deleted')"
        ))
        .bind(node_id)
        .bind(quarantine_rows_json)
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
    claimed_json: Json<serde_json::Value>,
) -> Result<Vec<SandboxId>, ApplyQuarantineClaimsError> {
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "UPDATE sandboxes s
                 SET lifecycle = 'quarantined'
                 FROM jsonb_to_recordset($1::jsonb) AS input(
                     sandbox_id UUID,
                     from_lifecycle TEXT
                 )
                 WHERE s.id = input.sandbox_id
                   AND s.active_node_id = $2
                   AND s.lifecycle = input.from_lifecycle
                 RETURNING s.id"
    ))
    .bind(claimed_json)
    .bind(node_id)
    .fetch_all(&mut *conn)
    .await
    .map_err(ApplyQuarantineClaimsError::Update)
}

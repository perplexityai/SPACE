//! Sandbox subjects: the reads a sandbox campaign's driver finds and
//! re-reads its sandboxes with. The kind-agnostic verbs record, lease and
//! finish their items ([`crate::insert_pending`], [`crate::lease_items`],
//! [`crate::finish_item`]).

use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_sql_locator::sql;

use crate::{CampaignId, ItemPhase, SubjectKind};

/// A sandbox a campaign may cover, with the execution it last published,
/// which says which environment it's on.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct SandboxCandidate {
    pub sandbox_id: uuid::Uuid,
    /// `sandbox_observed_states.execution_metadata`, unparsed.
    pub execution_metadata: Option<sqlx::types::Json<serde_json::Value>>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

/// Up to `limit` sandboxes after `after`, in id order, that `campaign_id`
/// has no item for yet, or only a skipped one with leases left of
/// `max_attempts`: rows of the `managed_by` substrate in one of `lifecycles`
/// whose node has published them. A sandbox whose published execution names
/// an environment other than the campaign's source is left out here, since
/// it can't be on the source; the caller attributes the rest (bound to the
/// source, or unbound) through the catalog. A page shorter than `limit` ends
/// a pass. Takes no lock.
pub async fn sandbox_candidates(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
    managed_by: &str,
    lifecycles: &[SandboxLifecycle],
    after: Option<uuid::Uuid>,
    limit: u32,
    max_attempts: u64,
) -> Result<Vec<SandboxCandidate>, sqlx::Error> {
    let lifecycles: Vec<&str> = lifecycles
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    // A string `environment` binds the record, as
    // `RecordedExecution::from_record` reads it; references parse only in
    // their canonical lowercase form, so comparing the text is comparing the
    // references.
    sqlx::query_as::<_, SandboxCandidate>(sql!(
        "SELECT s.id AS sandbox_id, o.execution_metadata, o.runtime, o.runtime_version,
                o.state_format_name, o.state_format_version
         FROM runtime_upgrade_campaigns c
         JOIN sandboxes s ON s.managed_by = $2
         JOIN sandbox_observed_states o ON o.sandbox_id = s.id
         WHERE c.id = $1
           AND s.lifecycle = ANY($3)
           AND ($4::uuid IS NULL OR s.id > $4)
           AND (jsonb_typeof(o.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                OR o.execution_metadata ->> 'environment' = c.from_environment_ref)
           AND NOT EXISTS (
               SELECT 1
               FROM runtime_upgrade_items i
               WHERE i.campaign_id = $1
                 AND i.subject_kind = $6
                 AND i.subject_id = s.id
                 AND (i.phase <> $7 OR i.attempts >= $8))
         ORDER BY s.id
         LIMIT $5"
    ))
    .bind(campaign_id)
    .bind(managed_by)
    .bind(&lifecycles)
    .bind(after)
    .bind(i64::from(limit))
    .bind(SubjectKind::Sandbox.as_str())
    .bind(ItemPhase::Skipped.as_str())
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .fetch_all(executor)
    .await
}

/// A leased sandbox as it is now: its lifecycle, when that lifecycle last
/// changed (`sandboxes_touch_lifecycle_changed_at`), and the execution it last
/// published, which says which environment it's on.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct SubjectSandbox {
    pub lifecycle: String,
    pub lifecycle_changed_at: time::OffsetDateTime,
    /// The published execution. Every field but the id is `None` when the
    /// node hasn't published the sandbox.
    #[sqlx(flatten)]
    pub execution: SandboxCandidate,
}

/// `sandbox_id`'s lifecycle and published execution, or `None` once the
/// sandbox is deleted. Takes no lock.
pub async fn subject_sandbox(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: uuid::Uuid,
) -> Result<Option<SubjectSandbox>, sqlx::Error> {
    sqlx::query_as::<_, SubjectSandbox>(sql!(
        "SELECT s.lifecycle, s.lifecycle_changed_at, s.id AS sandbox_id, o.execution_metadata,
                o.runtime, o.runtime_version, o.state_format_name, o.state_format_version
         FROM sandboxes s
         LEFT JOIN sandbox_observed_states o ON o.sandbox_id = s.id
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}

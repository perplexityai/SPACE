//! Sandbox subjects: the reads a sandbox campaign's driver finds and
//! re-reads its sandboxes with. The kind-agnostic verbs record, lease and
//! finish their items ([`crate::insert_pending`], [`crate::lease_items`],
//! [`crate::finish_item`]).

use std::sync::LazyLock;

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_sandbox_teardown::observed_states::publish_carries_execution;
use pplx_space_sql_locator::sql;
use sqlx::types::Json;

use crate::{CampaignId, ItemPhase, SubjectKind};

/// A sandbox a campaign may cover, with the execution it last published,
/// which says which environment it's on.
#[derive(Clone, Debug, PartialEq)]
pub struct SandboxCandidate {
    pub sandbox_id: uuid::Uuid,
    /// `sandbox_observed_states.execution_metadata`, unparsed.
    pub execution_metadata: Option<serde_json::Value>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

#[derive(sqlx::FromRow)]
struct CandidateRow {
    sandbox_id: uuid::Uuid,
    execution_metadata: Option<Json<serde_json::Value>>,
    runtime: Option<String>,
    runtime_version: Option<String>,
    state_format_name: Option<String>,
    state_format_version: Option<String>,
}

impl From<CandidateRow> for SandboxCandidate {
    fn from(row: CandidateRow) -> Self {
        Self {
            sandbox_id: row.sandbox_id,
            execution_metadata: row.execution_metadata.map(|metadata| metadata.0),
            runtime: row.runtime,
            runtime_version: row.runtime_version,
            state_format_name: row.state_format_name,
            state_format_version: row.state_format_version,
        }
    }
}

/// Up to `limit` sandboxes after `after`, in id order, that `campaign_id`
/// has no item for yet, or only a skipped one with leases left of
/// `max_attempts`: rows of the `managed_by` substrate in one of `lifecycles`
/// whose node has published an execution for them, as a start's read of the
/// published execution requires ([`publish_carries_execution`]). A sandbox
/// whose published execution names an environment other than the campaign's
/// source is left out here, since it can't be on the source; the caller
/// attributes the rest (bound to the source, or unbound) through the catalog.
/// A page shorter than `limit` ends a pass. Takes no lock.
pub async fn sandbox_candidates(
    executor: impl Executor<'_>,
    campaign_id: CampaignId,
    managed_by: &str,
    lifecycles: &[SandboxLifecycle],
    after: Option<uuid::Uuid>,
    limit: u32,
    max_attempts: u64,
) -> Result<Vec<SandboxCandidate>, DbError> {
    let lifecycles: Vec<&str> = lifecycles
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    // A string `environment` binds the record, as
    // `RecordedExecution::from_record` reads it; references parse only in
    // their canonical lowercase form, so comparing the text is comparing the
    // references.
    /// Built once: the evidence predicate is the only interpolation.
    static CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT s.id AS sandbox_id, o.execution_metadata, o.runtime, o.runtime_version,
                    o.state_format_name, o.state_format_version
             FROM runtime_upgrade_campaigns c
             JOIN sandboxes s ON s.managed_by = $2
             JOIN sandbox_observed_states o ON o.sandbox_id = s.id
             WHERE c.id = $1
               AND s.lifecycle = ANY($3)
               AND ($4::uuid IS NULL OR s.id > $4)
               AND {}
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
             LIMIT $5",
            publish_carries_execution("o")
        ))
    });
    let rows = sqlx::query_as::<_, CandidateRow>(sqlx::AssertSqlSafe(CANDIDATES.as_str()))
        .bind(campaign_id)
        .bind(managed_by)
        .bind(&lifecycles)
        .bind(after)
        .bind(i64::from(limit))
        .bind(SubjectKind::Sandbox.as_str())
        .bind(ItemPhase::Skipped.as_str())
        .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
        .fetch_all(executor.into_sqlx())
        .await?;
    Ok(rows.into_iter().map(SandboxCandidate::from).collect())
}

/// A leased sandbox as it is now: its lifecycle and the execution it last
/// published, which says which environment it's on.
#[derive(Clone, Debug, PartialEq)]
pub struct SubjectSandbox {
    pub lifecycle: String,
    /// The published execution, or `None` when the node hasn't published the
    /// sandbox or its publish carried no execution
    /// ([`publish_carries_execution`]).
    pub execution: Option<SandboxCandidate>,
}

#[derive(sqlx::FromRow)]
struct SubjectRow {
    lifecycle: String,
    published: bool,
    #[sqlx(flatten)]
    execution: CandidateRow,
}

/// `sandbox_id`'s lifecycle and published execution, or `None` once its
/// `sandboxes` row is gone. A deleted sandbox's row is kept for a while with
/// lifecycle `deleted`, which this reads as it is. Takes no lock.
pub async fn subject_sandbox(
    executor: impl Executor<'_>,
    sandbox_id: uuid::Uuid,
) -> Result<Option<SubjectSandbox>, DbError> {
    /// Built once: the evidence predicate is the only interpolation.
    static SUBJECT: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT s.lifecycle, {} AS published, s.id AS sandbox_id, o.execution_metadata,
                    o.runtime, o.runtime_version, o.state_format_name, o.state_format_version
             FROM sandboxes s
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = s.id
             WHERE s.id = $1",
            publish_carries_execution("o")
        ))
    });
    let row = sqlx::query_as::<_, SubjectRow>(sqlx::AssertSqlSafe(SUBJECT.as_str()))
        .bind(sandbox_id)
        .fetch_optional(executor.into_sqlx())
        .await?;
    Ok(row.map(|row| SubjectSandbox {
        lifecycle: row.lifecycle,
        execution: row.published.then(|| row.execution.into()),
    }))
}

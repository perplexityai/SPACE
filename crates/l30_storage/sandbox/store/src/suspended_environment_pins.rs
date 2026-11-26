//! How `suspended` sandboxes are pinned to a runtime environment: the candidate
//! scan and single-row pin read for `sandbox_memory_drops` campaigns.
//!
//! Across both substrates (`SubstrateKind::Kubernetes` and
//! `SubstrateKind::Space`), an offline memory drop reads the pinned `full`
//! checkpoint from object storage and registers a `ready` projected `disk`
//! snapshot with a live storage location before re-pinning `sandboxes`. Both
//! substrates therefore require a `ready` pinned snapshot
//! ([`snapshot_is_current_local_artifact`]) with at least one unorphaned
//! `snapshot_storage_locations` row — matching the pinned `suspended` branch of
//! [`crate::runtime_population::sandbox_population`] on Kubernetes and the
//! durable-checkpoint precondition of offline memory drop on both substrates.
//!
//! [`candidates_page`] applies only the cheap SQL prefilter that skips rows
//! explicitly bound to another environment string, and [`pin_state`] returns the
//! pinned `LocalArtifactId` and raw execution columns
//! ([`PinnedCheckpointExecution`]) without interpreting `execution_metadata`.
//! The caller's `RecordedExecution::from_record` + `attribute` check is the
//! single judge of bound, unbound, and pre-metadata checkpoints, matching
//! activation-time runtime upgrade, memory-retirement admission, and
//! [`crate::runtime_population::sandbox_population`].
//!
//! [`candidates_page`] intentionally scans wider than offline projection's own
//! eligibility gates (such as node-liveness fencing while restore awaits
//! explicit pinned-artifact selection, a newer `full` snapshot row, an
//! unsupported storage URI, or unreadable checkpoint `metadata.json`). The
//! offline memory-drop dispatcher reports those rows as ineligible so the
//! campaign records them `skipped` with an explicit reason rather than silently
//! omitting them: any that later wake onto the target environment under
//! `memory_retired` are cold-booted by activation-time runtime upgrade and
//! restore-time memory drop, while any that remain `suspended` on the source
//! keep their pinned `full` checkpoint and stay counted in
//! [`crate::runtime_population::sandbox_population`] so blocking the source
//! environment still surfaces them unless `strand` is set.
//!
//! Once an offline memory drop re-pins the row to a projected `disk` artifact,
//! the row stays `suspended` and the retained `full` snapshot stays `ready`,
//! while [`snapshot_is_current_local_artifact`] shifts to the `disk` row:
//! [`candidates_page`] filters `sb.current_local_artifact_kind = 'full'` up
//! front so re-pinned `disk` rows and markerless (`NULL`) rows skip the lateral
//! `snapshots` probe, and [`pin_state`] reports [`ArtifactKind::Disk`]
//! under [`SandboxPinState::SuspendedPinned`] so a retry whose earlier attempt
//! already re-pinned the row records `done` without calling the dispatcher.
//! On Kubernetes, shifting [`snapshot_is_current_local_artifact`] to `disk`
//! also removes the row from `suspended` [`crate::runtime_population::sandbox_population`];
//! Direct (`SubstrateKind::Space`) still reads the newest `full` snapshot in
//! `sandbox_population` until its selector consults `current_local_artifact_*`
//! before Direct is registered for `sandbox_memory_drops`.
//! Rows present in another lifecycle (such as `restoring` or `suspending`
//! mid-transition) report [`SandboxPinState::NotPinned`] with their raw
//! `lifecycle` so the driver releases mid-transition items without spending an
//! attempt.

use std::sync::LazyLock;

use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_sandbox_portability::snapshot_is_current_local_artifact;
use pplx_space_snapshot_model::{ArtifactKind, LocalArtifactId};
use pplx_space_sql_locator::sql;
use sqlx::types::{Json, JsonValue};

use crate::SandboxStoreError;

/// Execution fields recorded on a sandbox's pinned checkpoint, left unparsed so
/// the caller attributes bound, unbound, and pre-metadata rows leniently
/// against the runtime catalog with the same rule as restore.
#[derive(Clone, Debug, Default, Eq, PartialEq, sqlx::FromRow)]
pub struct PinnedCheckpointExecution {
    pub execution_metadata: Option<Json<JsonValue>>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

/// One `suspended` sandbox pinned to a `ready`, unorphaned `full` checkpoint
/// that isn't explicitly bound to another environment string.
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct SuspendedPinnedCandidate {
    pub sandbox_id: SandboxId,
    #[sqlx(flatten)]
    pub execution: PinnedCheckpointExecution,
}

/// Current lifecycle and pinned-checkpoint state of a single sandbox row on
/// `managed_by`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SandboxPinState {
    /// `suspended` on `managed_by` and pinned to a `ready`, unorphaned `full`
    /// or `disk` checkpoint, carrying the pinned `LocalArtifactId` and raw
    /// execution columns for caller attribution.
    SuspendedPinned {
        artifact_id: LocalArtifactId,
        kind: ArtifactKind,
        execution: PinnedCheckpointExecution,
    },
    /// Present on `managed_by`, but either in another `lifecycle` (for example,
    /// `restoring` or `suspending` mid-transition, or `started`/`stopped`) or
    /// `suspended` without a `ready`, unorphaned `full` or `disk` checkpoint
    /// matching its current local artifact pin.
    NotPinned { lifecycle: String },
    /// Absent or managed by another substrate.
    Absent,
}

#[derive(sqlx::FromRow)]
struct PinStateRow {
    lifecycle: String,
    artifact_id: Option<LocalArtifactId>,
    snapshot_type: Option<String>,
    execution_metadata: Option<Json<JsonValue>>,
    runtime: Option<String>,
    runtime_version: Option<String>,
    state_format_name: Option<String>,
    state_format_version: Option<String>,
}

const CANDIDATE_SELECT: &str = "sb.id AS sandbox_id,
                pinned_snapshot.execution_metadata,
                pinned_snapshot.runtime,
                pinned_snapshot.runtime_version,
                pinned_snapshot.state_format_name,
                pinned_snapshot.state_format_version";

static CANDIDATE_JOINS: LazyLock<String> = LazyLock::new(|| {
    let marked = snapshot_is_current_local_artifact("sb", "sn");
    format!(
        "CROSS JOIN LATERAL (
             SELECT sn.execution_metadata,
                    sn.runtime,
                    sn.runtime_version,
                    sn.state_format_name,
                    sn.state_format_version
             FROM snapshots sn
             WHERE sn.sandbox_id = sb.id
               AND sn.type = 'full'
               AND sn.state = 'ready'
               AND {marked}
               AND EXISTS (
                   SELECT 1
                   FROM snapshot_storage_locations ssl
                   WHERE ssl.snapshot_id = sn.id
                     AND ssl.orphaned_at IS NULL
               )
             ORDER BY sn.timestamp DESC, sn.id DESC
             LIMIT 1
         ) pinned_snapshot"
    )
});

/// First-page candidate scan, built once from [`CANDIDATE_SELECT`] and
/// [`CANDIDATE_JOINS`]: every value is bound. Kept separate from
/// [`CANDIDATES_AFTER_PAGE_QUERY`] so `after`-paged generic plans push
/// `sb.id > $3` directly into `sandboxes_pkey`, while unconditional
/// `sn.state = 'ready'` lets the lateral probe use
/// `idx_snapshots_ready_local_artifact`.
static CANDIDATES_FIRST_PAGE_QUERY: LazyLock<String> = LazyLock::new(|| {
    let joins = CANDIDATE_JOINS.as_str();
    sql!(format!(
        "SELECT {CANDIDATE_SELECT}
         FROM sandboxes sb
         {joins}
         WHERE sb.managed_by = $1
           AND sb.lifecycle = 'suspended'
           AND sb.current_local_artifact_kind = 'full'
           AND (jsonb_typeof(pinned_snapshot.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                OR pinned_snapshot.execution_metadata ->> 'environment' = $2)
         ORDER BY sb.id
         LIMIT $3"
    ))
});

/// Keyset-continued candidate scan after `sb.id > $3`, built once from
/// [`CANDIDATE_SELECT`] and [`CANDIDATE_JOINS`].
static CANDIDATES_AFTER_PAGE_QUERY: LazyLock<String> = LazyLock::new(|| {
    let joins = CANDIDATE_JOINS.as_str();
    sql!(format!(
        "SELECT {CANDIDATE_SELECT}
         FROM sandboxes sb
         {joins}
         WHERE sb.managed_by = $1
           AND sb.lifecycle = 'suspended'
           AND sb.current_local_artifact_kind = 'full'
           AND sb.id > $3
           AND (jsonb_typeof(pinned_snapshot.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                OR pinned_snapshot.execution_metadata ->> 'environment' = $2)
         ORDER BY sb.id
         LIMIT $4"
    ))
});

/// Single-row lifecycle and pin read on `sandboxes_pkey`, built once over both
/// `'full'` and `'disk'` pins.
static PIN_STATE_QUERY: LazyLock<String> = LazyLock::new(|| {
    let marked = snapshot_is_current_local_artifact("sb", "sn");
    sql!(format!(
        "SELECT sb.lifecycle,
                sb.current_local_artifact_id AS artifact_id,
                pinned_snapshot.type AS snapshot_type,
                pinned_snapshot.execution_metadata,
                pinned_snapshot.runtime,
                pinned_snapshot.runtime_version,
                pinned_snapshot.state_format_name,
                pinned_snapshot.state_format_version
         FROM sandboxes sb
         LEFT JOIN LATERAL (
             SELECT sn.type,
                    sn.execution_metadata,
                    sn.runtime,
                    sn.runtime_version,
                    sn.state_format_name,
                    sn.state_format_version
             FROM snapshots sn
             WHERE sb.lifecycle = 'suspended'
               AND sb.current_local_artifact_kind IN ('full', 'disk')
               AND sn.sandbox_id = sb.id
               AND sn.type = sb.current_local_artifact_kind
               AND sn.state = 'ready'
               AND {marked}
               AND EXISTS (
                   SELECT 1
                   FROM snapshot_storage_locations ssl
                   WHERE ssl.snapshot_id = sn.id
                     AND ssl.orphaned_at IS NULL
               )
             ORDER BY sn.timestamp DESC, sn.id DESC
             LIMIT 1
         ) pinned_snapshot ON TRUE
         WHERE sb.id = $2
           AND sb.managed_by = $1"
    ))
});

/// Up to `limit` `suspended` sandboxes on `managed_by` after `after`, in id
/// order, that are pinned to a `ready`, unorphaned `full` checkpoint not
/// explicitly bound to another environment string. Takes no lock.
pub async fn candidates_page(
    executor: impl sqlx::PgExecutor<'_>,
    managed_by: SubstrateKind,
    environment: EnvironmentReference,
    after: Option<SandboxId>,
    limit: u32,
) -> Result<Vec<SuspendedPinnedCandidate>, SandboxStoreError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let environment = environment.to_string();
    let limit = i64::from(limit);
    let rows = match after {
        None => {
            sqlx::query_as::<_, SuspendedPinnedCandidate>(sqlx::AssertSqlSafe(
                CANDIDATES_FIRST_PAGE_QUERY.as_str(),
            ))
            .bind(managed_by)
            .bind(environment)
            .bind(limit)
            .fetch_all(executor)
            .await?
        }
        Some(after) => {
            sqlx::query_as::<_, SuspendedPinnedCandidate>(sqlx::AssertSqlSafe(
                CANDIDATES_AFTER_PAGE_QUERY.as_str(),
            ))
            .bind(managed_by)
            .bind(environment)
            .bind(after)
            .bind(limit)
            .fetch_all(executor)
            .await?
        }
    };
    Ok(rows)
}

/// Read `sandbox_id`'s current lifecycle and `suspended` checkpoint pin on
/// `managed_by`, returning raw execution columns so the caller attributes the
/// environment with the same rule as restore. Takes no lock.
pub async fn pin_state(
    executor: impl sqlx::PgExecutor<'_>,
    managed_by: SubstrateKind,
    sandbox_id: SandboxId,
) -> Result<SandboxPinState, SandboxStoreError> {
    let Some(row) = sqlx::query_as::<_, PinStateRow>(sqlx::AssertSqlSafe(PIN_STATE_QUERY.as_str()))
        .bind(managed_by)
        .bind(sandbox_id)
        .fetch_optional(executor)
        .await?
    else {
        return Ok(SandboxPinState::Absent);
    };
    let kind = row
        .snapshot_type
        .as_deref()
        .and_then(|snapshot_type| snapshot_type.parse::<ArtifactKind>().ok());
    let (Some(artifact_id), Some(kind)) = (row.artifact_id, kind) else {
        return Ok(SandboxPinState::NotPinned {
            lifecycle: row.lifecycle,
        });
    };
    Ok(SandboxPinState::SuspendedPinned {
        artifact_id,
        kind,
        execution: PinnedCheckpointExecution {
            execution_metadata: row.execution_metadata,
            runtime: row.runtime,
            runtime_version: row.runtime_version,
            state_format_name: row.state_format_name,
            state_format_version: row.state_format_version,
        },
    })
}

#[cfg(test)]
#[path = "suspended_environment_pins/tests.rs"]
mod tests;

//! Sandbox subjects: the reads a sandbox campaign's driver finds and
//! re-reads its sandboxes with. The kind-agnostic verbs record, lease and
//! finish their items ([`crate::insert_pending`], [`crate::lease_items`],
//! [`crate::finish_item`]).

mod fences;

use std::sync::LazyLock;

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxLifecycle;
use pplx_space_sandbox_teardown::observed_states::publish_carries_execution;
use pplx_space_snapshot_model::{ArtifactKind, ArtifactOrigin, SnapshotKind, SnapshotState};
use pplx_space_sql_locator::sql;
use sqlx::types::Json;

pub use self::fences::{
    ExternalOperation, StoppedExecutionOwnership, external_activation_since, external_stop_since,
    stopped_execution_ownership,
};
use crate::items::{
    TARGET_START_SETTLED, TARGET_START_SETTLED_PREFIX, TARGET_STARTING, TARGET_STARTING_PREFIX,
    UNSTARTED_PREFIX,
};
use crate::{CampaignId, CampaignPhase, ItemPhase, SubjectKind};

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
    managed_by: SubstrateKind,
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

/// A running, paused, or mid-transition sandbox a `sandbox_force_restarts`
/// campaign may cover, with its current lifecycle and any execution its node
/// has published.
#[derive(Clone, Debug, PartialEq)]
pub struct ForceRestartCandidate {
    pub sandbox_id: uuid::Uuid,
    pub lifecycle: String,
    /// The published execution, or `None` when the node hasn't published the
    /// sandbox, its publish carried no execution
    /// ([`publish_carries_execution`]), or `observed_projection_gc` pruned its
    /// projection while `suspended`.
    pub execution: Option<SandboxCandidate>,
}

#[derive(sqlx::FromRow)]
struct ForceRestartCandidateRow {
    lifecycle: String,
    published: bool,
    #[sqlx(flatten)]
    execution: CandidateRow,
}

/// Up to `limit` sandboxes after `after`, in id order, that `campaign_id`
/// has no item for yet, or only a skipped one with leases left of
/// `max_attempts`: rows of the `managed_by` substrate in one of
/// `settled_lifecycles` or `transition_lifecycles` whether or not
/// `sandbox_observed_states` is present yet (including newly settled or
/// degraded status-only active residents and `suspended` sandboxes whose
/// projection was pruned before entering `restoring`). Any row whose published
/// execution already binds an environment other than the campaign's source is
/// left out here; the caller classifies the rest through the catalog and
/// current lifecycle. A page shorter than `limit` ends a pass. Takes no lock.
#[expect(
    clippy::too_many_arguments,
    reason = "executor, campaign, substrate, settled/transition lifecycle sets, keyset cursor, page size, and attempt cap are distinct query inputs"
)]
pub async fn force_restart_candidates(
    executor: impl Executor<'_>,
    campaign_id: CampaignId,
    managed_by: SubstrateKind,
    settled_lifecycles: &[SandboxLifecycle],
    transition_lifecycles: &[SandboxLifecycle],
    after: Option<uuid::Uuid>,
    limit: u32,
    max_attempts: u64,
) -> Result<Vec<ForceRestartCandidate>, DbError> {
    let settled: Vec<&str> = settled_lifecycles
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    let transitions: Vec<&str> = transition_lifecycles
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    /// Built once: the evidence predicate is the only interpolation.
    static FORCE_RESTART_CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT s.lifecycle, {} AS published, s.id AS sandbox_id, o.execution_metadata,
                    o.runtime, o.runtime_version, o.state_format_name, o.state_format_version
             FROM runtime_upgrade_campaigns c
             JOIN sandboxes s ON s.managed_by = $2
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = s.id
             WHERE c.id = $1
               AND (s.lifecycle = ANY($3) OR s.lifecycle = ANY($4))
               AND ($5::uuid IS NULL OR s.id > $5)
               AND (jsonb_typeof(o.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                    OR o.execution_metadata ->> 'environment' = c.from_environment_ref)
               AND NOT EXISTS (
                   SELECT 1
                   FROM runtime_upgrade_items i
                   WHERE i.campaign_id = $1
                     AND i.subject_kind = $7
                     AND i.subject_id = s.id
                     AND (i.phase <> $8 OR i.attempts >= $9))
             ORDER BY s.id
             LIMIT $6",
            publish_carries_execution("o")
        ))
    });
    let rows = sqlx::query_as::<_, ForceRestartCandidateRow>(sqlx::AssertSqlSafe(
        FORCE_RESTART_CANDIDATES.as_str(),
    ))
    .bind(campaign_id)
    .bind(managed_by)
    .bind(&settled)
    .bind(&transitions)
    .bind(after)
    .bind(i64::from(limit))
    .bind(SubjectKind::Sandbox.as_str())
    .bind(ItemPhase::Skipped.as_str())
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .fetch_all(executor.into_sqlx())
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| ForceRestartCandidate {
            sandbox_id: row.execution.sandbox_id,
            lifecycle: row.lifecycle,
            execution: row.published.then(|| row.execution.into()),
        })
        .collect())
}

/// A leased sandbox as it is now: its lifecycle, when that lifecycle last
/// changed (`sandboxes_touch_lifecycle_changed_at`), whether it's a `'stopped'`
/// row pinned to a `'ready'` `'disk'`/`'stop'` artifact (as `node_drain`
/// records when evacuating and optionally re-placing a stopped sandbox), and
/// the execution it published or pinned on that stop disk.
#[derive(Clone, Debug, PartialEq)]
pub struct SubjectSandbox {
    pub lifecycle: String,
    pub lifecycle_changed_at: time::OffsetDateTime,
    pub pinned_stop_disk: bool,
    /// The published execution, or `None` when the node hasn't published the
    /// sandbox or its publish carried no execution
    /// ([`publish_carries_execution`]).
    pub execution: Option<SandboxCandidate>,
    /// When `pinned_stop_disk` holds, the execution recorded on the pinned
    /// `'ready'` `'disk'`/`'stop'` snapshot if it carries one
    /// ([`publish_carries_execution`]).
    pub stop_disk_execution: Option<SandboxCandidate>,
}

impl SubjectSandbox {
    /// The node's published execution when present, else any execution
    /// recorded on a pinned `'ready'` `'disk'`/`'stop'` snapshot.
    pub fn effective_execution(&self) -> Option<&SandboxCandidate> {
        self.execution
            .as_ref()
            .or(self.stop_disk_execution.as_ref())
    }
}

#[derive(sqlx::FromRow)]
struct SubjectRow {
    lifecycle: String,
    lifecycle_changed_at: time::OffsetDateTime,
    pinned_stop_disk: bool,
    published: bool,
    disk_has_execution: bool,
    #[sqlx(flatten)]
    execution: CandidateRow,
    disk_execution_metadata: Option<Json<serde_json::Value>>,
    disk_runtime: Option<String>,
    disk_runtime_version: Option<String>,
    disk_state_format_name: Option<String>,
    disk_state_format_version: Option<String>,
}

/// `sandbox_id`'s lifecycle, stop-disk pin state, and published or pinned-disk
/// execution, or `None` once its `sandboxes` row is gone. A deleted sandbox's
/// row is kept for a while with lifecycle `deleted`, which this reads as it is.
/// Takes no lock.
pub async fn subject_sandbox(
    executor: impl Executor<'_>,
    sandbox_id: uuid::Uuid,
) -> Result<Option<SubjectSandbox>, DbError> {
    /// Built once: the evidence predicates are the only interpolations.
    static SUBJECT: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT s.lifecycle,
                    s.lifecycle_changed_at,
                    (disk.id IS NOT NULL) AS pinned_stop_disk,
                    {} AS published,
                    (disk.id IS NOT NULL AND {}) AS disk_has_execution,
                    s.id AS sandbox_id,
                    o.execution_metadata,
                    o.runtime,
                    o.runtime_version,
                    o.state_format_name,
                    o.state_format_version,
                    disk.execution_metadata AS disk_execution_metadata,
                    disk.runtime AS disk_runtime,
                    disk.runtime_version AS disk_runtime_version,
                    disk.state_format_name AS disk_state_format_name,
                    disk.state_format_version AS disk_state_format_version
             FROM sandboxes s
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = s.id
             LEFT JOIN LATERAL (
                 SELECT sn.id, sn.execution_metadata, sn.runtime, sn.runtime_version,
                        sn.state_format_name, sn.state_format_version
                 FROM snapshots sn
                 WHERE s.lifecycle = $2
                   AND s.current_local_artifact_id IS NOT NULL
                   AND s.current_local_artifact_kind = $3
                   AND s.current_local_artifact_origin = $4
                   AND sn.sandbox_id = s.id
                   AND sn.local_artifact_id = s.current_local_artifact_id
                   AND sn.type = $5
                   AND sn.state = $6
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             ) disk ON TRUE
             WHERE s.id = $1",
            publish_carries_execution("o"),
            publish_carries_execution("disk"),
        ))
    });
    let row = sqlx::query_as::<_, SubjectRow>(sqlx::AssertSqlSafe(SUBJECT.as_str()))
        .bind(sandbox_id)
        .bind(SandboxLifecycle::Stopped.as_str())
        .bind(ArtifactKind::Disk.as_str())
        .bind(ArtifactOrigin::Stop.as_str())
        .bind(SnapshotKind::Disk.as_str())
        .bind(SnapshotState::Ready.as_str())
        .fetch_optional(executor.into_sqlx())
        .await?;
    Ok(row.map(|row| decode_subject_row(row).1))
}

fn decode_subject_row(row: SubjectRow) -> (uuid::Uuid, SubjectSandbox) {
    let sandbox_id = row.execution.sandbox_id;
    let stop_disk_execution = row.disk_has_execution.then(|| SandboxCandidate {
        sandbox_id,
        execution_metadata: row.disk_execution_metadata.map(|metadata| metadata.0),
        runtime: row.disk_runtime,
        runtime_version: row.disk_runtime_version,
        state_format_name: row.disk_state_format_name,
        state_format_version: row.disk_state_format_version,
    });
    (
        sandbox_id,
        SubjectSandbox {
            lifecycle: row.lifecycle,
            lifecycle_changed_at: row.lifecycle_changed_at,
            pinned_stop_disk: row.pinned_stop_disk,
            execution: row.published.then(|| row.execution.into()),
            stop_disk_execution,
        },
    )
}

/// One bounded probe window returned by [`started_pending_target_sandboxes`].
#[derive(Clone, Debug, PartialEq)]
pub struct StartedPendingTargetPage {
    /// Whether the campaign's leasing, cluster-slot, canary, and failure-budget
    /// gates allowed probing pending items.
    pub gate_open: bool,
    /// Number of `pending` rows examined in this bounded `limit` window
    /// (`0..=limit`), including rows whose target-readiness probe didn't match.
    pub probed: usize,
    /// Greatest `subject_id` in this bounded `limit` window (`None` when
    /// `probed == 0` or `!gate_open`), used to advance the caller's ring
    /// cursor past non-matching `pending` prefixes.
    pub last_probed: Option<uuid::Uuid>,
    /// Matching target-ready or already-started-on-target items within the
    /// probed window, ordered by `sandbox_id`.
    pub items: Vec<StartedPendingTargetSandbox>,
}

/// One matching item in [`StartedPendingTargetPage::items`], ordered by
/// `sandbox_id`.
#[derive(Clone, Debug, PartialEq)]
pub struct StartedPendingTargetSandbox {
    pub sandbox_id: uuid::Uuid,
    pub node_id: Option<NodeId>,
    pub remaining_node_slots: Option<u64>,
    pub remaining_cluster_slots: u64,
    pub started_on_target: bool,
    pub starting_on_target: bool,
    /// Present when the `sandboxes` row still exists; `None` when
    /// `started_on_target` matched an item whose sandbox was deleted after
    /// settling on target.
    pub sandbox: Option<SubjectSandbox>,
}

#[derive(sqlx::FromRow)]
struct StartedPendingTargetRow {
    gate_open: bool,
    probed_count: i64,
    last_probed: Option<uuid::Uuid>,
    matched_sandbox_id: Option<uuid::Uuid>,
    started_on_target: Option<bool>,
    starting_on_target: Option<bool>,
    sandbox_present: Option<bool>,
    node_id: Option<NodeId>,
    remaining_node_slots: Option<i64>,
    remaining_cluster_slots: Option<i64>,
    lifecycle: Option<String>,
    lifecycle_changed_at: Option<time::OffsetDateTime>,
    pinned_stop_disk: Option<bool>,
    published: Option<bool>,
    disk_has_execution: Option<bool>,
    execution_metadata: Option<Json<serde_json::Value>>,
    runtime: Option<String>,
    runtime_version: Option<String>,
    state_format_name: Option<String>,
    state_format_version: Option<String>,
    disk_execution_metadata: Option<Json<serde_json::Value>>,
    disk_runtime: Option<String>,
    disk_runtime_version: Option<String>,
    disk_state_format_name: Option<String>,
    disk_state_format_version: Option<String>,
}

// `pending_window` is `MATERIALIZED` ahead of `matched_candidates` so Postgres
// caps the `(campaign_id, phase, subject_id)` index walk to `$19` rows before
// running per-item joins and lateral snapshot probes, while `window_summary`
// still returns `(gate_open, probed_count, last_probed)` when zero rows match.
pub(crate) static STARTED_PENDING_TARGET_FIRST: LazyLock<String> = LazyLock::new(|| {
    let published = publish_carries_execution("o");
    let disk_published = publish_carries_execution("disk");
    let node_evidence = pplx_space_node_status_store::environment_evidence("nss");
    sql!(format!(
        "WITH in_flight AS (
             SELECT o_if.node_id, count(*) AS items
             FROM runtime_upgrade_items i_if
             LEFT JOIN LATERAL (
                 SELECT o_row.node_id
                 FROM sandbox_observed_states o_row
                 WHERE NOT COALESCE(i_if.last_error = $20 OR starts_with(i_if.last_error, $21), FALSE)
                   AND o_row.sandbox_id = i_if.subject_id
                 LIMIT 1
             ) o_if ON TRUE
             WHERE i_if.campaign_id = $1
               AND i_if.phase = $12
             GROUP BY o_if.node_id
         ),
         in_flight_cluster AS (
             SELECT COALESCE(sum(items), 0)::bigint AS total
             FROM in_flight
         ),
         campaign_gate AS (
             SELECT c.id, c.phase, c.to_environment_ref,
                    c.max_in_flight_per_node, c.max_in_flight_per_cluster,
                    ifc.total AS cluster_in_flight
             FROM runtime_upgrade_campaigns c
             CROSS JOIN in_flight_cluster ifc
             WHERE c.id = $1
               AND c.phase = ANY($15)
               AND ifc.total < (CASE WHEN c.phase = $16 THEN 1 ELSE c.max_in_flight_per_cluster END)
               AND (
                   c.phase <> $16
                   OR NOT EXISTS (
                       SELECT done_i.subject_id
                       FROM runtime_upgrade_items done_i
                       WHERE done_i.campaign_id = $1
                         AND done_i.phase = $17
                       ORDER BY done_i.subject_id
                   )
               )
               AND (
                   SELECT count(*)
                   FROM (
                       SELECT failed_i.subject_id
                       FROM runtime_upgrade_items failed_i
                       WHERE failed_i.campaign_id = $1
                         AND failed_i.phase = $18
                       ORDER BY failed_i.subject_id
                       LIMIT GREATEST(c.failure_budget, 0)
                   ) capped_failed
               ) < c.failure_budget
         ),
         pending_window AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE EXISTS (SELECT 1 FROM campaign_gate)
               AND i.campaign_id = $1
               AND i.phase = $9
             ORDER BY i.subject_id
             LIMIT $19
         ),
         window_summary AS (
             SELECT EXISTS (SELECT 1 FROM campaign_gate) AS gate_open,
                    count(*)::bigint AS probed_count,
                    (SELECT pw_last.subject_id
                     FROM pending_window pw_last
                     ORDER BY pw_last.subject_id DESC
                     LIMIT 1) AS last_probed
             FROM pending_window pw
         ),
         matched_candidates AS (
             SELECT pw.subject_id AS matched_sandbox_id,
                    COALESCE(pw.last_error = $20 OR starts_with(pw.last_error, $21), FALSE) AS started_on_target,
                    (
                        COALESCE(pw.last_error = $22 OR starts_with(pw.last_error, $23), FALSE)
                        AND s.id IS NOT NULL
                        AND s.managed_by = $2
                        AND s.lifecycle = $24
                    ) AS starting_on_target,
                    (s.id IS NOT NULL) AS sandbox_present,
                    CASE
                        WHEN COALESCE(pw.last_error = $20 OR starts_with(pw.last_error, $21), FALSE)
                        THEN NULL::uuid
                        ELSE o.node_id
                    END AS node_id,
                    CASE
                        WHEN NOT COALESCE(pw.last_error = $20 OR starts_with(pw.last_error, $21), FALSE)
                         AND o.node_id IS NOT NULL
                        THEN GREATEST(cg.max_in_flight_per_node - COALESCE(ifn.items, 0), 0)
                    END AS remaining_node_slots,
                    GREATEST(
                        (CASE WHEN cg.phase = $16 THEN 1 ELSE cg.max_in_flight_per_cluster END)
                            - cg.cluster_in_flight,
                        0
                    ) AS remaining_cluster_slots,
                    s.lifecycle,
                    s.lifecycle_changed_at,
                    (disk.id IS NOT NULL) AS pinned_stop_disk,
                    (s.id IS NOT NULL AND {published}) AS published,
                    (disk.id IS NOT NULL AND {disk_published}) AS disk_has_execution,
                    o.execution_metadata,
                    o.runtime,
                    o.runtime_version,
                    o.state_format_name,
                    o.state_format_version,
                    disk.execution_metadata AS disk_execution_metadata,
                    disk.runtime AS disk_runtime,
                    disk.runtime_version AS disk_runtime_version,
                    disk.state_format_name AS disk_state_format_name,
                    disk.state_format_version AS disk_state_format_version
             FROM pending_window pw
             CROSS JOIN campaign_gate cg
             LEFT JOIN sandboxes s ON s.id = pw.subject_id
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = pw.subject_id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
             LEFT JOIN in_flight ifn ON ifn.node_id = o.node_id
             LEFT JOIN LATERAL (
                 SELECT sn.id, sn.execution_metadata, sn.runtime, sn.runtime_version,
                        sn.state_format_name, sn.state_format_version
                 FROM snapshots sn
                 WHERE s.lifecycle = $4
                   AND s.current_local_artifact_id IS NOT NULL
                   AND s.current_local_artifact_kind = $5
                   AND s.current_local_artifact_origin = $6
                   AND sn.sandbox_id = s.id
                   AND sn.local_artifact_id = s.current_local_artifact_id
                   AND sn.type = $7
                   AND sn.state = $10
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             ) disk ON TRUE
             WHERE pw.last_error IS NOT NULL
               AND NOT starts_with(pw.last_error, $11)
               AND (
                   (pw.last_error = $20 OR starts_with(pw.last_error, $21))
                   OR o.node_id IS NULL
                   OR (
                       nss.reachable IS TRUE
                       AND {node_evidence}
                       AND COALESCE(ifn.items, 0) < cg.max_in_flight_per_node
                       AND NOT (o.node_id = ANY($14))
                   )
               )
               AND (
                   (pw.last_error = $20 OR starts_with(pw.last_error, $21))
                   OR (
                       (pw.last_error = $22 OR starts_with(pw.last_error, $23))
                       AND s.id IS NOT NULL
                       AND s.managed_by = $2
                       AND s.lifecycle = $24
                   )
                   OR (
                       s.id IS NOT NULL
                       AND s.managed_by = $2
                       AND s.lifecycle = ANY($3)
                       AND (
                           (
                               {published}
                               AND (
                                   jsonb_typeof(o.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                                   OR o.execution_metadata ->> 'environment' = cg.to_environment_ref
                               )
                           )
                           OR (
                               NOT {published}
                               AND disk.id IS NOT NULL
                               AND {disk_published}
                               AND (
                                   jsonb_typeof(disk.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                                   OR disk.execution_metadata ->> 'environment' = cg.to_environment_ref
                               )
                           )
                       )
                   )
               )
               AND NOT EXISTS (
                   SELECT 1
                   FROM runtime_upgrade_campaigns other_c
                   JOIN runtime_upgrade_items other_i
                     ON other_i.campaign_id = other_c.id
                    AND other_i.subject_kind = $8
                    AND other_i.subject_id = pw.subject_id
                   WHERE other_c.id <> cg.id
                     AND other_c.phase <> $13
                     AND other_i.phase = $12
                     AND other_i.lease_expires_at > clock_timestamp()
               )
         )
         SELECT ws.gate_open,
                ws.probed_count,
                ws.last_probed,
                mc.matched_sandbox_id,
                mc.started_on_target,
                mc.starting_on_target,
                mc.sandbox_present,
                mc.node_id,
                mc.remaining_node_slots,
                mc.remaining_cluster_slots,
                mc.lifecycle,
                mc.lifecycle_changed_at,
                mc.pinned_stop_disk,
                mc.published,
                mc.disk_has_execution,
                mc.execution_metadata,
                mc.runtime,
                mc.runtime_version,
                mc.state_format_name,
                mc.state_format_version,
                mc.disk_execution_metadata,
                mc.disk_runtime,
                mc.disk_runtime_version,
                mc.disk_state_format_name,
                mc.disk_state_format_version
         FROM window_summary ws
         LEFT JOIN matched_candidates mc ON TRUE
         ORDER BY mc.matched_sandbox_id",
    ))
});

pub(crate) static STARTED_PENDING_TARGET_AFTER: LazyLock<String> = LazyLock::new(|| {
    let published = publish_carries_execution("o");
    let disk_published = publish_carries_execution("disk");
    let node_evidence = pplx_space_node_status_store::environment_evidence("nss");
    sql!(format!(
        "WITH in_flight AS (
             SELECT o_if.node_id, count(*) AS items
             FROM runtime_upgrade_items i_if
             LEFT JOIN LATERAL (
                 SELECT o_row.node_id
                 FROM sandbox_observed_states o_row
                 WHERE NOT COALESCE(i_if.last_error = $20 OR starts_with(i_if.last_error, $21), FALSE)
                   AND o_row.sandbox_id = i_if.subject_id
                 LIMIT 1
             ) o_if ON TRUE
             WHERE i_if.campaign_id = $1
               AND i_if.phase = $12
             GROUP BY o_if.node_id
         ),
         in_flight_cluster AS (
             SELECT COALESCE(sum(items), 0)::bigint AS total
             FROM in_flight
         ),
         campaign_gate AS (
             SELECT c.id, c.phase, c.to_environment_ref,
                    c.max_in_flight_per_node, c.max_in_flight_per_cluster,
                    ifc.total AS cluster_in_flight
             FROM runtime_upgrade_campaigns c
             CROSS JOIN in_flight_cluster ifc
             WHERE c.id = $1
               AND c.phase = ANY($15)
               AND ifc.total < (CASE WHEN c.phase = $16 THEN 1 ELSE c.max_in_flight_per_cluster END)
               AND (
                   c.phase <> $16
                   OR NOT EXISTS (
                       SELECT done_i.subject_id
                       FROM runtime_upgrade_items done_i
                       WHERE done_i.campaign_id = $1
                         AND done_i.phase = $17
                       ORDER BY done_i.subject_id
                   )
               )
               AND (
                   SELECT count(*)
                   FROM (
                       SELECT failed_i.subject_id
                       FROM runtime_upgrade_items failed_i
                       WHERE failed_i.campaign_id = $1
                         AND failed_i.phase = $18
                       ORDER BY failed_i.subject_id
                       LIMIT GREATEST(c.failure_budget, 0)
                   ) capped_failed
               ) < c.failure_budget
         ),
         pending_window AS MATERIALIZED (
             SELECT i.subject_id, i.last_error
             FROM runtime_upgrade_items i
             WHERE EXISTS (SELECT 1 FROM campaign_gate)
               AND i.campaign_id = $1
               AND i.phase = $9
               AND i.subject_id > $25
             ORDER BY i.subject_id
             LIMIT $19
         ),
         window_summary AS (
             SELECT EXISTS (SELECT 1 FROM campaign_gate) AS gate_open,
                    count(*)::bigint AS probed_count,
                    (SELECT pw_last.subject_id
                     FROM pending_window pw_last
                     ORDER BY pw_last.subject_id DESC
                     LIMIT 1) AS last_probed
             FROM pending_window pw
         ),
         matched_candidates AS (
             SELECT pw.subject_id AS matched_sandbox_id,
                    COALESCE(pw.last_error = $20 OR starts_with(pw.last_error, $21), FALSE) AS started_on_target,
                    (
                        COALESCE(pw.last_error = $22 OR starts_with(pw.last_error, $23), FALSE)
                        AND s.id IS NOT NULL
                        AND s.managed_by = $2
                        AND s.lifecycle = $24
                    ) AS starting_on_target,
                    (s.id IS NOT NULL) AS sandbox_present,
                    CASE
                        WHEN COALESCE(pw.last_error = $20 OR starts_with(pw.last_error, $21), FALSE)
                        THEN NULL::uuid
                        ELSE o.node_id
                    END AS node_id,
                    CASE
                        WHEN NOT COALESCE(pw.last_error = $20 OR starts_with(pw.last_error, $21), FALSE)
                         AND o.node_id IS NOT NULL
                        THEN GREATEST(cg.max_in_flight_per_node - COALESCE(ifn.items, 0), 0)
                    END AS remaining_node_slots,
                    GREATEST(
                        (CASE WHEN cg.phase = $16 THEN 1 ELSE cg.max_in_flight_per_cluster END)
                            - cg.cluster_in_flight,
                        0
                    ) AS remaining_cluster_slots,
                    s.lifecycle,
                    s.lifecycle_changed_at,
                    (disk.id IS NOT NULL) AS pinned_stop_disk,
                    (s.id IS NOT NULL AND {published}) AS published,
                    (disk.id IS NOT NULL AND {disk_published}) AS disk_has_execution,
                    o.execution_metadata,
                    o.runtime,
                    o.runtime_version,
                    o.state_format_name,
                    o.state_format_version,
                    disk.execution_metadata AS disk_execution_metadata,
                    disk.runtime AS disk_runtime,
                    disk.runtime_version AS disk_runtime_version,
                    disk.state_format_name AS disk_state_format_name,
                    disk.state_format_version AS disk_state_format_version
             FROM pending_window pw
             CROSS JOIN campaign_gate cg
             LEFT JOIN sandboxes s ON s.id = pw.subject_id
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = pw.subject_id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
             LEFT JOIN in_flight ifn ON ifn.node_id = o.node_id
             LEFT JOIN LATERAL (
                 SELECT sn.id, sn.execution_metadata, sn.runtime, sn.runtime_version,
                        sn.state_format_name, sn.state_format_version
                 FROM snapshots sn
                 WHERE s.lifecycle = $4
                   AND s.current_local_artifact_id IS NOT NULL
                   AND s.current_local_artifact_kind = $5
                   AND s.current_local_artifact_origin = $6
                   AND sn.sandbox_id = s.id
                   AND sn.local_artifact_id = s.current_local_artifact_id
                   AND sn.type = $7
                   AND sn.state = $10
                 ORDER BY sn.timestamp DESC, sn.id DESC
                 LIMIT 1
             ) disk ON TRUE
             WHERE pw.last_error IS NOT NULL
               AND NOT starts_with(pw.last_error, $11)
               AND (
                   (pw.last_error = $20 OR starts_with(pw.last_error, $21))
                   OR o.node_id IS NULL
                   OR (
                       nss.reachable IS TRUE
                       AND {node_evidence}
                       AND COALESCE(ifn.items, 0) < cg.max_in_flight_per_node
                       AND NOT (o.node_id = ANY($14))
                   )
               )
               AND (
                   (pw.last_error = $20 OR starts_with(pw.last_error, $21))
                   OR (
                       (pw.last_error = $22 OR starts_with(pw.last_error, $23))
                       AND s.id IS NOT NULL
                       AND s.managed_by = $2
                       AND s.lifecycle = $24
                   )
                   OR (
                       s.id IS NOT NULL
                       AND s.managed_by = $2
                       AND s.lifecycle = ANY($3)
                       AND (
                           (
                               {published}
                               AND (
                                   jsonb_typeof(o.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                                   OR o.execution_metadata ->> 'environment' = cg.to_environment_ref
                               )
                           )
                           OR (
                               NOT {published}
                               AND disk.id IS NOT NULL
                               AND {disk_published}
                               AND (
                                   jsonb_typeof(disk.execution_metadata -> 'environment') IS DISTINCT FROM 'string'
                                   OR disk.execution_metadata ->> 'environment' = cg.to_environment_ref
                               )
                           )
                       )
                   )
               )
               AND NOT EXISTS (
                   SELECT 1
                   FROM runtime_upgrade_campaigns other_c
                   JOIN runtime_upgrade_items other_i
                     ON other_i.campaign_id = other_c.id
                    AND other_i.subject_kind = $8
                    AND other_i.subject_id = pw.subject_id
                   WHERE other_c.id <> cg.id
                     AND other_c.phase <> $13
                     AND other_i.phase = $12
                     AND other_i.lease_expires_at > clock_timestamp()
               )
         )
         SELECT ws.gate_open,
                ws.probed_count,
                ws.last_probed,
                mc.matched_sandbox_id,
                mc.started_on_target,
                mc.starting_on_target,
                mc.sandbox_present,
                mc.node_id,
                mc.remaining_node_slots,
                mc.remaining_cluster_slots,
                mc.lifecycle,
                mc.lifecycle_changed_at,
                mc.pinned_stop_disk,
                mc.published,
                mc.disk_has_execution,
                mc.execution_metadata,
                mc.runtime,
                mc.runtime_version,
                mc.state_format_name,
                mc.state_format_version,
                mc.disk_execution_metadata,
                mc.disk_runtime,
                mc.disk_runtime_version,
                mc.disk_state_format_name,
                mc.disk_state_format_version
         FROM window_summary ws
         LEFT JOIN matched_candidates mc ON TRUE
         ORDER BY mc.matched_sandbox_id",
    ))
});

/// Probe up to `limit` pending `sandbox` items of `campaign_id` after `after`
/// (in `subject_id` keyset order), returning the window's probe count and tail
/// cursor together with any matching target-ready or target-settled items.
///
/// The `pending_window` CTE bounds the `(campaign_id, phase, subject_id)` index
/// walk to `limit` rows before joining `sandboxes`, `sandbox_observed_states`,
/// `node_status_snapshots`, or `snapshots`, so large prefixes of untouched
/// `pending` items don't cause per-tick full-campaign scans while
/// `ScanPlan::moves == false`. Callers advance their per-campaign ring cursor
/// with [`StartedPendingTargetPage::last_probed`] across windows and passes.
/// Takes no lock.
pub async fn started_pending_target_sandboxes(
    executor: impl Executor<'_>,
    campaign_id: CampaignId,
    managed_by: SubstrateKind,
    excluded_nodes: &[NodeId],
    after: Option<uuid::Uuid>,
    limit: u32,
) -> Result<StartedPendingTargetPage, DbError> {
    let lifecycles = [
        SandboxLifecycle::Stopped.as_str(),
        SandboxLifecycle::Started.as_str(),
        SandboxLifecycle::Paused.as_str(),
    ];
    let leasing_phases: Vec<&str> = CampaignPhase::ALL
        .iter()
        .filter(|phase| phase.leases())
        .map(|phase| phase.as_str())
        .collect();
    let excluded_node_ids: Vec<uuid::Uuid> =
        excluded_nodes.iter().map(|node| node.as_uuid()).collect();
    let rows = if let Some(after_id) = after {
        sqlx::query_as::<_, StartedPendingTargetRow>(sqlx::AssertSqlSafe(
            STARTED_PENDING_TARGET_AFTER.as_str(),
        ))
        .bind(campaign_id)
        .bind(managed_by)
        .bind(&lifecycles[..])
        .bind(SandboxLifecycle::Stopped.as_str())
        .bind(ArtifactKind::Disk.as_str())
        .bind(ArtifactOrigin::Stop.as_str())
        .bind(SnapshotKind::Disk.as_str())
        .bind(SubjectKind::Sandbox.as_str())
        .bind(ItemPhase::Pending.as_str())
        .bind(SnapshotState::Ready.as_str())
        .bind(UNSTARTED_PREFIX)
        .bind(ItemPhase::InFlight.as_str())
        .bind(CampaignPhase::Completed.as_str())
        .bind(&excluded_node_ids[..])
        .bind(&leasing_phases[..])
        .bind(CampaignPhase::Canary.as_str())
        .bind(ItemPhase::Done.as_str())
        .bind(ItemPhase::Failed.as_str())
        .bind(i64::from(limit))
        .bind(TARGET_START_SETTLED)
        .bind(TARGET_START_SETTLED_PREFIX)
        .bind(TARGET_STARTING)
        .bind(TARGET_STARTING_PREFIX)
        .bind(SandboxLifecycle::Starting.as_str())
        .bind(after_id)
        .fetch_all(executor.into_sqlx())
        .await?
    } else {
        sqlx::query_as::<_, StartedPendingTargetRow>(sqlx::AssertSqlSafe(
            STARTED_PENDING_TARGET_FIRST.as_str(),
        ))
        .bind(campaign_id)
        .bind(managed_by)
        .bind(&lifecycles[..])
        .bind(SandboxLifecycle::Stopped.as_str())
        .bind(ArtifactKind::Disk.as_str())
        .bind(ArtifactOrigin::Stop.as_str())
        .bind(SnapshotKind::Disk.as_str())
        .bind(SubjectKind::Sandbox.as_str())
        .bind(ItemPhase::Pending.as_str())
        .bind(SnapshotState::Ready.as_str())
        .bind(UNSTARTED_PREFIX)
        .bind(ItemPhase::InFlight.as_str())
        .bind(CampaignPhase::Completed.as_str())
        .bind(&excluded_node_ids[..])
        .bind(&leasing_phases[..])
        .bind(CampaignPhase::Canary.as_str())
        .bind(ItemPhase::Done.as_str())
        .bind(ItemPhase::Failed.as_str())
        .bind(i64::from(limit))
        .bind(TARGET_START_SETTLED)
        .bind(TARGET_START_SETTLED_PREFIX)
        .bind(TARGET_STARTING)
        .bind(TARGET_STARTING_PREFIX)
        .bind(SandboxLifecycle::Starting.as_str())
        .fetch_all(executor.into_sqlx())
        .await?
    };
    let Some(first) = rows.first() else {
        return Ok(StartedPendingTargetPage {
            gate_open: false,
            probed: 0,
            last_probed: None,
            items: Vec::new(),
        });
    };
    let gate_open = first.gate_open;
    let probed = usize::try_from(first.probed_count).unwrap_or(0);
    let last_probed = first.last_probed;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(sandbox_id) = row.matched_sandbox_id else {
            continue;
        };
        let sandbox = if row.sandbox_present == Some(true) {
            let (
                Some(lifecycle),
                Some(lifecycle_changed_at),
                Some(pinned_stop_disk),
                Some(published),
                Some(disk_has_execution),
            ) = (
                row.lifecycle,
                row.lifecycle_changed_at,
                row.pinned_stop_disk,
                row.published,
                row.disk_has_execution,
            )
            else {
                continue;
            };
            Some(
                decode_subject_row(SubjectRow {
                    lifecycle,
                    lifecycle_changed_at,
                    pinned_stop_disk,
                    published,
                    disk_has_execution,
                    execution: CandidateRow {
                        sandbox_id,
                        execution_metadata: row.execution_metadata,
                        runtime: row.runtime,
                        runtime_version: row.runtime_version,
                        state_format_name: row.state_format_name,
                        state_format_version: row.state_format_version,
                    },
                    disk_execution_metadata: row.disk_execution_metadata,
                    disk_runtime: row.disk_runtime,
                    disk_runtime_version: row.disk_runtime_version,
                    disk_state_format_name: row.disk_state_format_name,
                    disk_state_format_version: row.disk_state_format_version,
                })
                .1,
            )
        } else {
            None
        };
        items.push(StartedPendingTargetSandbox {
            sandbox_id,
            node_id: row.node_id,
            remaining_node_slots: row
                .remaining_node_slots
                .map(|slots| u64::try_from(slots).unwrap_or(0)),
            remaining_cluster_slots: row
                .remaining_cluster_slots
                .and_then(|slots| u64::try_from(slots).ok())
                .unwrap_or(0),
            started_on_target: row.started_on_target.unwrap_or(false),
            starting_on_target: row.starting_on_target.unwrap_or(false),
            sandbox,
        });
    }
    Ok(StartedPendingTargetPage {
        gate_open,
        probed,
        last_probed,
        items,
    })
}

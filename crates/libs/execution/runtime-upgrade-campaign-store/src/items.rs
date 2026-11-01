//! Item verbs: finding sandbox subjects, recording them, leasing them under a
//! campaign's pacing, and recording outcomes under the lease.

use std::collections::HashMap;
use std::time::Duration;

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SandboxLifecycle, SubstrateKind};
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{
    CampaignId, CampaignKind, CampaignPhase, ItemError, ItemPhase, MAX_LEASE, SubjectKind,
    end_transaction,
};

/// A sandbox a campaign may cover, with the execution it last published,
/// which says which environment it's on.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct SandboxCandidate {
    pub sandbox_id: SandboxId,
    /// `sandbox_observed_states.execution_metadata`, unparsed.
    pub execution_metadata: Option<sqlx::types::Json<serde_json::Value>>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
}

/// Up to `limit` sandboxes after `after`, in id order, that `campaign_id`
/// has no item for yet, or only a skipped one with leases left of
/// `max_attempts`: rows of `managed_by`'s substrate in one of
/// `lifecycles` whose node has published them. A sandbox whose published
/// execution names an environment other than the campaign's source is left
/// out here, since it can't be on the source; the caller attributes the rest
/// (bound to the source, or unbound) through the catalog. A page shorter than
/// `limit` ends a pass. Takes no lock.
pub async fn sandbox_candidates(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
    managed_by: SubstrateKind,
    lifecycles: &[SandboxLifecycle],
    after: Option<SandboxId>,
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

/// Use up the leases of `campaign_id`'s skipped items for `sandbox_ids`,
/// which the caller attributed away from the campaign's source, so they're
/// neither candidates again nor hold the campaign open. `attempts` becomes
/// `max_attempts`, as for a sandbox the node kept declining. Items in other
/// phases are left alone. Returns how many were retired.
pub async fn retire_skipped_sandboxes(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
    sandbox_ids: &[SandboxId],
    max_attempts: u64,
) -> Result<u64, sqlx::Error> {
    if sandbox_ids.is_empty() {
        return Ok(0);
    }
    Ok(sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET attempts = $4
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = ANY($3)
           AND phase = $5
           AND attempts < $4"
    ))
    .bind(campaign_id)
    .bind(SubjectKind::Sandbox.as_str())
    .bind(sandbox_ids)
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .bind(ItemPhase::Skipped.as_str())
    .execute(executor)
    .await?
    .rows_affected())
}

/// A leased sandbox as it is now: its lifecycle and the execution it last
/// published, which says which environment it's on.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub struct SubjectSandbox {
    pub lifecycle: String,
    /// The published execution. Every field is `None` when the node hasn't
    /// published the sandbox.
    #[sqlx(flatten)]
    pub execution: SandboxCandidate,
}

/// `sandbox_id`'s lifecycle and published execution, or `None` once the
/// sandbox is deleted. Takes no lock.
pub async fn subject_sandbox(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<SubjectSandbox>, sqlx::Error> {
    sqlx::query_as::<_, SubjectSandbox>(sql!(
        "SELECT s.lifecycle, s.id AS sandbox_id, o.execution_metadata, o.runtime,
                o.runtime_version, o.state_format_name, o.state_format_version
         FROM sandboxes s
         LEFT JOIN sandbox_observed_states o ON o.sandbox_id = s.id
         WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}

/// Record `sandbox_ids` as pending items of `campaign_id`. A sandbox whose
/// item was skipped (it had started, say, when it was leased) is pending
/// again while it has leases left of `max_attempts`, keeping its count, so a
/// sandbox the node keeps declining is dispatched a bounded number of times.
/// Items in any other phase are left alone. Inserts nothing unless the
/// campaign is an unfinished sandbox campaign, which it locks for the insert
/// so a concurrent completion can't finish it under new items. Returns how
/// many items were added or made pending again. Opens a transaction, or a
/// savepoint inside the caller's.
pub async fn insert_pending_sandboxes(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    sandbox_ids: &[SandboxId],
    max_attempts: u64,
) -> Result<u64, sqlx::Error> {
    if sandbox_ids.is_empty() {
        return Ok(0);
    }
    let mut tx = conn.begin().await?;
    let inserted = insert_pending_in(&mut tx, campaign_id, sandbox_ids, max_attempts).await;
    end_transaction(tx, inserted).await
}

async fn insert_pending_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    sandbox_ids: &[SandboxId],
    max_attempts: u64,
) -> Result<u64, sqlx::Error> {
    let open = sqlx::query_scalar::<_, i32>(sql!(
        "SELECT 1
         FROM runtime_upgrade_campaigns
         WHERE id = $1
           AND kind = $2
           AND finished_at IS NULL
         FOR SHARE"
    ))
    .bind(campaign_id)
    .bind(CampaignKind::Sandboxes.as_str())
    .fetch_optional(&mut *tx)
    .await?
    .is_some();
    if !open {
        return Ok(0);
    }
    Ok(sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase)
         SELECT $1, $2, subject_id, $4
         FROM unnest($3::uuid[]) AS subject_id
         ON CONFLICT (campaign_id, subject_kind, subject_id) DO UPDATE
         SET phase = EXCLUDED.phase,
             last_error = NULL
         WHERE runtime_upgrade_items.phase = $5
           AND runtime_upgrade_items.attempts < $6"
    ))
    .bind(campaign_id)
    .bind(SubjectKind::Sandbox.as_str())
    .bind(sandbox_ids)
    .bind(ItemPhase::Pending.as_str())
    .bind(ItemPhase::Skipped.as_str())
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .execute(&mut *tx)
    .await?
    .rows_affected())
}

/// One item leased to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeasedItem {
    pub campaign_id: CampaignId,
    pub sandbox_id: SandboxId,
    /// The token the outcome must be recorded under.
    pub lease_token: uuid::Uuid,
    /// Leases taken on this item so far, this one included.
    pub attempts: u64,
    /// The node that last published the sandbox, if it still has a published
    /// state.
    pub node_id: Option<NodeId>,
}

/// How many items a canary leases: one at a time, until one is done.
const CANARY_IN_FLIGHT: u64 = 1;

/// Lease pending sandbox items of `campaign_id` for `lease` (clamped to
/// 1 second through [`MAX_LEASE`]), up to the campaign's pacing:
///
/// * nothing unless the campaign is a sandbox campaign in `canary` or
///   `rolling` whose failed items haven't reached its failure budget;
/// * in `canary`, one item at a time, and nothing once one is done: the
///   canary then waits for an operator to resume the campaign;
/// * at most `max_in_flight_per_cluster` items in flight in all, counting
///   those already leased;
/// * at most `max_in_flight_per_node` in flight on any node, by the node
///   that last published each sandbox. An item whose sandbox has no
///   published state (deleted, or never published) counts toward the
///   cluster cap only.
///
/// Only the first `scan_limit` pending items in subject order are
/// considered, so items on a saturated node can hold back items behind them
/// until the node drains.
///
/// Locks the campaign row for the whole lease, so concurrent leasers are
/// serialized and a pause or abort committed first is seen. Opens a
/// transaction, or a savepoint inside the caller's.
pub async fn lease_sandbox_items(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    lease: Duration,
    scan_limit: u32,
) -> Result<Vec<LeasedItem>, sqlx::Error> {
    let lease = lease.clamp(Duration::from_secs(1), MAX_LEASE);
    let mut tx = conn.begin().await?;
    let leased = lease_in(&mut tx, campaign_id, lease, scan_limit).await;
    end_transaction(tx, leased).await
}

async fn lease_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    lease: Duration,
    scan_limit: u32,
) -> Result<Vec<LeasedItem>, sqlx::Error> {
    let leasing: Vec<&str> = CampaignPhase::ALL
        .iter()
        .filter(|phase| phase.leases())
        .map(|phase| phase.as_str())
        .collect();
    let Some((canary, per_node, per_cluster, failure_budget)) =
        sqlx::query_as::<_, (bool, i64, i64, i64)>(sql!(
            "SELECT phase = $3, max_in_flight_per_node, max_in_flight_per_cluster, failure_budget
             FROM runtime_upgrade_campaigns
             WHERE id = $1
               AND kind = $2
               AND phase = ANY($4)
             FOR UPDATE"
        ))
        .bind(campaign_id)
        .bind(CampaignKind::Sandboxes.as_str())
        .bind(CampaignPhase::Canary.as_str())
        .bind(&leasing)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(Vec::new());
    };
    // Re-checked under the lock: a failure committed since the caller's
    // budget check must not let another batch out.
    let failed = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*)
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $2"
    ))
    .bind(campaign_id)
    .bind(ItemPhase::Failed.as_str())
    .fetch_one(&mut *tx)
    .await?;
    if failed >= failure_budget {
        return Ok(Vec::new());
    }
    let per_node = u64::try_from(per_node).unwrap_or(0);
    let mut per_cluster = u64::try_from(per_cluster).unwrap_or(0);
    if canary {
        // One statement, so one snapshot: a canary item that finishes
        // between two reads could otherwise look neither done nor in
        // flight, and a second canary would be leased.
        let (canary_done, canary_in_flight) = sqlx::query_as::<_, (bool, i64)>(sql!(
            "SELECT coalesce(bool_or(phase = $2), false), count(*) FILTER (WHERE phase = $3)
             FROM runtime_upgrade_items
             WHERE campaign_id = $1
               AND phase = ANY(ARRAY[$2, $3])"
        ))
        .bind(campaign_id)
        .bind(ItemPhase::Done.as_str())
        .bind(ItemPhase::InFlight.as_str())
        .fetch_one(&mut *tx)
        .await?;
        if canary_done || canary_in_flight >= CANARY_IN_FLIGHT as i64 {
            return Ok(Vec::new());
        }
        per_cluster = per_cluster.min(CANARY_IN_FLIGHT);
    }

    let in_flight = sqlx::query_as::<_, (Option<NodeId>, i64)>(sql!(
        "SELECT o.node_id, count(*)
         FROM runtime_upgrade_items i
         LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
         WHERE i.campaign_id = $1
           AND i.phase = $2
         GROUP BY o.node_id"
    ))
    .bind(campaign_id)
    .bind(ItemPhase::InFlight.as_str())
    .fetch_all(&mut *tx)
    .await?;
    let mut on_node: HashMap<NodeId, u64> = HashMap::new();
    let mut in_cluster = 0_u64;
    for (node_id, count) in in_flight {
        let count = u64::try_from(count).unwrap_or(0);
        in_cluster += count;
        if let Some(node_id) = node_id {
            *on_node.entry(node_id).or_default() += count;
        }
    }
    if in_cluster >= per_cluster {
        return Ok(Vec::new());
    }

    let pending = sqlx::query_as::<_, (SandboxId, Option<NodeId>)>(sql!(
        "SELECT i.subject_id, o.node_id
         FROM runtime_upgrade_items i
         LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
         WHERE i.campaign_id = $1
           AND i.subject_kind = $2
           AND i.phase = $3
         ORDER BY i.subject_id
         LIMIT $4"
    ))
    .bind(campaign_id)
    .bind(SubjectKind::Sandbox.as_str())
    .bind(ItemPhase::Pending.as_str())
    .bind(i64::from(scan_limit))
    .fetch_all(&mut *tx)
    .await?;

    let mut leased = Vec::new();
    for (sandbox_id, node_id) in pending {
        if in_cluster >= per_cluster {
            break;
        }
        if let Some(node_id) = node_id
            && on_node.get(&node_id).copied().unwrap_or(0) >= per_node
        {
            continue;
        }
        let lease_token = uuid::Uuid::now_v7();
        let attempts = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE runtime_upgrade_items
             SET phase = $4,
                 lease_token = $5,
                 lease_expires_at = now() + make_interval(secs => $6),
                 attempts = attempts + 1
             WHERE campaign_id = $1
               AND subject_kind = $2
               AND subject_id = $3
               AND phase = $7
             RETURNING attempts"
        ))
        .bind(campaign_id)
        .bind(SubjectKind::Sandbox.as_str())
        .bind(sandbox_id)
        .bind(ItemPhase::InFlight.as_str())
        .bind(lease_token)
        .bind(lease.as_secs_f64())
        .bind(ItemPhase::Pending.as_str())
        .fetch_optional(&mut *tx)
        .await?;
        // The campaign lock serializes leasers, so a pending item read above
        // is still pending; a miss means some other writer moved it.
        let Some(attempts) = attempts else {
            continue;
        };
        in_cluster += 1;
        if let Some(node_id) = node_id {
            *on_node.entry(node_id).or_default() += 1;
        }
        leased.push(LeasedItem {
            campaign_id,
            sandbox_id,
            lease_token,
            attempts: u64::try_from(attempts).unwrap_or(0),
            node_id,
        });
    }
    Ok(leased)
}

/// Extend `item`'s lease to `lease` from now (clamped as for
/// [`lease_sandbox_items`]). `false` when the lease is no longer the
/// caller's: it expired on the database clock, it was re-driven, or another
/// outcome was recorded. The caller then stops working on the item.
pub async fn renew_lease(
    executor: impl sqlx::PgExecutor<'_>,
    item: &LeasedItem,
    lease: Duration,
) -> Result<bool, sqlx::Error> {
    let lease = lease.clamp(Duration::from_secs(1), MAX_LEASE);
    let renewed = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = now() + make_interval(secs => $5)
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $6
           AND lease_token = $4
           AND lease_expires_at > now()"
    ))
    .bind(item.campaign_id)
    .bind(SubjectKind::Sandbox.as_str())
    .bind(item.sandbox_id)
    .bind(item.lease_token)
    .bind(lease.as_secs_f64())
    .bind(ItemPhase::InFlight.as_str())
    .execute(executor)
    .await?
    .rows_affected();
    Ok(renewed == 1)
}

/// How a leased item ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ItemOutcome {
    /// Upgraded, or already on the target.
    Done,
    /// Not upgraded, and not a failure: the subject is gone or no longer
    /// one this campaign moves.
    Skipped(ItemError),
    /// Failed; counts against the failure budget.
    Failed(ItemError),
    /// Not upgraded this time; back to pending for a later lease. The lease
    /// counts as an attempt.
    Retry(ItemError),
    /// Not attempted: the subject wasn't ready (a sandbox mid-transition,
    /// say). Back to pending, and the lease doesn't count as an attempt.
    Release(ItemError),
}

impl ItemOutcome {
    fn phase(&self) -> ItemPhase {
        match self {
            Self::Done => ItemPhase::Done,
            Self::Skipped(_) => ItemPhase::Skipped,
            Self::Failed(_) => ItemPhase::Failed,
            Self::Retry(_) | Self::Release(_) => ItemPhase::Pending,
        }
    }

    /// Attempts to give back: a release undoes its lease's.
    const fn refunded_attempts(&self) -> i64 {
        match self {
            Self::Release(_) => 1,
            Self::Done | Self::Skipped(_) | Self::Failed(_) | Self::Retry(_) => 0,
        }
    }

    fn error(&self) -> Option<&str> {
        match self {
            Self::Done => None,
            Self::Skipped(error)
            | Self::Failed(error)
            | Self::Retry(error)
            | Self::Release(error) => error.as_deref(),
        }
    }
}

/// Record `outcome` for `item` and release its lease. `false` when the lease
/// is no longer the caller's, in which case nothing is recorded. An expired
/// lease that hasn't been re-driven still records: nobody else holds the
/// item, and refusing the outcome of finished work would only make a later
/// lease redo it.
pub async fn finish_item(
    executor: impl sqlx::PgExecutor<'_>,
    item: &LeasedItem,
    outcome: &ItemOutcome,
) -> Result<bool, sqlx::Error> {
    let finished = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET phase = $5,
             last_error = $6,
             attempts = GREATEST(attempts - $8, 0),
             lease_token = NULL,
             lease_expires_at = NULL
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $7
           AND lease_token = $4"
    ))
    .bind(item.campaign_id)
    .bind(SubjectKind::Sandbox.as_str())
    .bind(item.sandbox_id)
    .bind(item.lease_token)
    .bind(outcome.phase().as_str())
    .bind(outcome.error())
    .bind(ItemPhase::InFlight.as_str())
    .bind(outcome.refunded_attempts())
    .execute(executor)
    .await?
    .rows_affected();
    Ok(finished == 1)
}

/// What [`redrive_expired`] did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RedriveReport {
    /// Items returned to pending for another lease.
    pub requeued: u64,
    /// Items that had used up their attempts, now failed.
    pub failed: u64,
}

/// The `last_error` of an item whose lease lapsed.
pub(crate) const LEASE_EXPIRED: &str = "the lease expired before an outcome was recorded";

/// Re-drive `campaign_id`'s items stuck `in_flight` past their lease: back to
/// pending, or to failed once they've been leased `max_attempts` times. The
/// upgrade is idempotent, so a later lease redoes it safely, and the lapsed
/// worker can no longer record an outcome.
pub async fn redrive_expired(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
    max_attempts: u64,
) -> Result<RedriveReport, sqlx::Error> {
    let phases = sqlx::query_scalar::<_, String>(sql!(
        "UPDATE runtime_upgrade_items
         SET phase = CASE WHEN attempts >= $2 THEN $3 ELSE $4 END,
             last_error = $5,
             lease_token = NULL,
             lease_expires_at = NULL
         WHERE campaign_id = $1
           AND phase = $6
           AND lease_expires_at < now()
         RETURNING phase"
    ))
    .bind(campaign_id)
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .bind(ItemPhase::Failed.as_str())
    .bind(ItemPhase::Pending.as_str())
    .bind(LEASE_EXPIRED)
    .bind(ItemPhase::InFlight.as_str())
    .fetch_all(executor)
    .await?;
    let failed = phases
        .iter()
        .filter(|phase| phase.as_str() == ItemPhase::Failed.as_str())
        .count() as u64;
    Ok(RedriveReport {
        requeued: phases.len() as u64 - failed,
        failed,
    })
}

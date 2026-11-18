//! Item verbs: recording a driver's subjects, leasing them under a
//! campaign's pacing, and recording outcomes under the lease. Every verb
//! takes or reads the subject kind, so one set of verbs serves every
//! campaign kind; only the lease's node pacing differs by kind
//! ([`SubjectKind::has_node`]).

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_accounts::NodeId;
use pplx_space_sql_locator::sql;
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{
    CampaignId, CampaignKind, CampaignPhase, ItemError, ItemPhase, SubjectKind, clamp_lease,
    end_transaction,
};

/// Use up the leases of `campaign_id`'s skipped `subject_kind` items for
/// `subject_ids`, which the caller's driver found off the campaign's source,
/// so a driver that excludes items out of attempts doesn't find them again.
/// `attempts` becomes `max_attempts`, as for a subject the node kept
/// declining. Items in other phases are left alone. Returns how many were
/// retired. Retirement is permanent.
pub async fn retire_skipped(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    subject_ids: &[uuid::Uuid],
    max_attempts: u64,
) -> Result<u64, sqlx::Error> {
    if subject_ids.is_empty() {
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
    .bind(subject_kind.as_str())
    .bind(subject_ids)
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .bind(ItemPhase::Skipped.as_str())
    .execute(executor)
    .await?
    .rows_affected())
}

/// Record `subject_ids` as pending `subject_kind` items of `campaign_id`. An
/// item that was skipped (its sandbox had started when it was leased, say) is
/// pending again while it has leases left of `max_attempts`, keeping its
/// count, so a subject the node keeps declining is dispatched a bounded
/// number of times. Items in any other phase are left alone. Inserts nothing
/// unless the campaign is unfinished and of the kind whose subjects these
/// are, which it locks for the insert so a concurrent completion can't finish
/// it under new items. Returns how many items were added or made pending
/// again. Opens a transaction, or a savepoint inside the caller's.
pub async fn insert_pending(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    subject_ids: &[uuid::Uuid],
    max_attempts: u64,
) -> Result<u64, sqlx::Error> {
    if subject_ids.is_empty() {
        return Ok(0);
    }
    let mut tx = conn.begin().await?;
    let inserted = insert_pending_in(
        &mut tx,
        campaign_id,
        subject_kind,
        subject_ids,
        max_attempts,
    )
    .await;
    end_transaction(tx, inserted).await
}

async fn insert_pending_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    subject_ids: &[uuid::Uuid],
    max_attempts: u64,
) -> Result<u64, sqlx::Error> {
    let kinds: Vec<&str> = CampaignKind::ALL
        .iter()
        .filter(|kind| kind.subject_kind() == subject_kind)
        .map(|kind| kind.as_str())
        .collect();
    let open = sqlx::query_scalar::<_, i32>(sql!(
        "SELECT 1
         FROM runtime_upgrade_campaigns
         WHERE id = $1
           AND kind = ANY($2)
           AND finished_at IS NULL
         FOR SHARE"
    ))
    .bind(campaign_id)
    .bind(&kinds)
    .fetch_optional(&mut *tx)
    .await?
    .is_some();
    if !open {
        return Ok(0);
    }
    Ok(sqlx::query(sql!(
        "INSERT INTO runtime_upgrade_items (campaign_id, subject_kind, subject_id, phase)
         SELECT DISTINCT $1, $2, subject_id, $4
         FROM unnest($3::uuid[]) AS subject_id
         ON CONFLICT (campaign_id, subject_kind, subject_id) DO UPDATE
         SET phase = EXCLUDED.phase,
             last_error = NULL
         WHERE runtime_upgrade_items.phase = $5
           AND runtime_upgrade_items.attempts < $6"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(subject_ids)
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
    pub subject_kind: SubjectKind,
    /// The sandbox or template release id, as `subject_kind` says.
    pub subject_id: uuid::Uuid,
    /// The token the outcome must be recorded under.
    pub lease_token: uuid::Uuid,
    /// Leases taken on this item so far, this one included.
    pub attempts: u64,
    /// For a kind whose subjects have a node, the node that last published
    /// the subject, if it still has a published state. Always `None` for
    /// other kinds.
    pub node_id: Option<NodeId>,
}

/// How many items a canary leases: one at a time, until one is done.
const CANARY_IN_FLIGHT: u64 = 1;

/// Lease pending items of `campaign_id` for `lease` (clamped to 1 second
/// through [`MAX_LEASE`]), up to the campaign's pacing:
///
/// * nothing unless the campaign is in `canary` or `rolling` and its failed
///   items haven't reached its failure budget;
/// * in `canary`, one item at a time, and nothing once one is done: the
///   canary then waits for an operator to resume the campaign;
/// * at most `max_in_flight_per_cluster` items in flight in all, counting
///   those already leased;
/// * for a kind whose subjects have a node ([`SubjectKind::has_node`]), at
///   most `max_in_flight_per_node` in flight on any node, by the node that
///   last published each sandbox, and only on a node that's reachable and
///   whose status is evidence of its environments
///   ([`pplx_space_node_status_store::environment_evidence`]). An item on
///   any other node stays pending, holding the campaign open, until its node
///   qualifies again. An item with no published node counts toward the
///   cluster cap only and is leased: the driver finds it on neither end of
///   the campaign and skips it, where pending it would hold the campaign open
///   for good. Items of other kinds have no node and are paced by the cluster
///   cap only.
///
/// * never a subject another campaign holds in flight under a live lease
///   (an aborted campaign's, while its worker may still be on it); the item
///   stays pending until that lease lapses.
///
/// Only the first `scan_limit` pending items in subject order are
/// considered, so items on a saturated node can hold back items behind them
/// until the node drains.
///
/// Each lease runs from the moment it's written (`clock_timestamp()`), not
/// from the transaction's start, so time spent waiting for the locks below
/// doesn't use it up.
///
/// Locks the campaign row for the whole lease, so concurrent leasers are
/// serialized and a pause committed first is seen. Opens a transaction, or a
/// savepoint inside the caller's.
pub async fn lease_items(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    lease: Duration,
    scan_limit: u32,
) -> Result<Vec<LeasedItem>, LeaseError> {
    let lease = clamp_lease(lease);
    let mut tx = conn.begin().await?;
    let leased = lease_in(&mut tx, campaign_id, lease, scan_limit).await;
    end_transaction(tx, leased).await
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error(transparent)]
    UnknownValue(#[from] crate::UnknownValue),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// What the lease read of the campaign under its lock.
struct Pacing {
    subject_kind: SubjectKind,
    per_node: u64,
    per_cluster: u64,
}

async fn lease_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    lease: Duration,
    scan_limit: u32,
) -> Result<Vec<LeasedItem>, LeaseError> {
    let leasing: Vec<&str> = CampaignPhase::ALL
        .iter()
        .filter(|phase| phase.leases())
        .map(|phase| phase.as_str())
        .collect();
    let Some((kind, canary, per_node, per_cluster, failure_budget)) =
        sqlx::query_as::<_, (String, bool, i64, i64, i64)>(sql!(
            "SELECT kind, phase = $2, max_in_flight_per_node, max_in_flight_per_cluster,
                    failure_budget
             FROM runtime_upgrade_campaigns
             WHERE id = $1
               AND phase = ANY($3)
             FOR UPDATE"
        ))
        .bind(campaign_id)
        .bind(CampaignPhase::Canary.as_str())
        .bind(&leasing)
        .fetch_optional(&mut *tx)
        .await?
    else {
        return Ok(Vec::new());
    };
    let subject_kind = kind.parse::<CampaignKind>()?.subject_kind();
    // Re-checked under the lock: a failure committed since the caller's
    // budget check doesn't let another batch out. Outcomes take no campaign
    // lock, so failures among items already in flight can still overshoot
    // the budget by up to their number.
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
    let mut pacing = Pacing {
        subject_kind,
        per_node: u64::try_from(per_node).unwrap_or(0),
        per_cluster: u64::try_from(per_cluster).unwrap_or(0),
    };
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
        pacing.per_cluster = pacing.per_cluster.min(CANARY_IN_FLIGHT);
    }
    let (in_flight, pending) = if subject_kind.has_node() {
        node_bound_candidates(tx, campaign_id, subject_kind, scan_limit).await?
    } else {
        nodeless_candidates(tx, campaign_id, subject_kind, scan_limit).await?
    };
    let held_elsewhere = held_by_other_campaigns(tx, campaign_id, subject_kind).await?;
    let pending = pending
        .into_iter()
        .filter(|(subject_id, _)| !held_elsewhere.contains(subject_id))
        .collect();
    lease_within(tx, campaign_id, lease, &pacing, in_flight, pending).await
}

/// The `subject_kind` subjects another campaign holds in flight under a live
/// lease, which this lease must skip: one subject is upgraded by one worker
/// at a time, whichever campaigns name it. The case that needs it is an
/// aborted campaign, whose worker may still be mid-upgrade until its next
/// renewal fails ([`renew_lease`]), while a new campaign for the same source
/// leases the same subjects.
///
/// Serialized per subject kind by a transaction-scoped advisory lock, taken
/// after the campaign row lock like every leaser takes it, so two campaigns'
/// leases can't both read a subject as free and both take it.
async fn held_by_other_campaigns(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
) -> Result<std::collections::HashSet<uuid::Uuid>, sqlx::Error> {
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(format!("runtime_upgrade_items/lease/{subject_kind}"))
    .execute(&mut *tx)
    .await?;
    // Campaigns first, so each one's in-flight items come from the
    // (campaign_id, phase, subject_id) index. A completed campaign has none:
    // completion needs no item in flight, under the lock leasers take.
    let held = sqlx::query_scalar::<_, uuid::Uuid>(sql!(
        "SELECT i.subject_id
         FROM runtime_upgrade_campaigns c
         JOIN runtime_upgrade_items i ON i.campaign_id = c.id AND i.phase = $3
         WHERE c.id <> $1
           AND c.phase <> $4
           AND i.subject_kind = $2
           AND i.lease_expires_at > now()"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(ItemPhase::InFlight.as_str())
    .bind(CampaignPhase::Completed.as_str())
    .fetch_all(&mut *tx)
    .await?;
    Ok(held.into_iter().collect())
}

/// In-flight counts by node, and the pending items to consider with their
/// nodes.
type Candidates = (
    Vec<(Option<NodeId>, i64)>,
    Vec<(uuid::Uuid, Option<NodeId>)>,
);

/// For sandbox items: in flight by the node that last published each
/// sandbox, and pending items whose node qualifies or that have none.
async fn node_bound_candidates(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    scan_limit: u32,
) -> Result<Candidates, sqlx::Error> {
    debug_assert_eq!(subject_kind, SubjectKind::Sandbox);
    let in_flight = sqlx::query_as::<_, (Option<NodeId>, i64)>(sql!(
        "SELECT o.node_id, count(*)
         FROM runtime_upgrade_items i
         LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
         WHERE i.campaign_id = $1
           AND i.subject_kind = $2
           AND i.phase = $3
         GROUP BY o.node_id"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(ItemPhase::InFlight.as_str())
    .fetch_all(&mut *tx)
    .await?;
    // A sandbox published on a node is leased only while that node is
    // reachable and its status is evidence of its environments. One with no
    // published node is leased, for the driver to skip.
    /// Built once: the node evidence predicate is the only interpolation.
    static PENDING: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "SELECT i.subject_id, o.node_id
             FROM runtime_upgrade_items i
             LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
             LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
             WHERE i.campaign_id = $1
               AND i.subject_kind = $2
               AND i.phase = $3
               AND (o.node_id IS NULL OR (nss.reachable IS TRUE AND {}))
             ORDER BY i.subject_id
             LIMIT $4",
            pplx_space_node_status_store::environment_evidence("nss")
        ))
    });
    let pending =
        sqlx::query_as::<_, (uuid::Uuid, Option<NodeId>)>(sqlx::AssertSqlSafe(PENDING.as_str()))
            .bind(campaign_id)
            .bind(subject_kind.as_str())
            .bind(ItemPhase::Pending.as_str())
            .bind(i64::from(scan_limit))
            .fetch_all(&mut *tx)
            .await?;
    Ok((in_flight, pending))
}

/// For items with no node: the in-flight count and the first pending items.
async fn nodeless_candidates(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    scan_limit: u32,
) -> Result<Candidates, sqlx::Error> {
    let in_flight = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*)
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND phase = $3"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(ItemPhase::InFlight.as_str())
    .fetch_one(&mut *tx)
    .await?;
    let pending = sqlx::query_scalar::<_, uuid::Uuid>(sql!(
        "SELECT subject_id
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND phase = $3
         ORDER BY subject_id
         LIMIT $4"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(ItemPhase::Pending.as_str())
    .bind(i64::from(scan_limit))
    .fetch_all(&mut *tx)
    .await?;
    Ok((
        vec![(None, in_flight)],
        pending.into_iter().map(|id| (id, None)).collect(),
    ))
}

async fn lease_within(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    lease: Duration,
    pacing: &Pacing,
    in_flight: Vec<(Option<NodeId>, i64)>,
    pending: Vec<(uuid::Uuid, Option<NodeId>)>,
) -> Result<Vec<LeasedItem>, LeaseError> {
    let mut on_node: HashMap<NodeId, u64> = HashMap::new();
    let mut in_cluster = 0_u64;
    for (node_id, count) in in_flight {
        let count = u64::try_from(count).unwrap_or(0);
        in_cluster += count;
        if let Some(node_id) = node_id {
            *on_node.entry(node_id).or_default() += count;
        }
    }
    let mut leased = Vec::new();
    for (subject_id, node_id) in pending {
        if in_cluster >= pacing.per_cluster {
            break;
        }
        if let Some(node_id) = node_id
            && on_node.get(&node_id).copied().unwrap_or(0) >= pacing.per_node
        {
            continue;
        }
        let lease_token = uuid::Uuid::now_v7();
        let attempts = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE runtime_upgrade_items
             SET phase = $4,
                 lease_token = $5,
                 lease_expires_at = clock_timestamp() + make_interval(secs => $6),
                 attempts = attempts + 1
             WHERE campaign_id = $1
               AND subject_kind = $2
               AND subject_id = $3
               AND phase = $7
             RETURNING attempts"
        ))
        .bind(campaign_id)
        .bind(pacing.subject_kind.as_str())
        .bind(subject_id)
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
            subject_kind: pacing.subject_kind,
            subject_id,
            lease_token,
            attempts: u64::try_from(attempts).unwrap_or(0),
            node_id,
        });
    }
    Ok(leased)
}

/// Extend `item`'s lease to `lease` from now (clamped as for
/// [`lease_items`]). `false` when the lease is no longer the caller's: it
/// expired on the database clock, it was re-driven, another outcome was
/// recorded, or its campaign finished (an operator aborted it). The caller
/// then stops working on the item.
///
/// A worker renews at a third of its lease, so after an abort it stops
/// at its next renewal attempt, a third of a lease after its last renewal.
/// If that renewal can't reach the database it stops at its own deadline
/// instead, which is never later than the database's lease. Either way it
/// stops before the lease lapses, and until it lapses [`lease_items`] gives
/// the subject to no other campaign.
pub async fn renew_lease(
    executor: impl sqlx::PgExecutor<'_>,
    item: &LeasedItem,
    lease: Duration,
) -> Result<bool, sqlx::Error> {
    let lease = clamp_lease(lease);
    let renewed = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET lease_expires_at = clock_timestamp() + make_interval(secs => $5)
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $6
           AND lease_token = $4
           AND lease_expires_at > clock_timestamp()
           AND EXISTS (
               SELECT 1
               FROM runtime_upgrade_campaigns c
               WHERE c.id = $1
                 AND c.finished_at IS NULL)"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
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
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
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

/// Return the in-flight items of finished campaigns whose leases lapsed to
/// pending, as [`redrive_expired`] does for an unfinished one. Only an
/// aborted campaign can have one: completion needs no item in flight. An aborted
/// campaign keeps an item in flight until its worker's lease lapses, so no
/// other campaign leases the subject while that worker may still be on it
/// ([`lease_items`]); this tidies up after. Returns how many it released.
pub async fn release_lapsed_in_finished(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET phase = $2,
             last_error = $3,
             lease_token = NULL,
             lease_expires_at = NULL
         WHERE phase = $1
           AND lease_expires_at < now()
           AND campaign_id IN (
               SELECT id
               FROM runtime_upgrade_campaigns
               WHERE phase = $4)"
    ))
    .bind(ItemPhase::InFlight.as_str())
    .bind(ItemPhase::Pending.as_str())
    .bind(LEASE_EXPIRED)
    .bind(CampaignPhase::Aborted.as_str())
    .execute(executor)
    .await?
    .rows_affected())
}

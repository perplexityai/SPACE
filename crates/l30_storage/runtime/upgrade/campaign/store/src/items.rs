//! Item verbs: recording a driver's subjects, leasing them under a
//! campaign's pacing, and recording outcomes under the lease. Every verb
//! takes or reads the subject kind, so one set of verbs serves every
//! campaign kind; only the lease's node pacing differs by kind
//! ([`SubjectKind::has_node`]). Leasing and renewal run the steps of
//! [`pplx_space_rollout_queue::claim`] and [`pplx_space_rollout_queue::renew`]
//! on these tables, and an outcome or a lapse does what
//! [`pplx_space_rollout_queue::Outcome`] says.

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

use pplx_space_accounts::NodeId;
pub(crate) use pplx_space_rollout_queue::LEASE_EXPIRED;
pub use pplx_space_rollout_queue::RedriveReport;
use pplx_space_rollout_queue::{Candidates, ClaimStore, Outcome, Pacing, RenewStore};
use pplx_space_sql_locator::sql;
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{
    CampaignId, CampaignKind, CampaignPhase, ItemError, ItemPhase, SubjectKind, end_transaction,
};

/// `attempts` of a retired item: above any `max_attempts` a caller passes,
/// so no limit, even one a later release raises, makes it pending again.
pub const RETIRED_ATTEMPTS: u64 = i64::MAX as u64;

/// Use up the leases of `campaign_id`'s skipped `subject_kind` items for
/// `subject_ids`, which the caller's driver found off the campaign's source,
/// so a driver that excludes items out of attempts doesn't find them again.
/// `attempts` becomes [`RETIRED_ATTEMPTS`]. Items in other phases are left
/// alone. Returns how many were retired. Retirement is permanent:
/// [`insert_pending`] never makes a retired item pending again, whatever
/// `max_attempts` it's given.
pub async fn retire_skipped(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    subject_ids: &[uuid::Uuid],
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
    .bind(RETIRED_ATTEMPTS as i64)
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
/// again. Opens a transaction, or a savepoint inside the caller's; inside
/// the caller's, its share lock on the campaign row lasts until the caller's
/// transaction ends. So don't lease the campaign later in that transaction:
/// the lease's row lock would upgrade the share lock, and two callers doing
/// both would each wait for the other's share lock.
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

/// Lease pending items of `campaign_id` for `lease` (clamped to 1 second
/// through [`MAX_LEASE`](crate::MAX_LEASE)), up to the campaign's pacing:
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
/// serialized and a pause committed first is seen, and then the lease
/// handoff lock of the campaign's subject kind, which renewals of that kind
/// wait on. Opens a transaction, or a savepoint inside the caller's; inside
/// the caller's, the locks last until the caller's transaction ends. So
/// don't lease a second campaign of the same kind inside a transaction that
/// already leased one: it would take that campaign's row lock after the
/// handoff lock, and could deadlock with a lease of the second campaign
/// that holds its row and waits for the handoff lock. Nor lease a campaign
/// inside a transaction that inserted its items: see [`insert_pending`].
/// Nor lease campaigns of both subject kinds in one transaction: two callers
/// taking the kinds' handoff locks in opposite orders deadlock. Nor lease a
/// campaign in a transaction that already finished one of its items: that
/// transaction holds the item row, which a concurrent renewal waits on while
/// it holds the campaign's share lock that the lease waits on. Call each verb
/// in a transaction of its own. Run it at `READ COMMITTED`: a caller's `REPEATABLE READ` snapshot would predate
/// the handoff lock, so a renewal committed while this lease waited for it
/// would go unseen.
pub async fn lease_items(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    lease: Duration,
    scan_limit: u32,
) -> Result<Vec<LeasedItem>, LeaseError> {
    let mut tx = conn.begin().await?;
    let mut leasing = Leasing {
        tx: &mut tx,
        campaign_id,
    };
    let leased = pplx_space_rollout_queue::claim(&mut leasing, lease, scan_limit).await;
    end_transaction(tx, leased).await
}

#[derive(Debug, thiserror::Error)]
pub enum LeaseError {
    #[error(transparent)]
    UnknownValue(#[from] crate::UnknownValue),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// [`lease_items`]'s steps.
struct Leasing<'c> {
    tx: &'c mut PgConnection,
    campaign_id: CampaignId,
}

impl ClaimStore for Leasing<'_> {
    /// The kind of subject the campaign's items name.
    type Header = SubjectKind;
    type Subject = uuid::Uuid;
    type Node = NodeId;
    type Claimed = LeasedItem;
    type Error = LeaseError;

    async fn lock_leasable(&mut self) -> Result<Option<(SubjectKind, Pacing)>, LeaseError> {
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
            .bind(self.campaign_id)
            .bind(CampaignPhase::Canary.as_str())
            .bind(&leasing)
            .fetch_optional(&mut *self.tx)
            .await?
        else {
            return Ok(None);
        };
        let subject_kind = kind.parse::<CampaignKind>()?.subject_kind();
        Ok(Some((
            subject_kind,
            Pacing {
                canary,
                per_node: u64::try_from(per_node).unwrap_or(0),
                per_cluster: u64::try_from(per_cluster).unwrap_or(0),
                failure_budget: u64::try_from(failure_budget).unwrap_or(0),
            },
        )))
    }

    async fn canary_progress(&mut self) -> Result<(bool, u64), LeaseError> {
        let (canary_done, canary_in_flight) = sqlx::query_as::<_, (bool, i64)>(sql!(
            "SELECT coalesce(bool_or(phase = $2), false), count(*) FILTER (WHERE phase = $3)
             FROM runtime_upgrade_items
             WHERE campaign_id = $1
               AND phase = ANY(ARRAY[$2, $3])"
        ))
        .bind(self.campaign_id)
        .bind(ItemPhase::Done.as_str())
        .bind(ItemPhase::InFlight.as_str())
        .fetch_one(&mut *self.tx)
        .await?;
        Ok((canary_done, u64::try_from(canary_in_flight).unwrap_or(0)))
    }

    async fn lock_handoff(&mut self, subject_kind: &SubjectKind) -> Result<(), LeaseError> {
        Ok(lock_lease_handoff(self.tx, *subject_kind, Handoff::Lease).await?)
    }

    async fn held_elsewhere(
        &mut self,
        subject_kind: &SubjectKind,
    ) -> Result<HashSet<uuid::Uuid>, LeaseError> {
        Ok(held_by_other_campaigns(self.tx, self.campaign_id, *subject_kind).await?)
    }

    async fn candidates(
        &mut self,
        subject_kind: &SubjectKind,
        scan_limit: u32,
    ) -> Result<ItemCandidates, LeaseError> {
        Ok(if subject_kind.has_node() {
            node_bound_candidates(self.tx, self.campaign_id, *subject_kind, scan_limit).await?
        } else {
            nodeless_candidates(self.tx, self.campaign_id, *subject_kind, scan_limit).await?
        })
    }

    async fn failed(&mut self) -> Result<u64, LeaseError> {
        Ok(crate::phase::count_failed(self.tx, self.campaign_id).await?)
    }

    async fn claim(
        &mut self,
        subject_kind: &SubjectKind,
        subject_id: uuid::Uuid,
        node_id: Option<NodeId>,
        lease_token: uuid::Uuid,
        lease: Duration,
    ) -> Result<Option<LeasedItem>, LeaseError> {
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
        .bind(self.campaign_id)
        .bind(subject_kind.as_str())
        .bind(subject_id)
        .bind(ItemPhase::InFlight.as_str())
        .bind(lease_token)
        .bind(lease.as_secs_f64())
        .bind(ItemPhase::Pending.as_str())
        .fetch_optional(&mut *self.tx)
        .await?;
        Ok(attempts.map(|attempts| LeasedItem {
            campaign_id: self.campaign_id,
            subject_kind: *subject_kind,
            subject_id,
            lease_token,
            attempts: u64::try_from(attempts).unwrap_or(0),
            node_id,
        }))
    }
}

/// The `subject_kind` subjects another campaign holds in flight under a live
/// lease, which this lease must skip: one subject is upgraded by one worker
/// at a time, whichever campaigns name it. The case that needs it is an
/// aborted campaign, whose worker may still be mid-upgrade until its next
/// renewal fails ([`renew_lease`]), while a new campaign for the same source
/// leases the same subjects.
///
/// Read after [`lock_lease_handoff`], which serializes it per subject kind
/// and is taken after the campaign row lock like every leaser takes it, so
/// two campaigns' leases can't both read a subject as free and both take it.
async fn held_by_other_campaigns(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
) -> Result<HashSet<uuid::Uuid>, sqlx::Error> {
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
           AND i.lease_expires_at > clock_timestamp()"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(ItemPhase::InFlight.as_str())
    .bind(CampaignPhase::Completed.as_str())
    .fetch_all(&mut *tx)
    .await?;
    Ok(held.into_iter().collect())
}

/// Who takes the lease handoff lock, which decides its mode.
#[derive(Clone, Copy)]
enum Handoff {
    /// A lease: exclusive, so leases of one kind serialize with each other
    /// and with renewals.
    Lease,
    /// A renewal: shared, so renewals don't block each other. A renewal
    /// that arrives while a lease waits for the lock queues behind that
    /// lease, though, as Postgres queues a shared request behind a waiting
    /// exclusive one.
    Renewal,
}

/// Take the transaction-scoped advisory lock that hands `subject_kind`'s
/// subjects between campaigns. Leases take it exclusively and renewals
/// shared, so a lease reads another campaign's lease either before a
/// renewal or after the renewal commits, never a renewal that's written but
/// not yet committed.
async fn lock_lease_handoff(
    tx: &mut PgConnection,
    subject_kind: SubjectKind,
    handoff: Handoff,
) -> Result<(), sqlx::Error> {
    let statement = match handoff {
        Handoff::Lease => sql!("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"),
        Handoff::Renewal => {
            sql!("SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))")
        }
    };
    sqlx::query(statement)
        .bind(format!("runtime_upgrade_items/lease/{subject_kind}"))
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// In-flight counts by node, and the pending items to consider with their
/// nodes.
type ItemCandidates = Candidates<uuid::Uuid, NodeId>;

/// For sandbox items: in flight by the node that last published each
/// sandbox, and pending items whose node qualifies or that have none.
async fn node_bound_candidates(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    scan_limit: u32,
) -> Result<ItemCandidates, sqlx::Error> {
    debug_assert_eq!(subject_kind, SubjectKind::Sandbox);
    // One statement, so one snapshot: the in-flight counts and the pending
    // items see each sandbox on the same node. Read in two, a sandbox
    // published on another node in between could be counted on its old node
    // while a pending item on its new one is leased past the cap. A sandbox
    // a node publishes after this read is seen by the next lease.
    //
    // A sandbox published on a node is leased only while that node is
    // reachable and its status is evidence of its environments. One with no
    // published node is leased, for the driver to skip.
    /// Built once: the node evidence predicate is the only interpolation.
    static CANDIDATES: LazyLock<String> = LazyLock::new(|| {
        sql!(format!(
            "WITH in_flight AS (
                 SELECT o.node_id, count(*) AS items
                 FROM runtime_upgrade_items i
                 LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
                 WHERE i.campaign_id = $1
                   AND i.subject_kind = $2
                   AND i.phase = $5
                 GROUP BY o.node_id
             ),
             pending AS (
                 SELECT i.subject_id, o.node_id
                 FROM runtime_upgrade_items i
                 LEFT JOIN sandbox_observed_states o ON o.sandbox_id = i.subject_id
                 LEFT JOIN node_status_snapshots nss ON nss.node_id = o.node_id
                 WHERE i.campaign_id = $1
                   AND i.subject_kind = $2
                   AND i.phase = $3
                   AND (o.node_id IS NULL OR (nss.reachable IS TRUE AND {}))
                 ORDER BY i.subject_id
                 LIMIT $4
             )
             SELECT node_id, items, NULL::uuid AS subject_id FROM in_flight
             UNION ALL
             SELECT node_id, NULL::bigint, subject_id FROM pending",
            pplx_space_node_status_store::environment_evidence("nss")
        ))
    });
    let rows = sqlx::query_as::<_, (Option<NodeId>, Option<i64>, Option<uuid::Uuid>)>(
        sqlx::AssertSqlSafe(CANDIDATES.as_str()),
    )
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(ItemPhase::Pending.as_str())
    .bind(i64::from(scan_limit))
    .bind(ItemPhase::InFlight.as_str())
    .fetch_all(&mut *tx)
    .await?;
    let mut in_flight = Vec::new();
    let mut pending = Vec::new();
    for (node_id, items, subject_id) in rows {
        match (items, subject_id) {
            // count(*) is never negative.
            (Some(items), _) => in_flight.push((node_id, u64::try_from(items).unwrap_or(0))),
            (None, Some(subject_id)) => pending.push((subject_id, node_id)),
            (None, None) => {}
        }
    }
    // UNION ALL doesn't promise the branch's order.
    pending.sort_unstable_by_key(|(subject_id, _)| *subject_id);
    Ok(Candidates { in_flight, pending })
}

/// For items with no node: the in-flight count and the first pending items.
async fn nodeless_candidates(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    scan_limit: u32,
) -> Result<ItemCandidates, sqlx::Error> {
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
    Ok(Candidates {
        // count(*) is never negative.
        in_flight: vec![(None, u64::try_from(in_flight).unwrap_or(0))],
        pending: pending.into_iter().map(|id| (id, None)).collect(),
    })
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
///
/// Takes the lease handoff lock [`lease_items`] takes, shared, so a lease of
/// the same subject for another campaign waits for the renewal to commit,
/// and then sees the renewed lease. Without it, a renewal inside a caller's
/// transaction that stays open past the old expiry would be invisible to that
/// lease, and both campaigns would hold the subject once it committed. It
/// takes a share lock on the campaign row first, in the lock order every
/// leaser follows, so it waits out a lease or a phase change of the same
/// campaign and reads whether the campaign has finished exactly. Opens a
/// transaction, or a savepoint inside the caller's. Give it a transaction of
/// its own: inside a caller's, its locks last until the caller's transaction
/// ends, holding off every lease of the subject kind, and a campaign row
/// lock or a renewal of another campaign taken after it in the same
/// transaction can deadlock with a concurrent lease.
pub async fn renew_lease(
    conn: impl Acquire<'_, Database = Postgres>,
    item: &LeasedItem,
    lease: Duration,
) -> Result<bool, sqlx::Error> {
    let mut tx = conn.begin().await?;
    let mut renewing = Renewing { tx: &mut tx, item };
    let renewed = pplx_space_rollout_queue::renew(&mut renewing, lease).await;
    end_transaction(tx, renewed).await
}

/// [`renew_lease`]'s steps.
struct Renewing<'c, 'i> {
    tx: &'c mut PgConnection,
    item: &'i LeasedItem,
}

impl RenewStore for Renewing<'_, '_> {
    type Error = sqlx::Error;

    async fn lock_unfinished(&mut self) -> Result<bool, sqlx::Error> {
        // Shared, so renewals of one campaign don't wait on each other here.
        let unfinished = sqlx::query_scalar::<_, i32>(sql!(
            "SELECT 1
             FROM runtime_upgrade_campaigns
             WHERE id = $1
               AND finished_at IS NULL
             FOR SHARE"
        ))
        .bind(self.item.campaign_id)
        .fetch_optional(&mut *self.tx)
        .await?;
        Ok(unfinished.is_some())
    }

    async fn lock_handoff(&mut self) -> Result<(), sqlx::Error> {
        lock_lease_handoff(self.tx, self.item.subject_kind, Handoff::Renewal).await
    }

    async fn lock_item(&mut self) -> Result<(), sqlx::Error> {
        // insert_pending's conflict check, say, locks a row without
        // updating it.
        sqlx::query(sql!(
            "SELECT 1
             FROM runtime_upgrade_items
             WHERE campaign_id = $1
               AND subject_kind = $2
               AND subject_id = $3
             FOR UPDATE"
        ))
        .bind(self.item.campaign_id)
        .bind(self.item.subject_kind.as_str())
        .bind(self.item.subject_id)
        .fetch_optional(&mut *self.tx)
        .await?;
        Ok(())
    }

    async fn extend(&mut self, lease: Duration) -> Result<bool, sqlx::Error> {
        let renewed = sqlx::query(sql!(
            "UPDATE runtime_upgrade_items
             SET lease_expires_at = clock_timestamp() + make_interval(secs => $5)
             WHERE campaign_id = $1
               AND subject_kind = $2
               AND subject_id = $3
               AND phase = $6
               AND lease_token = $4
               AND lease_expires_at > clock_timestamp()"
        ))
        .bind(self.item.campaign_id)
        .bind(self.item.subject_kind.as_str())
        .bind(self.item.subject_id)
        .bind(self.item.lease_token)
        .bind(lease.as_secs_f64())
        .bind(ItemPhase::InFlight.as_str())
        .execute(&mut *self.tx)
        .await?
        .rows_affected();
        Ok(renewed == 1)
    }
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
    /// counts as an attempt. The store doesn't cap retries: a caller records
    /// a retry on the item's last attempt ([`LeasedItem::attempts`]) as
    /// [`Self::Failed`] instead.
    Retry(ItemError),
    /// Not attempted: the subject wasn't ready (a sandbox mid-transition,
    /// say). Back to pending, and the lease doesn't count as an attempt.
    Release(ItemError),
}

impl ItemOutcome {
    const fn kind(&self) -> Outcome {
        match self {
            Self::Done => Outcome::Done,
            Self::Skipped(_) => Outcome::Skipped,
            Self::Failed(_) => Outcome::Failed,
            Self::Retry(_) => Outcome::Retry,
            Self::Release(_) => Outcome::Release,
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
    let outcome_kind = outcome.kind();
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
    .bind(ItemPhase::from_queue(outcome_kind.state()).as_str())
    .bind(outcome.error())
    .bind(ItemPhase::InFlight.as_str())
    .bind(i64::try_from(outcome_kind.refunded_attempts()).unwrap_or(i64::MAX))
    .execute(executor)
    .await?
    .rows_affected();
    Ok(finished == 1)
}

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
           AND lease_expires_at <= clock_timestamp()
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
           AND lease_expires_at <= clock_timestamp()
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

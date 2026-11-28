//! Campaign phase changes the reconciler makes on its own: pausing at the
//! failure budget and completing a drained campaign. Operator changes are in
//! [`crate::admin`].
//!
//! Each verb locks the campaign row before it writes, in the down
//! migration's order, only moves a campaign from the phase it checked, so a
//! phase an operator wrote first wins. The schema's trigger raises the
//! generation in the same statement. Both run the steps of
//! [`pplx_space_rollout_queue::pause_if_over_budget`] and
//! [`pplx_space_rollout_queue::complete_if_drained`] on these tables.

use pplx_space_postgres_pool::{Db, DbError, Executor};
use pplx_space_rollout_queue::{Drain, FailedItems, Leasing, PhaseStore};
use pplx_space_sql_locator::sql;
use sqlx::{Acquire, Postgres};

use crate::{CampaignId, CampaignPhase, ItemPhase, end_db_transaction, end_transaction};

/// Pause `campaign_id` if it's leasing (canary or rolling) and its failed
/// items have reached its failure budget. `true` when this call paused it.
///
/// Locks the campaign row before counting the failed items, in a statement of
/// its own. A single `UPDATE` would count them from a snapshot taken before it
/// waited for the lock: Postgres rechecks the campaign row it waited on, but
/// not the items, so a resume that returned the failures to pending while it
/// waited would be paused again on the failures it just cleared. Opens a
/// transaction on `db`.
pub async fn pause_if_over_budget(db: &Db, campaign_id: CampaignId) -> Result<bool, DbError> {
    let mut tx = db.begin().await?;
    let mut reconciling = Reconciling {
        tx: &mut tx,
        campaign_id,
    };
    let paused = pplx_space_rollout_queue::pause_if_over_budget(&mut reconciling).await;
    end_db_transaction(tx, paused).await
}

/// Complete `campaign_id` if it's rolling, no item is pending or in flight,
/// and its failed items haven't reached its failure budget: a campaign over
/// its budget is the operator's to look at, so it pauses rather than
/// completes. A canary completes the same way while none of its items is
/// done: there's no successful upgrade for an operator to judge, and it
/// would otherwise wait for a resume that its own rule refuses. Like a
/// rolling campaign, it can complete with failed items under its budget,
/// which its item counts show. A canary with a done item waits for the
/// operator.
///
/// Skipped items don't hold a campaign open. A subject skipped for a passing
/// reason is pending again once its driver finds it on the source
/// ([`crate::insert_pending`]), which the caller must make sure happens
/// first: the reconciler completes only after a pass during which nothing
/// was pending, in flight or leased.
///
/// The caller decides when no more subjects will arrive, which for the
/// reconciler is the end of a whole pass of its driver's subjects. `true`
/// when this call completed the campaign.
///
/// Locks the campaign row before reading its items, in a statement of its
/// own: a single `UPDATE` would read the items from a snapshot taken before
/// it waited for the lock, and could complete a campaign under items that
/// [`crate::insert_pending`] committed meanwhile. That holds at `READ
/// COMMITTED`, where each statement takes a new snapshot; a caller's
/// `REPEATABLE READ` transaction would read the items as they were before
/// the lock. Opens a transaction, or a savepoint inside the caller's.
pub async fn complete_if_drained(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
) -> Result<bool, DbError> {
    let mut tx = conn.begin().await?;
    let mut reconciling = Reconciling {
        tx: &mut *tx,
        campaign_id,
    };
    let completed = pplx_space_rollout_queue::complete_if_drained(&mut reconciling).await;
    end_transaction(tx, completed).await
}

/// How many of `campaign_id`'s items have failed.
pub(crate) async fn count_failed<C>(tx: &mut C, campaign_id: CampaignId) -> Result<u64, DbError>
where
    for<'a> &'a mut C: Executor<'a>,
{
    let failed = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*)
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $2"
    ))
    .bind(campaign_id)
    .bind(ItemPhase::Failed.as_str())
    .fetch_one(tx.into_sqlx())
    .await?;
    // count(*) is never negative.
    Ok(u64::try_from(failed).unwrap_or(0))
}

/// [`pause_if_over_budget`]'s and [`complete_if_drained`]'s steps.
///
/// `C` is the verb's own transaction: a [`pplx_space_postgres_pool::Tx`],
/// or the connection of the driver's transaction or savepoint.
struct Reconciling<'c, C> {
    tx: &'c mut C,
    campaign_id: CampaignId,
}

impl<C> FailedItems for Reconciling<'_, C>
where
    for<'a> &'a mut C: Executor<'a>,
{
    type Error = DbError;

    async fn failed(&mut self) -> Result<u64, DbError> {
        count_failed(&mut *self.tx, self.campaign_id).await
    }
}

impl<C> PhaseStore for Reconciling<'_, C>
where
    for<'a> &'a mut C: Executor<'a>,
{
    async fn lock_leasing(&mut self) -> Result<Option<Leasing>, DbError> {
        let leasing: Vec<&str> = CampaignPhase::ALL
            .iter()
            .filter(|phase| phase.leases())
            .map(|phase| phase.as_str())
            .collect();
        let locked = sqlx::query_as::<_, (i64, bool)>(sql!(
            "SELECT failure_budget, phase = $3
             FROM runtime_upgrade_campaigns
             WHERE id = $1
               AND phase = ANY($2)
             FOR UPDATE"
        ))
        .bind(self.campaign_id)
        .bind(&leasing)
        .bind(CampaignPhase::Canary.as_str())
        .fetch_optional((&mut *self.tx).into_sqlx())
        .await?;
        Ok(locked.map(|(failure_budget, canary)| Leasing {
            canary,
            // The schema keeps it at least 1.
            failure_budget: u64::try_from(failure_budget).unwrap_or(0),
        }))
    }

    async fn drain(&mut self) -> Result<Drain, DbError> {
        let open: Vec<&str> = [ItemPhase::Pending, ItemPhase::InFlight]
            .iter()
            .map(|phase| phase.as_str())
            .collect();
        // Each reads only its phases' range of the (campaign_id, phase)
        // index.
        let (open, failed, any_done) = sqlx::query_as::<_, (bool, i64, bool)>(sql!(
            "SELECT EXISTS (
                        SELECT 1
                        FROM runtime_upgrade_items
                        WHERE campaign_id = $1
                          AND phase = ANY($2)),
                    (SELECT count(*)
                     FROM runtime_upgrade_items
                     WHERE campaign_id = $1
                       AND phase = $3),
                    EXISTS (
                        SELECT 1
                        FROM runtime_upgrade_items
                        WHERE campaign_id = $1
                          AND phase = $4)"
        ))
        .bind(self.campaign_id)
        .bind(&open)
        .bind(ItemPhase::Failed.as_str())
        .bind(ItemPhase::Done.as_str())
        .fetch_one((&mut *self.tx).into_sqlx())
        .await?;
        Ok(Drain {
            open,
            // count(*) is never negative.
            failed: u64::try_from(failed).unwrap_or(0),
            any_done,
        })
    }

    async fn write_paused(&mut self) -> Result<(), DbError> {
        sqlx::query(sql!(
            "UPDATE runtime_upgrade_campaigns
             SET phase = $2,
                 updated_by_user_id = NULL
             WHERE id = $1"
        ))
        .bind(self.campaign_id)
        .bind(CampaignPhase::Paused.as_str())
        .execute((&mut *self.tx).into_sqlx())
        .await?;
        Ok(())
    }

    /// A drained campaign needs no further evidence to complete.
    async fn write_completed(&mut self) -> Result<bool, DbError> {
        sqlx::query(sql!(
            "UPDATE runtime_upgrade_campaigns
             SET phase = $2,
                 finished_at = clock_timestamp(),
                 updated_by_user_id = NULL
             WHERE id = $1"
        ))
        .bind(self.campaign_id)
        .bind(CampaignPhase::Completed.as_str())
        .execute((&mut *self.tx).into_sqlx())
        .await?;
        Ok(true)
    }
}

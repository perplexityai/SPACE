//! Campaign phase changes the reconciler makes on its own: pausing at the
//! failure budget and completing a drained campaign. Operator changes are in
//! [`crate::admin`].
//!
//! Each verb locks the campaign row before it writes, in the down
//! migration's order, only moves a campaign from the phase it checked, so a
//! phase an operator wrote first wins. The schema's trigger raises the
//! generation in the same statement.

use pplx_space_sql_locator::sql;
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{CampaignId, CampaignPhase, ItemPhase, end_transaction};

/// Pause `campaign_id` if it's leasing (canary or rolling) and its failed
/// items have reached its failure budget. `true` when this call paused it.
///
/// Locks the campaign row before counting the failed items, in a statement of
/// its own. A single `UPDATE` would count them from a snapshot taken before it
/// waited for the lock: Postgres rechecks the campaign row it waited on, but
/// not the items, so a resume that returned the failures to pending while it
/// waited would be paused again on the failures it just cleared. Opens a
/// transaction, or a savepoint inside the caller's.
pub async fn pause_if_over_budget(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
) -> Result<bool, sqlx::Error> {
    let mut tx = conn.begin().await?;
    let paused = pause_if_over_budget_in(&mut tx, campaign_id).await;
    end_transaction(tx, paused).await
}

async fn pause_if_over_budget_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
) -> Result<bool, sqlx::Error> {
    let leasing: Vec<&str> = CampaignPhase::ALL
        .iter()
        .filter(|phase| phase.leases())
        .map(|phase| phase.as_str())
        .collect();
    let Some(failure_budget) = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT failure_budget
         FROM runtime_upgrade_campaigns
         WHERE id = $1
           AND phase = ANY($2)
         FOR UPDATE"
    ))
    .bind(campaign_id)
    .bind(&leasing)
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(false);
    };
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
    if failed < failure_budget {
        return Ok(false);
    }
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns
         SET phase = $2,
             updated_by_user_id = NULL
         WHERE id = $1"
    ))
    .bind(campaign_id)
    .bind(CampaignPhase::Paused.as_str())
    .execute(&mut *tx)
    .await?;
    Ok(true)
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
) -> Result<bool, sqlx::Error> {
    let mut tx = conn.begin().await?;
    let completed = complete_in(&mut tx, campaign_id).await;
    end_transaction(tx, completed).await
}

async fn complete_in(tx: &mut PgConnection, campaign_id: CampaignId) -> Result<bool, sqlx::Error> {
    let Some((failure_budget, canary)) = sqlx::query_as::<_, (i64, bool)>(sql!(
        "SELECT failure_budget, phase = $3
         FROM runtime_upgrade_campaigns
         WHERE id = $1
           AND phase = ANY(ARRAY[$2, $3])
         FOR UPDATE"
    ))
    .bind(campaign_id)
    .bind(CampaignPhase::Rolling.as_str())
    .bind(CampaignPhase::Canary.as_str())
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(false);
    };
    let open: Vec<&str> = [ItemPhase::Pending, ItemPhase::InFlight]
        .iter()
        .map(|phase| phase.as_str())
        .collect();
    // One statement, so both come from one snapshot taken after the lock.
    // Each reads only its phases' range of the (campaign_id, phase) index.
    let (open_items, failed, any_done) = sqlx::query_as::<_, (bool, i64, bool)>(sql!(
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
    .bind(campaign_id)
    .bind(&open)
    .bind(ItemPhase::Failed.as_str())
    .bind(ItemPhase::Done.as_str())
    .fetch_one(&mut *tx)
    .await?;
    if open_items || failed >= failure_budget || (canary && any_done) {
        return Ok(false);
    }
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns
         SET phase = $2,
             finished_at = clock_timestamp(),
             updated_by_user_id = NULL
         WHERE id = $1"
    ))
    .bind(campaign_id)
    .bind(CampaignPhase::Completed.as_str())
    .execute(&mut *tx)
    .await?;
    Ok(true)
}

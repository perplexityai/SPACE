//! Campaign phase changes the reconciler makes on its own: pausing at the
//! failure budget and completing a drained campaign. Operator changes
//! (resume, pause, abort) belong to the admin route that adds them.
//!
//! Each verb locks the campaign row before it writes, in the down
//! migration's order, and only moves a campaign from the phase it checked,
//! so a phase an operator wrote first wins.

use pplx_space_common_core::sql;
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{CampaignId, CampaignPhase, ItemPhase, end_transaction};

/// Pause `campaign_id` if it's leasing (canary or rolling) and its failed
/// items have reached its failure budget. `true` when this call paused it.
///
/// One `UPDATE`: the count comes from the statement's snapshot, so a failure
/// committed while it waited for the campaign lock is counted on the next
/// call, which every reconciler tick makes.
pub async fn pause_if_over_budget(
    executor: impl sqlx::PgExecutor<'_>,
    campaign_id: CampaignId,
) -> Result<bool, sqlx::Error> {
    let leasing: Vec<&str> = [CampaignPhase::Canary, CampaignPhase::Rolling]
        .iter()
        .map(|phase| phase.as_str())
        .collect();
    let paused = sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns c
         SET phase = $2,
             updated_by_user_id = NULL
         WHERE c.id = $1
           AND c.phase = ANY($3)
           AND c.failure_budget <= (
               SELECT count(*)
               FROM runtime_upgrade_items i
               WHERE i.campaign_id = c.id
                 AND i.phase = $4)"
    ))
    .bind(campaign_id)
    .bind(CampaignPhase::Paused.as_str())
    .bind(&leasing)
    .bind(ItemPhase::Failed.as_str())
    .execute(executor)
    .await?
    .rows_affected();
    Ok(paused == 1)
}

/// Complete `campaign_id` if it's rolling and has no pending or in-flight
/// items. The caller decides when no more subjects will arrive; this only
/// checks the items recorded so far. `true` when this call completed it.
///
/// Locks the campaign row before reading its items, in a statement of its
/// own: a single `UPDATE` would read the items from a snapshot taken before
/// it waited for the lock, and could complete a campaign under items that
/// [`crate::insert_pending_sandboxes`] committed meanwhile. Opens a
/// transaction, or a savepoint inside the caller's.
pub async fn complete_if_drained(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
) -> Result<bool, sqlx::Error> {
    let mut tx = conn.begin().await?;
    let completed = complete_in(&mut tx, campaign_id).await;
    end_transaction(tx, completed).await
}

async fn complete_in(tx: &mut PgConnection, campaign_id: CampaignId) -> Result<bool, sqlx::Error> {
    let rolling = sqlx::query_scalar::<_, i32>(sql!(
        "SELECT 1
         FROM runtime_upgrade_campaigns
         WHERE id = $1
           AND phase = $2
         FOR UPDATE"
    ))
    .bind(campaign_id)
    .bind(CampaignPhase::Rolling.as_str())
    .fetch_optional(&mut *tx)
    .await?
    .is_some();
    if !rolling {
        return Ok(false);
    }
    let open: Vec<&str> = [ItemPhase::Pending, ItemPhase::InFlight]
        .iter()
        .map(|phase| phase.as_str())
        .collect();
    let has_open = sqlx::query_scalar::<_, i32>(sql!(
        "SELECT 1
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = ANY($2)
         LIMIT 1"
    ))
    .bind(campaign_id)
    .bind(&open)
    .fetch_optional(&mut *tx)
    .await?
    .is_some();
    if has_open {
        return Ok(false);
    }
    sqlx::query(sql!(
        "UPDATE runtime_upgrade_campaigns
         SET phase = $2,
             finished_at = now(),
             updated_by_user_id = NULL
         WHERE id = $1"
    ))
    .bind(campaign_id)
    .bind(CampaignPhase::Completed.as_str())
    .execute(&mut *tx)
    .await?;
    Ok(true)
}

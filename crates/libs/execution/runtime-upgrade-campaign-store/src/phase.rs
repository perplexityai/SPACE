//! Campaign phase changes the reconciler makes on its own: pausing at the
//! failure budget and completing a drained campaign. Operator changes
//! (resume, pause, abort) belong to the admin route that adds them.
//!
//! Each verb locks the campaign row before it writes, in the down
//! migration's order, and only moves a campaign from the phase it checked,
//! so a phase an operator wrote first wins.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{SandboxLifecycle, SubstrateKind};
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{CampaignId, CampaignPhase, ItemPhase, SubjectKind, end_transaction};

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

/// Complete `campaign_id` if it's rolling and finished with its subjects:
///
/// * no item is pending or in flight;
/// * no item with leases left of `max_attempts` was skipped for a sandbox
///   that's still a candidate: of `managed_by`'s substrate, in one of
///   `lifecycles` (those the caller's passes scan), published, and not bound
///   to another environment, as [`crate::sandbox_candidates`] reads it. The
///   next pass finds such a sandbox again and gives it another lease. A
///   skipped item whose sandbox was deleted, has started under a campaign
///   that moves only stopped sandboxes, or was bound elsewhere doesn't hold
///   the campaign open. Neither does one the caller retired
///   ([`crate::retire_skipped_sandboxes`]) after attributing its unbound
///   execution away from the source;
/// * its failed items haven't reached its failure budget: a campaign over its
///   budget is the operator's to look at, so it pauses rather than completes.
///
/// The caller decides when no more subjects will arrive. `true` when this
/// call completed the campaign.
///
/// Locks the campaign row before reading its items, in a statement of its
/// own: a single `UPDATE` would read the items from a snapshot taken before
/// it waited for the lock, and could complete a campaign under items that
/// [`crate::insert_pending_sandboxes`] committed meanwhile. The item and
/// sandbox counts come from one snapshot taken after the lock, so an outcome
/// is either committed and judged by its sandbox's state, or still in flight.
/// Opens a transaction, or a savepoint inside the caller's.
pub async fn complete_if_drained(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    managed_by: SubstrateKind,
    lifecycles: &[SandboxLifecycle],
    max_attempts: u64,
) -> Result<bool, sqlx::Error> {
    let mut tx = conn.begin().await?;
    let completed = complete_in(&mut tx, campaign_id, managed_by, lifecycles, max_attempts).await;
    end_transaction(tx, completed).await
}

async fn complete_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    managed_by: SubstrateKind,
    lifecycles: &[SandboxLifecycle],
    max_attempts: u64,
) -> Result<bool, sqlx::Error> {
    let Some(failure_budget) = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT failure_budget
         FROM runtime_upgrade_campaigns
         WHERE id = $1
           AND phase = $2
         FOR UPDATE"
    ))
    .bind(campaign_id)
    .bind(CampaignPhase::Rolling.as_str())
    .fetch_optional(&mut *tx)
    .await?
    else {
        return Ok(false);
    };
    let open: Vec<&str> = [ItemPhase::Pending, ItemPhase::InFlight]
        .iter()
        .map(|phase| phase.as_str())
        .collect();
    let lifecycles: Vec<&str> = lifecycles
        .iter()
        .map(|lifecycle| lifecycle.as_str())
        .collect();
    // One statement, so the three counts come from one snapshot taken after
    // the lock. The candidate test is `sandbox_candidates`' own.
    let (open_items, retryable_skips, failed) = sqlx::query_as::<_, (i64, i64, i64)>(sql!(
        "SELECT count(*) FILTER (WHERE i.phase = ANY($2)),
                count(*) FILTER (
                    WHERE i.phase = $3
                      AND i.attempts < $4
                      AND i.subject_kind = $5
                      AND EXISTS (
                          SELECT 1
                          FROM sandboxes s
                          JOIN sandbox_observed_states o ON o.sandbox_id = s.id
                          WHERE s.id = i.subject_id
                            AND s.managed_by = $6
                            AND s.lifecycle = ANY($7)
                            AND (jsonb_typeof(o.execution_metadata -> 'environment')
                                     IS DISTINCT FROM 'string'
                                 OR o.execution_metadata ->> 'environment'
                                     = c.from_environment_ref))),
                count(*) FILTER (WHERE i.phase = $8)
         FROM runtime_upgrade_items i
         JOIN runtime_upgrade_campaigns c ON c.id = i.campaign_id
         WHERE i.campaign_id = $1"
    ))
    .bind(campaign_id)
    .bind(&open)
    .bind(ItemPhase::Skipped.as_str())
    .bind(i64::try_from(max_attempts).unwrap_or(i64::MAX))
    .bind(SubjectKind::Sandbox.as_str())
    .bind(managed_by)
    .bind(&lifecycles)
    .bind(ItemPhase::Failed.as_str())
    .fetch_one(&mut *tx)
    .await?;
    if open_items > 0 || retryable_skips > 0 || failed >= failure_budget {
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

//! Operator phase changes: pause, resume and abort.
//!
//! Each locks the campaign row, answering [`ChangePhaseError::NotFound`] for
//! a campaign of a kind outside [`crate::CampaignKind::RUNTIME`] as for an
//! absent one, refuses a transition its phase doesn't allow, then compares
//! the generation the caller expects, and writes the phase. The schema's
//! trigger raises the generation in the same statement, and the verb returns
//! the raised value. A refused transition, and a resume refused at the
//! failure budget, are reported before a stale generation, so a caller learns
//! what the campaign is now.
//!
//! # Resume and the failure budget
//!
//! The failure budget counts every failed item of the campaign. A campaign
//! that paused itself at its budget would pause again on the next tick if it
//! were resumed as it is, so [`resume_campaign`] refuses while the failed
//! items are at or over the budget ([`ChangePhaseError::AtFailureBudget`]),
//! unless the caller asks to retry them. Retrying returns every failed item
//! to pending with its attempts reset, keeping its `last_error` until a new
//! outcome replaces it, so each is leased afresh and the budget counts only
//! failures from then on.
//!
//! # Where a resume goes
//!
//! A resume goes to `rolling` once any item is done. A campaign with no done
//! item goes back to `canary`, whichever phase it started in: nothing has
//! shown the upgrade works yet, so it upgrades one subject and waits again.
//!
//! # Abort
//!
//! [`abort_campaign`] ends any unfinished campaign: `aborted` is final, with
//! `finished_at` set from `clock_timestamp()`, as completion sets it.
//! Nothing already upgraded is undone, and nothing leases the campaign's
//! items again. A new campaign for the same source can start at once.
//!
//! An item in flight keeps its lease, so its subject has one worker at a
//! time. Its worker can't renew a finished campaign's lease
//! ([`crate::renew_lease`]), and it renews at a third of the lease, so it
//! drops the work at its next renewal attempt, or at its own deadline if the
//! database is out of reach, before the lease lapses. No
//! other campaign leases the subject while the lease is live
//! ([`crate::lease_items`]). Once it lapses, the loop returns the item to
//! pending ([`crate::release_lapsed_in_finished`]). A worker that finishes
//! before its next renewal still records its outcome, which is true of the
//! subject.

use pplx_space_accounts::UserId;
use pplx_space_common_core::sql;
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{CampaignId, CampaignPhase, ItemPhase, RUNTIME_KINDS, UnknownValue, end_transaction};

/// Who asks for a phase change, and the generation they read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PhaseChange {
    pub actor: Option<UserId>,
    /// Apply only at this generation (the admin API's `If-Match`); `None`
    /// applies unconditionally.
    pub expected_generation: Option<u64>,
}

/// What a phase change left.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseChanged {
    pub phase: CampaignPhase,
    pub generation: u64,
    /// Failed items a resume returned to pending.
    pub retried: u64,
    /// In-flight items an abort left to drain: their workers stop at their
    /// next renewal, or have stopped already if the lease lapsed, and the
    /// items return to pending once their leases lapse.
    pub draining: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ChangePhaseError {
    #[error("no such campaign")]
    NotFound,
    #[error("the campaign is {0}, which is final")]
    Finished(CampaignPhase),
    #[error("the campaign is already paused")]
    AlreadyPaused,
    #[error("the campaign is {0}; only a paused campaign, or a canary whose item is done, resumes")]
    NotResumable(CampaignPhase),
    #[error(
        "the campaign has {failed} failed items, at or over its failure budget of \
         {failure_budget}; resume would pause it again. Retry the failed items to resume"
    )]
    AtFailureBudget { failed: u64, failure_budget: u64 },
    #[error("the campaign is at generation {generation}, not {expected}")]
    Stale { generation: u64, expected: u64 },
    #[error(transparent)]
    UnknownValue(#[from] UnknownValue),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

struct Locked {
    phase: CampaignPhase,
    generation: u64,
    failure_budget: u64,
}

async fn lock(tx: &mut PgConnection, campaign_id: CampaignId) -> Result<Locked, ChangePhaseError> {
    let (phase, generation, failure_budget) = sqlx::query_as::<_, (String, i64, i64)>(sql!(
        "SELECT phase, generation, failure_budget
         FROM runtime_upgrade_campaigns
         WHERE id = $1
           AND kind = ANY($2)
         FOR UPDATE"
    ))
    .bind(campaign_id)
    .bind(RUNTIME_KINDS)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(ChangePhaseError::NotFound)?;
    Ok(Locked {
        phase: phase.parse()?,
        // The schema keeps both non-negative.
        generation: u64::try_from(generation).unwrap_or(0),
        failure_budget: u64::try_from(failure_budget).unwrap_or(0),
    })
}

fn check_generation(change: &PhaseChange, generation: u64) -> Result<(), ChangePhaseError> {
    match change.expected_generation {
        Some(expected) if expected != generation => Err(ChangePhaseError::Stale {
            generation,
            expected,
        }),
        _ => Ok(()),
    }
}

async fn write_phase(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    phase: CampaignPhase,
    actor: Option<UserId>,
) -> Result<u64, sqlx::Error> {
    let generation = sqlx::query_scalar::<_, i64>(sql!(
        "UPDATE runtime_upgrade_campaigns
         SET phase = $2,
             finished_at = CASE WHEN $4 THEN clock_timestamp() END,
             updated_by_user_id = $3
         WHERE id = $1
         RETURNING generation"
    ))
    .bind(campaign_id)
    .bind(phase.as_str())
    .bind(actor)
    .bind(phase.finished())
    .fetch_one(&mut *tx)
    .await?;
    Ok(u64::try_from(generation).unwrap_or(0))
}

/// Pause a canary or rolling campaign: nothing new is leased, and items in
/// flight finish under their leases. Opens a transaction, or a savepoint
/// inside the caller's.
pub async fn pause_campaign(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    change: &PhaseChange,
) -> Result<PhaseChanged, ChangePhaseError> {
    let mut tx = conn.begin().await?;
    let paused = pause_in(&mut tx, campaign_id, change).await;
    end_transaction(tx, paused).await
}

async fn pause_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    change: &PhaseChange,
) -> Result<PhaseChanged, ChangePhaseError> {
    let locked = lock(tx, campaign_id).await?;
    match locked.phase {
        CampaignPhase::Completed | CampaignPhase::Aborted => {
            return Err(ChangePhaseError::Finished(locked.phase));
        }
        CampaignPhase::Paused => return Err(ChangePhaseError::AlreadyPaused),
        CampaignPhase::Canary | CampaignPhase::Rolling => {}
    }
    check_generation(change, locked.generation)?;
    let generation = write_phase(tx, campaign_id, CampaignPhase::Paused, change.actor).await?;
    Ok(PhaseChanged {
        phase: CampaignPhase::Paused,
        generation,
        retried: 0,
        draining: 0,
    })
}

/// Resume a paused campaign, or move a canary whose item is done on to
/// rolling. With `retry_failed`, failed items go back to pending first; see
/// the module docs for the budget and the phase a resume goes to. Opens a
/// transaction, or a savepoint inside the caller's.
pub async fn resume_campaign(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    change: &PhaseChange,
    retry_failed: bool,
) -> Result<PhaseChanged, ChangePhaseError> {
    let mut tx = conn.begin().await?;
    let resumed = resume_in(&mut tx, campaign_id, change, retry_failed).await;
    end_transaction(tx, resumed).await
}

async fn resume_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    change: &PhaseChange,
    retry_failed: bool,
) -> Result<PhaseChanged, ChangePhaseError> {
    let locked = lock(tx, campaign_id).await?;
    if locked.phase.finished() {
        return Err(ChangePhaseError::Finished(locked.phase));
    }
    // Outcomes take no campaign lock, so a failure recorded after this read
    // can still reach the budget once the campaign resumes; the reconciler's
    // budget pause then pauses it again. Each reads only its phase's range of
    // the (campaign_id, phase) index.
    let (any_done, failed) = sqlx::query_as::<_, (bool, i64)>(sql!(
        "SELECT EXISTS (
                    SELECT 1
                    FROM runtime_upgrade_items
                    WHERE campaign_id = $1
                      AND phase = $2),
                (SELECT count(*)
                 FROM runtime_upgrade_items
                 WHERE campaign_id = $1
                   AND phase = $3)"
    ))
    .bind(campaign_id)
    .bind(ItemPhase::Done.as_str())
    .bind(ItemPhase::Failed.as_str())
    .fetch_one(&mut *tx)
    .await?;
    // The count is never negative.
    let failed = u64::try_from(failed).unwrap_or(0);
    match locked.phase {
        CampaignPhase::Paused => {}
        CampaignPhase::Canary if any_done => {}
        phase => return Err(ChangePhaseError::NotResumable(phase)),
    }
    if failed >= locked.failure_budget && !retry_failed {
        return Err(ChangePhaseError::AtFailureBudget {
            failed,
            failure_budget: locked.failure_budget,
        });
    }
    check_generation(change, locked.generation)?;
    let retried = if retry_failed {
        sqlx::query(sql!(
            "UPDATE runtime_upgrade_items
             SET phase = $3,
                 attempts = 0
             WHERE campaign_id = $1
               AND phase = $2"
        ))
        .bind(campaign_id)
        .bind(ItemPhase::Failed.as_str())
        .bind(ItemPhase::Pending.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected()
    } else {
        0
    };
    let phase = if any_done {
        CampaignPhase::Rolling
    } else {
        CampaignPhase::Canary
    };
    let generation = write_phase(tx, campaign_id, phase, change.actor).await?;
    Ok(PhaseChanged {
        phase,
        generation,
        retried,
        draining: 0,
    })
}

/// Abort an unfinished campaign (canary, rolling or paused); see the module
/// docs. Opens a transaction, or a savepoint inside the caller's.
pub async fn abort_campaign(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    change: &PhaseChange,
) -> Result<PhaseChanged, ChangePhaseError> {
    let mut tx = conn.begin().await?;
    let aborted = abort_in(&mut tx, campaign_id, change).await;
    end_transaction(tx, aborted).await
}

async fn abort_in(
    tx: &mut PgConnection,
    campaign_id: CampaignId,
    change: &PhaseChange,
) -> Result<PhaseChanged, ChangePhaseError> {
    let locked = lock(tx, campaign_id).await?;
    if locked.phase.finished() {
        return Err(ChangePhaseError::Finished(locked.phase));
    }
    check_generation(change, locked.generation)?;
    // Under the campaign lock, which leasers take first, so no lease is
    // granted between this count and the phase write.
    let draining = sqlx::query_scalar::<_, i64>(sql!(
        "SELECT count(*)
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND phase = $2"
    ))
    .bind(campaign_id)
    .bind(ItemPhase::InFlight.as_str())
    .fetch_one(&mut *tx)
    .await?;
    let generation = write_phase(tx, campaign_id, CampaignPhase::Aborted, change.actor).await?;
    Ok(PhaseChanged {
        phase: CampaignPhase::Aborted,
        generation,
        retried: 0,
        draining: u64::try_from(draining).unwrap_or(0),
    })
}

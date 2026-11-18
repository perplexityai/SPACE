//! Operator phase changes: pause, resume and abort.
//!
//! Each locks the campaign row, refuses a transition its phase doesn't allow,
//! then compares the generation the caller expects, and writes the phase. The
//! schema's trigger raises the generation in the same statement, and the
//! verb returns the raised value. A refused transition, and a resume refused
//! at the failure budget, are reported before a stale generation, so a caller
//! learns what the campaign is now.
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
//!
//! Each runs the steps of [`pplx_space_rollout_queue::pause`],
//! [`pplx_space_rollout_queue::resume`] or [`pplx_space_rollout_queue::abort`]
//! on these tables.

use pplx_space_accounts::UserId;
use pplx_space_rollout_queue::{HeaderPhase, Locked, OperatorStore, Refusal};
use pplx_space_sql_locator::sql;
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{CampaignId, CampaignPhase, ItemPhase, UnknownValue, end_transaction};

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
    /// In-flight items an abort left to drain, counted under the campaign
    /// lock just before the abort is written: their workers stop at their
    /// next renewal, or have stopped already if the lease lapsed, and the
    /// items return to pending once their leases lapse. Outcomes take no
    /// campaign lock, so a worker can record an outcome between the count
    /// and the write; the count is of items in flight when it was read.
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

/// [`Operating`]'s error: a [`ChangePhaseError`] that the shared verbs'
/// refusals convert into. Private, so [`ChangePhaseError`] itself gains no
/// conversion from the shared crate's [`Refusal`].
struct OperatorError(ChangePhaseError);

impl From<Refusal> for OperatorError {
    fn from(refusal: Refusal) -> Self {
        Self(match refusal {
            Refusal::NotFound => ChangePhaseError::NotFound,
            Refusal::Finished(phase) => {
                ChangePhaseError::Finished(CampaignPhase::from_queue(phase))
            }
            Refusal::AlreadyPaused => ChangePhaseError::AlreadyPaused,
            Refusal::NotResumable(phase) => {
                ChangePhaseError::NotResumable(CampaignPhase::from_queue(phase))
            }
            Refusal::AtFailureBudget {
                failed,
                failure_budget,
            } => ChangePhaseError::AtFailureBudget {
                failed,
                failure_budget,
            },
            Refusal::Stale {
                generation,
                expected,
            } => ChangePhaseError::Stale {
                generation,
                expected,
            },
        })
    }
}

impl From<sqlx::Error> for OperatorError {
    fn from(error: sqlx::Error) -> Self {
        Self(error.into())
    }
}

impl From<UnknownValue> for OperatorError {
    fn from(error: UnknownValue) -> Self {
        Self(error.into())
    }
}

/// An operator change's steps.
struct Operating<'c> {
    tx: &'c mut PgConnection,
    campaign_id: CampaignId,
    actor: Option<UserId>,
}

impl OperatorStore for Operating<'_> {
    type Error = OperatorError;

    async fn lock(&mut self) -> Result<Option<Locked>, OperatorError> {
        let Some((phase, generation, failure_budget)) =
            sqlx::query_as::<_, (String, i64, i64)>(sql!(
                "SELECT phase, generation, failure_budget
                 FROM runtime_upgrade_campaigns
                 WHERE id = $1
                 FOR UPDATE"
            ))
            .bind(self.campaign_id)
            .fetch_optional(&mut *self.tx)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(Locked {
            phase: phase.parse::<CampaignPhase>()?.queue_phase(),
            // The schema keeps both non-negative.
            generation: u64::try_from(generation).unwrap_or(0),
            failure_budget: u64::try_from(failure_budget).unwrap_or(0),
        }))
    }

    async fn progress(&mut self) -> Result<(bool, u64), OperatorError> {
        // Each reads only its phase's range of the (campaign_id, phase)
        // index.
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
        .bind(self.campaign_id)
        .bind(ItemPhase::Done.as_str())
        .bind(ItemPhase::Failed.as_str())
        .fetch_one(&mut *self.tx)
        .await?;
        // The count is never negative.
        Ok((any_done, u64::try_from(failed).unwrap_or(0)))
    }

    async fn retry_failed(&mut self) -> Result<u64, OperatorError> {
        Ok(sqlx::query(sql!(
            "UPDATE runtime_upgrade_items
             SET phase = $3,
                 attempts = 0
             WHERE campaign_id = $1
               AND phase = $2"
        ))
        .bind(self.campaign_id)
        .bind(ItemPhase::Failed.as_str())
        .bind(ItemPhase::Pending.as_str())
        .execute(&mut *self.tx)
        .await?
        .rows_affected())
    }

    async fn in_flight(&mut self) -> Result<u64, OperatorError> {
        let in_flight = sqlx::query_scalar::<_, i64>(sql!(
            "SELECT count(*)
             FROM runtime_upgrade_items
             WHERE campaign_id = $1
               AND phase = $2"
        ))
        .bind(self.campaign_id)
        .bind(ItemPhase::InFlight.as_str())
        .fetch_one(&mut *self.tx)
        .await?;
        Ok(u64::try_from(in_flight).unwrap_or(0))
    }

    async fn write_phase(&mut self, phase: HeaderPhase) -> Result<u64, OperatorError> {
        let phase = CampaignPhase::from_queue(phase);
        let generation = sqlx::query_scalar::<_, i64>(sql!(
            "UPDATE runtime_upgrade_campaigns
             SET phase = $2,
                 finished_at = CASE WHEN $4 THEN clock_timestamp() END,
                 updated_by_user_id = $3
             WHERE id = $1
             RETURNING generation"
        ))
        .bind(self.campaign_id)
        .bind(phase.as_str())
        .bind(self.actor)
        .bind(phase.finished())
        .fetch_one(&mut *self.tx)
        .await?;
        Ok(u64::try_from(generation).unwrap_or(0))
    }
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
    let mut operating = Operating {
        tx: &mut tx,
        campaign_id,
        actor: change.actor,
    };
    let paused = pplx_space_rollout_queue::pause(&mut operating, change.expected_generation)
        .await
        .map(|generation| PhaseChanged {
            phase: CampaignPhase::Paused,
            generation,
            retried: 0,
            draining: 0,
        })
        .map_err(|OperatorError(error)| error);
    end_transaction(tx, paused).await
}

/// Resume a paused campaign, or move a canary whose item is done on to
/// rolling. With `retry_failed`, failed items go back to pending first; see
/// the module docs for the budget and the phase a resume goes to. Opens a
/// transaction, or a savepoint inside the caller's.
///
/// The caller's transaction must run at `READ COMMITTED`, Postgres's
/// default. Under `REPEATABLE READ` or `SERIALIZABLE` its snapshot can
/// predate the campaign lock, so the budget check could count failures as
/// they were before an outcome committed, and a retry would miss items that
/// failed since.
pub async fn resume_campaign(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    change: &PhaseChange,
    retry_failed: bool,
) -> Result<PhaseChanged, ChangePhaseError> {
    let mut tx = conn.begin().await?;
    let mut operating = Operating {
        tx: &mut tx,
        campaign_id,
        actor: change.actor,
    };
    let resumed =
        pplx_space_rollout_queue::resume(&mut operating, change.expected_generation, retry_failed)
            .await
            .map(|resumed| PhaseChanged {
                phase: CampaignPhase::from_queue(resumed.phase),
                generation: resumed.generation,
                retried: resumed.retried,
                draining: 0,
            })
            .map_err(|OperatorError(error)| error);
    end_transaction(tx, resumed).await
}

/// Abort an unfinished campaign (canary, rolling or paused); see the module
/// docs. Opens a transaction, or a savepoint inside the caller's.
///
/// The caller's transaction must run at `READ COMMITTED`, Postgres's
/// default. Under `REPEATABLE READ` or `SERIALIZABLE` its snapshot can
/// predate the campaign lock, so `PhaseChanged::draining` could count an item
/// as in flight after its outcome committed.
pub async fn abort_campaign(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign_id: CampaignId,
    change: &PhaseChange,
) -> Result<PhaseChanged, ChangePhaseError> {
    let mut tx = conn.begin().await?;
    let mut operating = Operating {
        tx: &mut tx,
        campaign_id,
        actor: change.actor,
    };
    let aborted = pplx_space_rollout_queue::abort(&mut operating, change.expected_generation)
        .await
        .map(|aborted| PhaseChanged {
            phase: CampaignPhase::Aborted,
            generation: aborted.generation,
            retried: 0,
            draining: aborted.draining,
        })
        .map_err(|OperatorError(error)| error);
    end_transaction(tx, aborted).await
}

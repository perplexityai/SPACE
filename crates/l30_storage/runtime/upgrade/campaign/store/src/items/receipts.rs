//! Durable `last_error` markers tracking `sandbox_force_restarts` item progress
//! before dedicated receipt columns land in a standalone schema migration.

use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;

use super::{ItemOutcome, LeasedItem};
use crate::{CampaignId, CampaignKind, ItemError, ItemPhase, SubjectKind};

pub(crate) const UNSTARTED_PREFIX: &str = "before fresh run started: ";
pub(crate) const STARTED_HOLD_PREFIX: &str = "after partial move: ";
pub(crate) const TARGET_STARTING: &str = "while starting on target";
pub(crate) const TARGET_STARTING_PREFIX: &str = "while starting on target: ";
pub(crate) const TARGET_START_SETTLED: &str = "after target start settled";
pub(crate) const TARGET_START_SETTLED_PREFIX: &str = "after target start settled: ";
pub(crate) const RUN_STARTED: &str = "run started";

pub(super) struct ForceRestartOutcomeErrors {
    pub(super) started: Option<ItemError>,
    pub(super) unstarted: Option<ItemError>,
    pub(super) target_starting: Option<ItemError>,
    pub(super) target_started: Option<ItemError>,
}

fn strip_internal_prefixes<'a>(raw: &'a str, fallback: &'a str) -> &'a str {
    let mut clean = raw;
    while let Some(rest) = clean
        .strip_prefix(UNSTARTED_PREFIX)
        .or_else(|| clean.strip_prefix(STARTED_HOLD_PREFIX))
        .or_else(|| clean.strip_prefix(TARGET_STARTING_PREFIX))
        .or_else(|| clean.strip_prefix(TARGET_START_SETTLED_PREFIX))
    {
        clean = rest;
    }
    if clean.is_empty() || clean == TARGET_START_SETTLED || clean == TARGET_STARTING {
        fallback
    } else {
        clean
    }
}

pub(super) fn force_restart_errors(outcome: &ItemOutcome) -> ForceRestartOutcomeErrors {
    match outcome {
        ItemOutcome::Done => ForceRestartOutcomeErrors {
            started: None,
            unstarted: None,
            target_starting: None,
            target_started: None,
        },
        ItemOutcome::Skipped(error) => ForceRestartOutcomeErrors {
            started: Some(error.clone()),
            unstarted: Some(error.clone()),
            target_starting: None,
            target_started: None,
        },
        ItemOutcome::Failed(error) => non_terminal_force_restart_errors(error, "failed"),
        ItemOutcome::Retry(error) => non_terminal_force_restart_errors(error, "retry"),
        ItemOutcome::Release(error) => non_terminal_force_restart_errors(error, "released"),
    }
}

fn non_terminal_force_restart_errors(
    error: &ItemError,
    fallback: &str,
) -> ForceRestartOutcomeErrors {
    let clean = strip_internal_prefixes(error.as_deref().unwrap_or(fallback), fallback);
    ForceRestartOutcomeErrors {
        started: Some(ItemError::new(clean)),
        unstarted: Some(ItemError::new(format!("{UNSTARTED_PREFIX}{clean}"))),
        target_starting: Some(ItemError::new(format!("{TARGET_STARTING_PREFIX}{clean}"))),
        target_started: Some(ItemError::new(format!(
            "{TARGET_START_SETTLED_PREFIX}{clean}"
        ))),
    }
}

pub(super) fn partial_move_error(outcome: &ItemOutcome) -> Option<ItemError> {
    let (error, fallback) = match outcome {
        ItemOutcome::Done | ItemOutcome::Skipped(_) => return None,
        ItemOutcome::Failed(error) => (error, "failed"),
        ItemOutcome::Retry(error) => (error, "retry"),
        ItemOutcome::Release(error) => (error, "released"),
    };
    let clean = strip_internal_prefixes(error.as_deref().unwrap_or(fallback), fallback);
    Some(ItemError::new(format!("{STARTED_HOLD_PREFIX}{clean}")))
}

/// Mark a started `sandbox_force_restarts` item as having claimed `'stopped' ->
/// 'starting'` toward the target environment while its row is in an active run
/// (`pending`, `in_flight`, or `failed` after [`mark_run_started`] committed).
/// Preserved across [`super::finish_item`] (`Retry`/`Release`/`Failed`),
/// [`super::redrive_expired`], [`super::release_lapsed_in_finished`], and
/// [`crate::resume_campaign`] (`retry_failed`) until cleared by
/// [`clear_leased_starting_on_target`] / [`clear_starting_on_target`] or
/// promoted to [`mark_started_on_target`], so a stranded-mode pass still
/// selects an item stuck in `'starting'` on the source environment to roll back
/// an undispatched claim or observe node settlement.
///
/// Prefer [`mark_leased_starting_on_target`] when the caller holds an active
/// [`LeasedItem`]; callers using this unfenced form must hold `FOR UPDATE` on
/// the `runtime_upgrade_items` row in an open transaction after verifying run
/// ownership.
pub async fn mark_starting_on_target(
    executor: impl Executor<'_>,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    subject_id: uuid::Uuid,
) -> Result<bool, DbError> {
    let active_phases = [
        ItemPhase::Pending.as_str(),
        ItemPhase::InFlight.as_str(),
        ItemPhase::Failed.as_str(),
    ];
    let marked = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET last_error = CASE
             WHEN last_error = $6
               OR starts_with(last_error, $7)
               OR last_error = $8
               OR starts_with(last_error, $9)
             THEN last_error
             WHEN phase = $10 THEN $8
             ELSE left($9 || last_error, $11)
         END
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = ANY($4)
           AND last_error IS NOT NULL
           AND NOT starts_with(last_error, $5)
           AND EXISTS (
               SELECT 1
               FROM runtime_upgrade_campaigns
               WHERE id = $1
                 AND kind = $12
           )"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(subject_id)
    .bind(active_phases)
    .bind(UNSTARTED_PREFIX)
    .bind(TARGET_START_SETTLED)
    .bind(TARGET_START_SETTLED_PREFIX)
    .bind(TARGET_STARTING)
    .bind(TARGET_STARTING_PREFIX)
    .bind(ItemPhase::InFlight.as_str())
    .bind(ItemError::MAX_CHARS as i32)
    .bind(CampaignKind::SandboxForceRestarts.as_str())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(marked == 1)
}

/// Like [`mark_starting_on_target`], fenced by `item`'s active `in_flight`
/// `lease_token`.
pub async fn mark_leased_starting_on_target(
    executor: impl Executor<'_>,
    item: &LeasedItem,
) -> Result<bool, DbError> {
    let marked = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET last_error = CASE
             WHEN last_error = $7 OR starts_with(last_error, $8) THEN last_error
             ELSE $9
         END
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $4
           AND lease_token = $5
           AND last_error IS NOT NULL
           AND NOT starts_with(last_error, $6)
           AND EXISTS (
               SELECT 1
               FROM runtime_upgrade_campaigns
               WHERE id = $1
                 AND kind = $10
           )"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .bind(UNSTARTED_PREFIX)
    .bind(TARGET_START_SETTLED)
    .bind(TARGET_START_SETTLED_PREFIX)
    .bind(TARGET_STARTING)
    .bind(CampaignKind::SandboxForceRestarts.as_str())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(marked == 1)
}

/// Clear [`mark_starting_on_target`]'s marker when an undispatched `'starting'`
/// claim rolls back to `'stopped'` or its stop ownership is cleared, keeping
/// the run marked started (`continuing_run == true`) without downgrading an
/// already-settled [`mark_started_on_target`] receipt.
///
/// Prefer [`clear_leased_starting_on_target`] when the caller holds an active
/// [`LeasedItem`]; callers using this unfenced form must hold `FOR UPDATE` on
/// the `runtime_upgrade_items` row in an open transaction after verifying run
/// ownership.
pub async fn clear_starting_on_target(
    executor: impl Executor<'_>,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    subject_id: uuid::Uuid,
) -> Result<bool, DbError> {
    let active_phases = [
        ItemPhase::Pending.as_str(),
        ItemPhase::InFlight.as_str(),
        ItemPhase::Failed.as_str(),
    ];
    let cleared = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET last_error = CASE
             WHEN last_error = $5 THEN $7
             ELSE COALESCE(NULLIF(substr(last_error, char_length($6) + 1), ''), $7)
         END
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = ANY($4)
           AND (last_error = $5 OR starts_with(last_error, $6))
           AND EXISTS (
               SELECT 1
               FROM runtime_upgrade_campaigns
               WHERE id = $1
                 AND kind = $8
           )"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(subject_id)
    .bind(active_phases)
    .bind(TARGET_STARTING)
    .bind(TARGET_STARTING_PREFIX)
    .bind(RUN_STARTED)
    .bind(CampaignKind::SandboxForceRestarts.as_str())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(cleared == 1)
}

/// Like [`clear_starting_on_target`], fenced by `item`'s active `in_flight`
/// `lease_token` so a stale worker cannot clear a newer lease's starting
/// marker.
pub async fn clear_leased_starting_on_target(
    executor: impl Executor<'_>,
    item: &LeasedItem,
) -> Result<bool, DbError> {
    let cleared = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET last_error = CASE
             WHEN last_error = $6 THEN $8
             ELSE COALESCE(NULLIF(substr(last_error, char_length($7) + 1), ''), $8)
         END
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $4
           AND lease_token = $5
           AND (last_error = $6 OR starts_with(last_error, $7))
           AND EXISTS (
               SELECT 1
               FROM runtime_upgrade_campaigns
               WHERE id = $1
                 AND kind = $9
           )"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .bind(TARGET_STARTING)
    .bind(TARGET_STARTING_PREFIX)
    .bind(RUN_STARTED)
    .bind(CampaignKind::SandboxForceRestarts.as_str())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(cleared == 1)
}

/// Whether `item`'s active `in_flight` lease currently carries the
/// [`mark_starting_on_target`] marker, or `None` when `item` is no longer
/// `in_flight` under `item.lease_token`.
pub async fn starting_on_target(
    executor: impl Executor<'_>,
    item: &LeasedItem,
) -> Result<Option<bool>, DbError> {
    let row = sqlx::query_as::<_, (Option<String>,)>(sql!(
        "SELECT last_error
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $4
           AND lease_token = $5"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(|(last_error,)| {
        last_error.as_deref().is_some_and(|error| {
            error == TARGET_STARTING || error.starts_with(TARGET_STARTING_PREFIX)
        })
    }))
}

/// Mark a started `sandbox_force_restarts` item as having completed its
/// campaign-owned start onto the target environment, while its row is still in
/// an active run (`pending`, `in_flight`, or `failed` after [`mark_run_started`]
/// committed). Preserved across [`super::finish_item`]
/// (`Retry`/`Release`/`Failed`), [`super::redrive_expired`],
/// [`super::release_lapsed_in_finished`], and [`crate::resume_campaign`]
/// (`retry_failed`) so a continuing retry certifies [`ItemOutcome::Done`] even
/// if the sandbox was deleted or `moves == false` before [`super::finish_item`]
/// committed.
pub async fn mark_started_on_target(
    executor: impl Executor<'_>,
    campaign_id: CampaignId,
    subject_kind: SubjectKind,
    subject_id: uuid::Uuid,
) -> Result<bool, DbError> {
    let active_phases = [
        ItemPhase::Pending.as_str(),
        ItemPhase::InFlight.as_str(),
        ItemPhase::Failed.as_str(),
    ];
    let marked = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET last_error = CASE
             WHEN last_error = $6 OR starts_with(last_error, $7) THEN last_error
             WHEN phase = $8 OR last_error = $10 THEN $6
             WHEN starts_with(last_error, $11)
             THEN left($7 || COALESCE(NULLIF(substr(last_error, char_length($11) + 1), ''), $12), $9)
             ELSE left($7 || last_error, $9)
         END
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = ANY($4)
           AND last_error IS NOT NULL
           AND NOT starts_with(last_error, $5)
           AND EXISTS (
               SELECT 1
               FROM runtime_upgrade_campaigns
               WHERE id = $1
                 AND kind = $13
           )"
    ))
    .bind(campaign_id)
    .bind(subject_kind.as_str())
    .bind(subject_id)
    .bind(active_phases)
    .bind(UNSTARTED_PREFIX)
    .bind(TARGET_START_SETTLED)
    .bind(TARGET_START_SETTLED_PREFIX)
    .bind(ItemPhase::InFlight.as_str())
    .bind(ItemError::MAX_CHARS as i32)
    .bind(TARGET_STARTING)
    .bind(TARGET_STARTING_PREFIX)
    .bind(RUN_STARTED)
    .bind(CampaignKind::SandboxForceRestarts.as_str())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(marked == 1)
}

/// Like [`mark_started_on_target`], fenced by `item`'s active `in_flight`
/// `lease_token` so a driver lease whose token was superseded or whose run has
/// not called [`mark_run_started`] cannot stamp target-start settlement onto a
/// later or reopened run.
pub async fn mark_leased_started_on_target(
    executor: impl Executor<'_>,
    item: &LeasedItem,
) -> Result<bool, DbError> {
    let marked = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET last_error = CASE
             WHEN last_error = $7 OR starts_with(last_error, $8) THEN last_error
             ELSE $7
         END
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $4
           AND lease_token = $5
           AND last_error IS NOT NULL
           AND NOT starts_with(last_error, $6)
           AND EXISTS (
               SELECT 1
               FROM runtime_upgrade_campaigns
               WHERE id = $1
                 AND kind = $9
           )"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .bind(UNSTARTED_PREFIX)
    .bind(TARGET_START_SETTLED)
    .bind(TARGET_START_SETTLED_PREFIX)
    .bind(CampaignKind::SandboxForceRestarts.as_str())
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(marked == 1)
}

/// Whether `item`'s active `in_flight` lease already settled a campaign-owned
/// start onto the target environment via [`mark_started_on_target`], or `None`
/// when `item` is no longer `in_flight` under `item.lease_token`.
pub async fn started_on_target(
    executor: impl Executor<'_>,
    item: &LeasedItem,
) -> Result<Option<bool>, DbError> {
    let row = sqlx::query_as::<_, (Option<String>,)>(sql!(
        "SELECT last_error
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $4
           AND lease_token = $5"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(|(last_error,)| {
        last_error.as_deref().is_some_and(|error| {
            error == TARGET_START_SETTLED || error.starts_with(TARGET_START_SETTLED_PREFIX)
        })
    }))
}

/// Mark a fresh or reopened-from-`skipped` run of `item` as started once its
/// pre-run cleanup has finished, so subsequent [`continuing_run`] checks,
/// retries, deadlines, and lease redrives treat the run as in progress.
/// Returns `false` when `item` is no longer `in_flight` under
/// `item.lease_token`.
pub async fn mark_run_started(
    executor: impl Executor<'_>,
    item: &LeasedItem,
) -> Result<bool, DbError> {
    let marked = sqlx::query(sql!(
        "UPDATE runtime_upgrade_items
         SET last_error = CASE
             WHEN last_error IS NULL OR starts_with(last_error, $6) THEN $7
             ELSE last_error
         END
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $4
           AND lease_token = $5"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .bind(UNSTARTED_PREFIX)
    .bind(RUN_STARTED)
    .execute(executor.into_sqlx())
    .await?
    .rows_affected();
    Ok(marked == 1)
}

/// Whether `item`'s current `in_flight` lease is continuing an interrupted run
/// (`Ok(Some(true))`) rather than starting a newly inserted or
/// reopened-from-`skipped` item (`Ok(Some(false))`), or `Ok(None)` when `item`
/// is no longer `in_flight` under `item.lease_token`.
///
/// [`super::insert_pending`] sets `last_error = NULL` both when inserting a new
/// item and when reopening a `skipped` item to `pending`. Until
/// [`mark_run_started`] records that a `sandbox_force_restarts` item finished
/// its pre-run cleanup, non-terminal outcomes, item-deadline failures, and
/// lease redrives keep the unstarted marker on `last_error` so the next lease
/// still sees `Some(false)`. Once [`mark_run_started`] commits, every
/// non-terminal re-queue ([`ItemOutcome::Retry`], [`ItemOutcome::Release`],
/// [`super::redrive_expired`], [`super::release_lapsed_in_finished`], and
/// [`crate::resume_campaign`] with `retry_failed` after
/// [`ItemOutcome::Failed`]) stores a started `last_error` even when given an
/// empty [`ItemError`].
pub async fn continuing_run(
    executor: impl Executor<'_>,
    item: &LeasedItem,
) -> Result<Option<bool>, DbError> {
    let row = sqlx::query_as::<_, (Option<String>,)>(sql!(
        "SELECT last_error
         FROM runtime_upgrade_items
         WHERE campaign_id = $1
           AND subject_kind = $2
           AND subject_id = $3
           AND phase = $4
           AND lease_token = $5"
    ))
    .bind(item.campaign_id)
    .bind(item.subject_kind.as_str())
    .bind(item.subject_id)
    .bind(ItemPhase::InFlight.as_str())
    .bind(item.lease_token)
    .fetch_optional(executor.into_sqlx())
    .await?;
    Ok(row.map(|(last_error,)| {
        last_error
            .as_deref()
            .is_some_and(|error| !error.starts_with(UNSTARTED_PREFIX))
    }))
}

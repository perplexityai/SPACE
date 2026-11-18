//! Starting a campaign. A new campaign is at generation 0.
//!
//! At most one unfinished campaign of a kind moves any one source
//! environment, so two campaigns don't both move a source's sandboxes or
//! releases.
//! [`create_campaign`] enforces it without a constraint: creators for one kind
//! and source serialize on a transaction-scoped advisory lock, and each looks
//! for an unfinished campaign after taking it, so the second sees the first's
//! committed row. A finished campaign doesn't hold its source: one aborted
//! with an upgrade still in flight keeps that item's lease until it lapses,
//! and [`crate::lease_items`] gives the subject to no other campaign while it
//! lives. A campaign created by a direct `INSERT` bypasses the check.

use pplx_space_accounts::UserId;
use pplx_space_execution_spec::EnvironmentReference;
use pplx_space_sql_locator::sql;
use sqlx::{Acquire, PgConnection, Postgres};

use crate::{CampaignId, CampaignKind, CampaignPhase, end_transaction};

/// A campaign to start.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewCampaign {
    pub from: EnvironmentReference,
    pub to: EnvironmentReference,
    pub kind: CampaignKind,
    /// Start as a canary, upgrading one subject and then waiting for an
    /// operator's resume; otherwise start rolling.
    pub canary: bool,
    pub max_in_flight_per_node: u64,
    pub max_in_flight_per_cluster: u64,
    pub failure_budget: u64,
    /// For a sandbox campaign, also take running and paused sandboxes. The
    /// sandbox driver gives it its meaning; a template campaign refuses it.
    pub include_running: bool,
    /// Why the campaign runs: 1 to 1024 characters, no NUL.
    pub reason: String,
    pub created_by: Option<UserId>,
}

impl NewCampaign {
    /// The phase the campaign starts in: `canary` or `rolling`.
    pub const fn initial_phase(&self) -> CampaignPhase {
        if self.canary {
            CampaignPhase::Canary
        } else {
            CampaignPhase::Rolling
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateCampaignError {
    #[error("campaign {0} already moves this environment; finish or abort it first")]
    AlreadyUnfinished(CampaignId),
    #[error("invalid campaign: {0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// The most characters a reason may hold, matching the schema's `CHECK`.
pub const REASON_MAX_CHARS: usize = 1024;

/// Why `reason` can't be recorded: it must be 1 to [`REASON_MAX_CHARS`]
/// characters with no NUL. The admin API checks pause and resume reasons,
/// which only its audit records, the same way.
pub fn check_reason(reason: &str) -> Result<(), &'static str> {
    let chars = reason.chars().count();
    if chars == 0 || chars > REASON_MAX_CHARS || reason.contains('\0') {
        Err("the reason must be 1 to 1024 characters with no NUL")
    } else {
        Ok(())
    }
}

pub(crate) fn validate(campaign: &NewCampaign) -> Result<(), CreateCampaignError> {
    if campaign.from == campaign.to {
        Err(CreateCampaignError::Invalid(
            "the source and target must differ",
        ))
    } else if campaign.max_in_flight_per_node == 0
        || campaign.max_in_flight_per_node > campaign.max_in_flight_per_cluster
    {
        Err(CreateCampaignError::Invalid(
            "max_in_flight_per_node must be at least 1 and at most max_in_flight_per_cluster",
        ))
    } else if campaign.failure_budget == 0 {
        Err(CreateCampaignError::Invalid(
            "failure_budget must be at least 1",
        ))
    } else if campaign.include_running && campaign.kind != CampaignKind::Sandboxes {
        Err(CreateCampaignError::Invalid(
            "include_running applies only to sandbox campaigns",
        ))
    } else {
        check_reason(&campaign.reason).map_err(CreateCampaignError::Invalid)
    }
}

/// Start `campaign`, refusing while an unfinished campaign of the same kind
/// moves the same source. Opens a transaction, or a savepoint inside the
/// caller's; run inside a caller's transaction, the advisory lock lasts until
/// that transaction ends. The checks read after the lock, so they're race
/// free only at `READ COMMITTED`: a caller's `REPEATABLE READ` snapshot from
/// before the lock would miss a campaign another creator committed.
pub async fn create_campaign(
    conn: impl Acquire<'_, Database = Postgres>,
    campaign: &NewCampaign,
) -> Result<CampaignId, CreateCampaignError> {
    validate(campaign)?;
    let mut tx = conn.begin().await?;
    let created = create_in(&mut tx, campaign).await;
    end_transaction(tx, created).await
}

async fn create_in(
    tx: &mut PgConnection,
    campaign: &NewCampaign,
) -> Result<CampaignId, CreateCampaignError> {
    let count = |value: u64| i64::try_from(value).unwrap_or(i64::MAX);
    if let Some(unfinished) =
        crate::unfinished_from_locked(&mut *tx, campaign.kind, campaign.from).await?
    {
        return Err(CreateCampaignError::AlreadyUnfinished(unfinished));
    }
    let phase = campaign.initial_phase();
    Ok(sqlx::query_scalar::<_, CampaignId>(sql!(
        "INSERT INTO runtime_upgrade_campaigns (
             from_environment_ref, to_environment_ref, kind, phase,
             max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
             include_running, reason, created_by_user_id, updated_by_user_id
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
         RETURNING id"
    ))
    .bind(campaign.from.to_string())
    .bind(campaign.to.to_string())
    .bind(campaign.kind.as_str())
    .bind(phase.as_str())
    .bind(count(campaign.max_in_flight_per_node))
    .bind(count(campaign.max_in_flight_per_cluster))
    .bind(count(campaign.failure_budget))
    .bind(campaign.include_running)
    .bind(&campaign.reason)
    .bind(campaign.created_by)
    .fetch_one(&mut *tx)
    .await?)
}

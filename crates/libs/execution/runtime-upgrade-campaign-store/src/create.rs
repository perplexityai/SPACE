//! Starting a campaign.
//!
//! At most one unfinished campaign of a kind moves any one source
//! environment, so two campaigns never act on the same sandbox or release.
//! [`create_campaign`] enforces it without a constraint: creators for one kind
//! and source serialize on a transaction-scoped advisory lock, and each looks
//! for an unfinished campaign after taking it, so the second sees the first's
//! committed row. A campaign created by a direct `INSERT` bypasses the check.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{EnvironmentReference, UserId};
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
    /// For a sandbox campaign, also take running and paused sandboxes when
    /// the source is blocked. Each is stopped, upgraded, and left stopped: a
    /// running sandbox loses its processes and memory and stays down until
    /// someone starts it.
    pub include_running: bool,
    /// Why the campaign runs: 1 to 1024 characters, no NUL.
    pub reason: String,
    pub created_by: Option<UserId>,
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

const REASON_MAX_CHARS: usize = 1024;

pub(crate) fn validate(campaign: &NewCampaign) -> Result<(), CreateCampaignError> {
    let reason_chars = campaign.reason.chars().count();
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
    } else if reason_chars == 0 || reason_chars > REASON_MAX_CHARS || campaign.reason.contains('\0')
    {
        Err(CreateCampaignError::Invalid(
            "the reason must be 1 to 1024 characters with no NUL",
        ))
    } else {
        Ok(())
    }
}

/// Start `campaign`, refusing while an unfinished campaign of the same kind
/// moves the same source. Opens a transaction, or a savepoint inside the
/// caller's; run inside a caller's transaction, the advisory lock lasts until
/// that transaction ends.
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
    sqlx::query(sql!(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
    ))
    .bind(format!(
        "runtime_upgrade_campaigns/{}/{}",
        campaign.kind, campaign.from
    ))
    .execute(&mut *tx)
    .await?;
    if let Some(unfinished) = sqlx::query_scalar::<_, CampaignId>(sql!(
        "SELECT id
         FROM runtime_upgrade_campaigns
         WHERE from_environment_ref = $1
           AND kind = $2
           AND finished_at IS NULL
         LIMIT 1"
    ))
    .bind(campaign.from.to_string())
    .bind(campaign.kind.as_str())
    .fetch_optional(&mut *tx)
    .await?
    {
        return Err(CreateCampaignError::AlreadyUnfinished(unfinished));
    }
    let phase = if campaign.canary {
        CampaignPhase::Canary
    } else {
        CampaignPhase::Rolling
    };
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

//! Seeding campaigns for tests, here and in the crates that drive them.
//! Compiled for this crate's tests and behind the `test-support` feature;
//! enable it only from `[dev-dependencies]`.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{Digest, EnvironmentReference};

use crate::{CampaignId, CampaignKind, CampaignPhase};

/// A campaign row as an operator would create it. Counts are signed so a
/// test can seed values the schema refuses.
#[derive(Clone, Debug)]
pub struct CampaignSeed {
    pub from: EnvironmentReference,
    pub to: EnvironmentReference,
    pub kind: CampaignKind,
    pub phase: CampaignPhase,
    pub per_node: i64,
    pub per_cluster: i64,
    pub failure_budget: i64,
    pub include_running: bool,
}

impl Default for CampaignSeed {
    /// A rolling sandbox campaign from `sha256:00…` to `sha256:01…`.
    fn default() -> Self {
        Self {
            from: EnvironmentReference::new(Digest::sha256([0; 32])),
            to: EnvironmentReference::new(Digest::sha256([1; 32])),
            kind: CampaignKind::Sandboxes,
            phase: CampaignPhase::Rolling,
            per_node: 1,
            per_cluster: 10,
            failure_budget: 3,
            include_running: false,
        }
    }
}

/// Insert `seed`, with `finished_at` set exactly when its phase is terminal,
/// as the schema requires.
pub async fn insert_campaign(
    executor: impl sqlx::PgExecutor<'_>,
    seed: CampaignSeed,
) -> Result<CampaignId, sqlx::Error> {
    let id = sqlx::query_scalar::<_, uuid::Uuid>(sql!(
        "INSERT INTO runtime_upgrade_campaigns (
             from_environment_ref, to_environment_ref, kind, phase,
             max_in_flight_per_node, max_in_flight_per_cluster, failure_budget,
             include_running, reason, finished_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'test campaign',
                 CASE WHEN $4 IN ($9, $10) THEN now() END)
         RETURNING id"
    ))
    .bind(seed.from.to_string())
    .bind(seed.to.to_string())
    .bind(seed.kind.as_str())
    .bind(seed.phase.as_str())
    .bind(seed.per_node)
    .bind(seed.per_cluster)
    .bind(seed.failure_budget)
    .bind(seed.include_running)
    .bind(CampaignPhase::Completed.as_str())
    .bind(CampaignPhase::Aborted.as_str())
    .fetch_one(executor)
    .await?;
    Ok(CampaignId(id))
}

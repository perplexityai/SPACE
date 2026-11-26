//! The production occupancy statements, exposed for other crates' tests that inspect their plans,
//! and the batched oracle those tests compare them against.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_residency::ResidencyEvidence;

use crate::admin_occupancy::NodeOccupancyCounts;

/// The statement `admin_occupancy::node_occupancy` runs for `ranked` and
/// `residency`.
pub fn node_occupancy_statement(ranked: bool, residency: ResidencyEvidence) -> &'static str {
    crate::admin_occupancy::node_occupancy_statement(ranked, residency)
}

/// The statement `admin_occupancy::fleet_occupancy` runs for `ranked` and
/// `residency`.
pub fn fleet_occupancy_statement(ranked: bool, residency: ResidencyEvidence) -> &'static str {
    crate::admin_occupancy::fleet_occupancy_statement(ranked, residency)
}

/// `admin_occupancy::batched_occupancy`: every node in `node_ids`, read by the
/// pre-optimization batched statement the occupancy tests use as their oracle.
pub async fn batched_occupancy(
    executor: impl Executor<'_>,
    node_ids: &[NodeId],
    long_runner_age_secs: Option<i64>,
    residency: ResidencyEvidence,
) -> Result<Vec<NodeOccupancyCounts>, DbError> {
    crate::admin_occupancy::batched_occupancy(executor, node_ids, long_runner_age_secs, residency)
        .await
}

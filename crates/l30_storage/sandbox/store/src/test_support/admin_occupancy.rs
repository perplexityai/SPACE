//! The production occupancy statements, exposed for other crates' tests that inspect their plans.

use pplx_space_sandbox_residency::ResidencyEvidence;

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

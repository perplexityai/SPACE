use pplx_space_sandbox_residency::ResidencyEvidence;

/// The fleet statement `AdminNodeStore::occupancy_for_nodes` runs, for tests
/// that inspect its plan.
pub(super) fn query(ranked: bool, residency: ResidencyEvidence) -> String {
    pplx_space_sandbox_store::admin_occupancy::fleet_occupancy_statement(ranked, residency)
        .to_owned()
}

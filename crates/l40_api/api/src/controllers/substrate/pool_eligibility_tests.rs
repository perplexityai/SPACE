use super::pool_eligible_with_env;

#[test]
fn env_bearing_create_is_ineligible_only_when_substrate_cannot_apply_env() {
    // Substrate-ineligible stays ineligible regardless of env.
    assert!(!pool_eligible_with_env(false, true, false));
    assert!(!pool_eligible_with_env(false, false, true));
    // Eligible + no env → eligible on any substrate.
    assert!(pool_eligible_with_env(true, true, false));
    assert!(pool_eligible_with_env(true, false, false));
    // Eligible + env → only when the substrate can apply env at adoption.
    assert!(pool_eligible_with_env(true, true, true));
    assert!(!pool_eligible_with_env(true, false, true));
}

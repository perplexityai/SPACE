use super::*;

#[test]
fn network_policy_target_round_trips_template() {
    let target = NetworkPolicyTarget::Template(TemplateName::parse("seed").unwrap());
    assert_eq!(target.to_string(), "template:seed");
    assert_eq!(
        target.to_string().parse::<NetworkPolicyTarget>().unwrap(),
        target
    );
}

#[test]
fn network_policy_target_rejects_missing_prefix() {
    assert!(matches!(
        "seed".parse::<NetworkPolicyTarget>(),
        Err(NetworkPolicyTargetParseError::MissingPrefix)
    ));
}

/// Each reason's `state_reconciliation_reason` label, in the order of
/// [`ReconcileReason::ALL`]. Queued triggers keep these across releases, so
/// none may change.
const SPELLINGS: [&str; 18] = [
    "node_registered",
    "node_updated",
    "manual_node_status_refresh",
    "manual_node_reconcile",
    "manual_node_network_policy_reconcile",
    "node_template_pulled",
    "sandbox_registered",
    "sandbox_port_exposure_updated",
    "sandbox_network_policy_updated",
    "sandbox_desired_state_updated",
    "template_network_policy_updated",
    "template_desired_state_updated",
    "template_base_snapshot_policy_updated",
    "template_pool_policy_updated",
    "template_pool_policy_deleted",
    "node_sandbox_inventory_published",
    "node_template_inventory_published",
    "firewall_rules_changed",
];

#[test]
fn every_reason_round_trips_through_its_spelling() {
    assert_eq!(ReconcileReason::ALL.len(), SPELLINGS.len());
    for (&reason, spelling) in ReconcileReason::ALL.iter().zip(SPELLINGS) {
        assert_eq!(reason.as_str(), spelling);
        assert_eq!(reason.to_string(), spelling);
        assert_eq!(reason.normalized(), spelling);
        assert_eq!(ReconcileReason::parse(spelling), Some(reason));
    }
}

/// Parsing neither trims nor folds case, so a near miss of the firewall rules
/// reason is no reason at all.
#[test]
fn near_misses_of_the_firewall_rules_reason_parse_as_nothing() {
    for near_miss in [
        "firewall_rules_change",
        "firewall-rules-changed",
        "FIREWALL_RULES_CHANGED",
        " firewall_rules_changed",
    ] {
        assert_eq!(ReconcileReason::parse(near_miss), None, "{near_miss:?}");
    }
}

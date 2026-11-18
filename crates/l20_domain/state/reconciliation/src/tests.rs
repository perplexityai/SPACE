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

/// Every reason with its spelling: the `state_reconciliation_reason` label
/// Postgres stores, which a queued trigger keeps across releases, so no
/// spelling may change.
const REASON_SPELLINGS: [(ReconcileReason, &str); 18] = [
    (ReconcileReason::NodeRegistered, "node_registered"),
    (ReconcileReason::NodeUpdated, "node_updated"),
    (
        ReconcileReason::ManualNodeStatusRefresh,
        "manual_node_status_refresh",
    ),
    (
        ReconcileReason::ManualNodeReconcile,
        "manual_node_reconcile",
    ),
    (
        ReconcileReason::ManualNodeNetworkPolicyReconcile,
        "manual_node_network_policy_reconcile",
    ),
    (ReconcileReason::NodeTemplatePulled, "node_template_pulled"),
    (ReconcileReason::SandboxRegistered, "sandbox_registered"),
    (
        ReconcileReason::SandboxPortExposureUpdated,
        "sandbox_port_exposure_updated",
    ),
    (
        ReconcileReason::SandboxNetworkPolicyUpdated,
        "sandbox_network_policy_updated",
    ),
    (
        ReconcileReason::SandboxDesiredStateUpdated,
        "sandbox_desired_state_updated",
    ),
    (
        ReconcileReason::TemplateNetworkPolicyUpdated,
        "template_network_policy_updated",
    ),
    (
        ReconcileReason::TemplateDesiredStateUpdated,
        "template_desired_state_updated",
    ),
    (
        ReconcileReason::TemplateBaseSnapshotPolicyUpdated,
        "template_base_snapshot_policy_updated",
    ),
    (
        ReconcileReason::TemplatePoolPolicyUpdated,
        "template_pool_policy_updated",
    ),
    (
        ReconcileReason::TemplatePoolPolicyDeleted,
        "template_pool_policy_deleted",
    ),
    (
        ReconcileReason::NodeSandboxInventoryPublished,
        "node_sandbox_inventory_published",
    ),
    (
        ReconcileReason::NodeTemplateInventoryPublished,
        "node_template_inventory_published",
    ),
    (
        ReconcileReason::FirewallRulesChanged,
        "firewall_rules_changed",
    ),
];

// Fails to compile when a reason is added without a row above.
const _: fn(ReconcileReason) = |reason| match reason {
    ReconcileReason::NodeRegistered
    | ReconcileReason::NodeUpdated
    | ReconcileReason::ManualNodeStatusRefresh
    | ReconcileReason::ManualNodeReconcile
    | ReconcileReason::ManualNodeNetworkPolicyReconcile
    | ReconcileReason::NodeTemplatePulled
    | ReconcileReason::SandboxRegistered
    | ReconcileReason::SandboxPortExposureUpdated
    | ReconcileReason::SandboxNetworkPolicyUpdated
    | ReconcileReason::SandboxDesiredStateUpdated
    | ReconcileReason::TemplateNetworkPolicyUpdated
    | ReconcileReason::TemplateDesiredStateUpdated
    | ReconcileReason::TemplateBaseSnapshotPolicyUpdated
    | ReconcileReason::TemplatePoolPolicyUpdated
    | ReconcileReason::TemplatePoolPolicyDeleted
    | ReconcileReason::NodeSandboxInventoryPublished
    | ReconcileReason::NodeTemplateInventoryPublished
    | ReconcileReason::FirewallRulesChanged => {}
};

#[test]
fn reasons_round_trip_through_their_spelling() {
    let mut seen = std::collections::BTreeSet::new();
    for (reason, spelling) in REASON_SPELLINGS {
        assert!(seen.insert(reason), "{reason:?} is listed twice");
        assert_eq!(reason.as_str(), spelling);
        assert_eq!(reason.to_string(), spelling);
        assert_eq!(reason.normalized(), spelling);
        assert_eq!(ReconcileReason::parse(spelling), Some(reason));
    }
}

/// A near miss of the firewall rules reason is no reason at all, rather than
/// the firewall rules reason or some other one: parsing does not trim or fold
/// case.
#[test]
fn near_misses_of_the_firewall_rules_reason_parse_as_nothing() {
    for near_miss in [
        "firewall_rules_change",
        "firewall_rules_changed_",
        "firewall_rules_update",
        "firewall_rules_updated",
        "firewall_rule_changed",
        "firewall-rules-changed",
        "firewallruleschanged",
        "Firewall_Rules_Changed",
        "FIREWALL_RULES_CHANGED",
        " firewall_rules_changed",
        "firewall_rules_changed ",
        "firewall_rules",
        "network_policy_changed",
        "",
    ] {
        assert_eq!(ReconcileReason::parse(near_miss), None, "{near_miss:?}");
    }
    // Normalization trims the diagnostic text, which then parses.
    assert_eq!(
        ReconcileReason::parse(&normalize_reconcile_reason(" firewall_rules_changed\n")),
        Some(ReconcileReason::FirewallRulesChanged)
    );
}

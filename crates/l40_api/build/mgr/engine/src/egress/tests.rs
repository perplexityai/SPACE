use pplx_space_net_common::{DenyReason, EgressDecision, NetworkPolicyV1};

use super::{
    BuildVmEgressPolicyError, ENV_EGRESS_ALLOW_CIDRS, ENV_EGRESS_DENY_CIDRS,
    build_vm_egress_policy, parse_cidrs,
};

fn policy(deny: &str, allow: &str) -> NetworkPolicyV1 {
    build_vm_egress_policy(
        parse_cidrs(ENV_EGRESS_DENY_CIDRS, deny).unwrap(),
        &parse_cidrs(ENV_EGRESS_ALLOW_CIDRS, allow).unwrap(),
    )
}

#[test]
fn unconfigured_build_vm_policy_allows_all_egress() {
    assert_eq!(policy("", ""), NetworkPolicyV1::allow_all());
}

#[test]
fn build_vm_policy_denies_configured_ranges_except_allowed_ranges() {
    let policy = policy(
        " 100.64.0.0/10, 192.0.2.0/24 ,fd00::/8",
        "100.64.1.2/32,100.80.0.0/16,fd00::53/128",
    );
    for (ip, allowed) in [
        ("100.64.0.1", false),
        ("100.64.1.1", false),
        ("100.64.1.2", true),
        ("100.64.1.3", false),
        ("100.80.255.255", true),
        ("100.81.0.0", false),
        ("192.0.2.10", false),
        ("fd00::1", false),
        ("fd00::53", true),
        ("8.8.8.8", true),
        ("198.51.100.1", true),
        ("2001:4860:4860::8888", true),
    ] {
        assert_eq!(
            policy.egress_decision_for_ip(ip.parse().unwrap()),
            if allowed {
                EgressDecision::Allow
            } else {
                EgressDecision::Deny {
                    reason: DenyReason::Rule,
                }
            },
            "{ip}"
        );
    }
}

#[test]
fn build_vm_policy_rejects_an_entry_that_is_not_a_cidr() {
    assert!(matches!(
        parse_cidrs(ENV_EGRESS_DENY_CIDRS, "100.64.0.0/10,100.64.1.2"),
        Err(BuildVmEgressPolicyError::InvalidCidr { value, .. }) if value == "100.64.1.2"
    ));
}

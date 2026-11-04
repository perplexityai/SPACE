use super::*;

fn v1_allow_all() -> NetworkPolicyV1 {
    NetworkPolicyV1::allow_all()
}

fn v1_deny_all() -> NetworkPolicyV1 {
    NetworkPolicyV1::deny_all()
}

fn dst() -> SocketAddr {
    "203.0.113.10:443".parse().unwrap()
}

fn admit() -> EgressContext {
    EgressContext::Admission(EgressProtocol::Tcp)
}

fn evaluate_v1(
    destination: SocketAddr,
    dns_allowed: bool,
    policy: &NetworkPolicyV1,
) -> EgressDecision {
    evaluate_egress(admit(), destination, dns_allowed, policy)
}

// --- evaluate_egress: V1 dispatch -------------------------------------

#[test]
fn evaluate_allows_under_v1_allow_all() {
    let decision = evaluate_v1(dst(), false, &v1_allow_all());
    assert_eq!(decision, EgressDecision::Allow);
}

#[test]
fn evaluate_denies_under_v1_deny_all() {
    let decision = evaluate_v1(dst(), false, &v1_deny_all());
    assert_eq!(
        decision,
        EgressDecision::Deny {
            reason: pplx_space_net_common::DenyReason::Default
        }
    );
}

#[test]
fn dns_allowed_overrides_v1_default_deny() {
    // An IP learned from a policy-allowed domain is permitted even when the
    // default action is deny — the DNS-allow input makes domain-based allow
    // rules enforceable on raw IP egress.
    assert_eq!(
        evaluate_v1(dst(), false, &v1_deny_all()),
        EgressDecision::Deny {
            reason: pplx_space_net_common::DenyReason::Default
        }
    );
    assert_eq!(
        evaluate_v1(dst(), true, &v1_deny_all()),
        EgressDecision::Allow
    );
}

#[test]
fn platform_guardrail_denies_even_under_v1_allow_all() {
    // EC2 IMDS is a platform hard-deny guardrail: it must take precedence
    // over any tenant policy, including allow-all.
    let imds: SocketAddr = "169.254.169.254:80".parse().unwrap();
    assert_eq!(
        evaluate_v1(imds, false, &v1_allow_all()),
        EgressDecision::platform_guardrail("ec2-imds"),
    );
}

#[test]
fn v1_explicit_deny_precedes_an_overlapping_allow() {
    let destination = dst();
    let policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: pplx_space_net_common::EgressPolicyV1::new(
            pplx_space_net_common::EgressDefaultAction::Deny,
            vec![pplx_space_net_common::EgressDestinationV1::Ip(
                destination.ip(),
            )],
            vec![pplx_space_net_common::EgressDestinationV1::Cidr(
                "203.0.113.0/24".parse().unwrap(),
            )],
        )
        .unwrap(),
    };

    assert_eq!(
        evaluate_v1(destination, false, &policy),
        EgressDecision::Deny {
            reason: pplx_space_net_common::DenyReason::Rule,
        }
    );
}

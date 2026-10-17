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

// --- port-scoped TLS interception exemptions --------------------------

/// Interception on, every address allowed, one domain denied, and the address
/// exempt only on port 5228.
fn exempt_on_port_5228() -> NetworkPolicyV1 {
    use pplx_space_net_common::{
        EgressDefaultAction, EgressDestinationV1, EgressPolicyV1, TlsInterception,
        TlsInterceptionExemptionV1,
    };

    NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: TlsInterception::On,
        tls_interception_exemptions: vec![
            TlsInterceptionExemptionV1::new(EgressDestinationV1::Ip(dst().ip()), Some(5228))
                .unwrap(),
        ],
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Allow,
            Vec::new(),
            vec![EgressDestinationV1::Domain(
                "blocked.example.test".parse().unwrap(),
            )],
        )
        .unwrap(),
    }
}

#[test]
fn admission_applies_a_port_scoped_exemption_only_on_its_port() {
    // An exempt flow is never inspected, so admission refuses a denied SNI
    // itself. On another port, interception sees the SNI and refuses it later.
    let policy = exempt_on_port_5228();
    let cache = DnsAllowCache::default();
    let scan = crate::tls_sni::ClientHelloScan::ServerName("blocked.example.test".parse().unwrap());
    let exempt = SocketAddr::new(dst().ip(), 5228);

    assert!(matches!(
        admit_tcp(exempt, &scan, &policy, &cache),
        Err(TcpAdmissionDenial::DnsAttribution(DenyReason::Rule))
    ));
    assert!(matches!(
        admit_tcp(dst(), &scan, &policy, &cache),
        Ok(AdmittedTcp::Tunnel(_))
    ));
}

#[test]
fn a_missing_client_hello_is_relayed_only_on_the_exemption_port() {
    use crate::sniff::MissingClientHello;

    let policy = exempt_on_port_5228();
    let cache = DnsAllowCache::default();

    assert_eq!(
        missing_client_hello(SocketAddr::new(dst().ip(), 5228), &policy, &cache),
        MissingClientHello::Proceeds,
        "an exempt flow relays without SNI"
    );
    assert_ne!(
        missing_client_hello(dst(), &policy, &cache),
        MissingClientHello::Proceeds,
        "interception still applies on another port"
    );
}

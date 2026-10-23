use super::*;
use crate::dns::test_fixtures::a_response as dns_a_response;
use crate::egress::tcp::dns_attributed_host;

fn v1_allow_all() -> NetworkPolicyV1 {
    NetworkPolicyV1::allow_all()
}

fn v1_deny_all() -> NetworkPolicyV1 {
    NetworkPolicyV1::deny_all()
}

fn domain_policy(allowed_domains: &[&str], denied_domains: &[&str]) -> NetworkPolicyV1 {
    use pplx_space_net_common::{
        EgressDefaultAction, EgressDestinationV1, EgressPolicyV1, TlsInterception,
    };

    NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: TlsInterception::On,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            allowed_domains
                .iter()
                .map(|domain| EgressDestinationV1::Domain(domain.parse().unwrap()))
                .collect(),
            denied_domains
                .iter()
                .map(|domain| EgressDestinationV1::Domain(domain.parse().unwrap()))
                .collect(),
        )
        .unwrap(),
    }
}

#[test]
fn policy_resweep_closes_opaque_tls_when_interception_turns_on() {
    use pplx_space_net_common::TlsInterception::{Off, On};
    use pplx_space_net_common::{EgressDefaultAction, EgressDestinationV1, EgressPolicyV1};

    let host = "pypi.org";
    let destination: SocketAddr = "93.184.216.34:443".parse().unwrap();
    for (admitted, current, cancelled) in [(Off, On, 1), (On, On, 0), (On, Off, 0), (Off, Off, 0)] {
        // Independent IP permission makes an opaque connection admissible.
        // Tightening to domain-only permission keeps a flow bound to its SNI,
        // unless a relay admitted with interception off meets a policy that
        // turns it on.
        let old_policy = NetworkPolicyV1 {
            tls_interception: admitted,
            tls_interception_exemptions: Vec::new(),
            network_credential_rules: Vec::new(),
            egress: EgressPolicyV1::new(
                EgressDefaultAction::Deny,
                vec![
                    EgressDestinationV1::Ip(destination.ip()),
                    EgressDestinationV1::Domain(host.parse().unwrap()),
                ],
                Vec::new(),
            )
            .unwrap(),
            version: Default::default(),
        };
        let cache = DnsAllowCache::default();
        cache.observe_response(&dns_a_response(host, [93, 184, 216, 34], 60), &old_policy);
        let tracker = DnsAttributedTcpTracker::default();
        let register = |token: &CancellationToken| {
            tracker.register(
                destination,
                ClientHelloScan::ServerName(host.parse().unwrap()),
                &old_policy,
                &cache,
                token.clone(),
            )
        };

        // Changing only interception mode keeps independent IP access, except
        // that an opaque TLS relay admitted under Off cannot survive a switch
        // to On: it can never reach the per-request Host gate.
        let ip_token = CancellationToken::new();
        let ip_guard = register(&ip_token);
        let mut ip_policy = old_policy.clone();
        ip_policy.tls_interception = current;
        let closes_relay = usize::from(admitted == Off && current == On);
        assert_eq!(
            tracker.cancel_denied(&ip_policy, &cache),
            closes_relay,
            "{admitted:?} -> {current:?}"
        );
        assert_eq!(
            ip_token.is_cancelled(),
            closes_relay != 0,
            "{admitted:?} -> {current:?}"
        );
        drop(ip_guard);

        let token = CancellationToken::new();
        let _guard = register(&token);
        let mut new_policy = domain_policy(&[host], &[]);
        new_policy.tls_interception = current;
        cache.retain_relevant_to_policy(&new_policy);
        assert!(cache.contains_host_binding(destination.ip(), host));
        assert_eq!(
            tracker.cancel_denied(&new_policy, &cache),
            cancelled,
            "{admitted:?} -> {current:?}"
        );
        assert_eq!(
            token.is_cancelled(),
            cancelled != 0,
            "{admitted:?} -> {current:?}"
        );
    }
}

#[test]
fn policy_resweep_cancels_only_denied_connections() {
    let dns_tracker = DnsAttributedTcpTracker::default();
    let cache = DnsAllowCache::default();
    let dns_token = CancellationToken::new();
    let _dns_guard = dns_tracker.register(
        "8.8.8.8:443".parse().unwrap(),
        ClientHelloScan::NoServerName,
        &v1_allow_all(),
        &cache,
        dns_token.clone(),
    );

    assert_eq!(dns_tracker.cancel_denied(&v1_allow_all(), &cache), 0);
    assert!(!dns_token.is_cancelled());

    assert_eq!(dns_tracker.cancel_denied(&v1_deny_all(), &cache), 1);
    assert!(dns_token.is_cancelled());
    assert_eq!(dns_tracker.cancel_denied(&v1_deny_all(), &cache), 0);
}

#[test]
fn tracker_unregisters_finished_connections() {
    let dns_tracker = DnsAttributedTcpTracker::default();
    let cache = DnsAllowCache::default();
    let dns_token = CancellationToken::new();
    {
        let _dns_guard = dns_tracker.register(
            "8.8.8.8:443".parse().unwrap(),
            ClientHelloScan::NoServerName,
            &v1_allow_all(),
            &cache,
            dns_token.clone(),
        );
    }

    assert_eq!(dns_tracker.cancel_denied(&v1_deny_all(), &cache), 0);
    assert!(!dns_token.is_cancelled());
}

/// A TLS flow to `pypi.org` shares its IP with `files.pythonhosted.org`, which
/// stays allowed under `new_policy`. The resweep must still cancel the pypi flow.
fn assert_resweep_cancels_pypi_on_shared_ip(new_policy: &NetworkPolicyV1, prune_cache: bool) {
    let pypi = "pypi.org";
    let files = "files.pythonhosted.org";
    let destination = "93.184.216.34:443".parse().unwrap();
    let old_policy = domain_policy(&[pypi, files], &[]);
    let cache = DnsAllowCache::default();
    cache.observe_response(&dns_a_response(pypi, [93, 184, 216, 34], 60), &old_policy);
    cache.observe_response(&dns_a_response(files, [93, 184, 216, 34], 60), &old_policy);
    let tracker = DnsAttributedTcpTracker::default();
    let token = CancellationToken::new();
    let _guard = tracker.register(
        destination,
        ClientHelloScan::ServerName(pypi.parse().unwrap()),
        &old_policy,
        &cache,
        token.clone(),
    );

    assert_eq!(tracker.cancel_denied(&old_policy, &cache), 0);
    if prune_cache {
        cache.retain_relevant_to_policy(new_policy);
    }
    assert!(
        cache.allows_ip(destination.ip(), new_policy),
        "the shared IP remains allowed through the other CDN host"
    );
    assert_eq!(
        tracker.cancel_denied(new_policy, &cache),
        1,
        "the pypi SNI must be cancelled despite another allowed host on the same IP"
    );
    assert!(token.is_cancelled());
}

#[test]
fn policy_resweep_revalidates_a_domain_bound_sni_not_only_the_shared_ip() {
    // Mirrors the install hook: prune the cache, then resweep.
    assert_resweep_cancels_pypi_on_shared_ip(
        &domain_policy(&["files.pythonhosted.org"], &[]),
        true,
    );
}

#[test]
fn policy_resweep_domain_deny_overrides_cached_allow_on_shared_ip() {
    // No prune, so the pypi binding stays live and only the deny check can cancel.
    assert_resweep_cancels_pypi_on_shared_ip(
        &domain_policy(&["pypi.org", "files.pythonhosted.org"], &["pypi.org"]),
        false,
    );
}

#[test]
fn policy_resweep_closes_a_plaintext_domain_derived_flow() {
    let host = "pypi.org";
    let destination: SocketAddr = "93.184.216.34:80".parse().unwrap();
    let policy = domain_policy(&[host], &[]);
    let cache = DnsAllowCache::default();
    cache.observe_response(&dns_a_response(host, [93, 184, 216, 34], 60), &policy);
    let tracker = DnsAttributedTcpTracker::default();
    let token = CancellationToken::new();
    let _guard = tracker.register(
        destination,
        ClientHelloScan::NotTls,
        &policy,
        &cache,
        token.clone(),
    );

    assert_eq!(
        tracker.cancel_denied(&policy, &cache),
        1,
        "a policy refresh must terminate any in-flight response before Host is checked again"
    );
    assert!(token.is_cancelled());
}

#[test]
fn policy_resweep_keeps_plaintext_when_the_new_policy_allows_the_ip_directly() {
    use pplx_space_net_common::{EgressDefaultAction, EgressDestinationV1, EgressPolicyV1};

    let host = "pypi.org";
    let destination: SocketAddr = "93.184.216.34:80".parse().unwrap();
    let old_policy = domain_policy(&[host], &[]);
    let new_policy = NetworkPolicyV1 {
        version: Default::default(),
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Ip(destination.ip())],
            Vec::new(),
        )
        .unwrap(),
    };
    let cache = DnsAllowCache::default();
    cache.observe_response(&dns_a_response(host, [93, 184, 216, 34], 60), &old_policy);
    cache.retain_relevant_to_policy(&new_policy);
    let tracker = DnsAttributedTcpTracker::default();
    let token = CancellationToken::new();
    let _guard = tracker.register(
        destination,
        ClientHelloScan::NotTls,
        &old_policy,
        &cache,
        token.clone(),
    );

    assert_eq!(
        tracker.cancel_denied(&new_policy, &cache),
        0,
        "a widening policy must not be overridden by admission-time domain state"
    );
    assert!(!token.is_cancelled());
}

#[test]
fn policy_resweep_closes_unclassified_off_relays_when_interception_turns_on() {
    use pplx_space_net_common::TlsInterception::{Off, On};
    use pplx_space_net_common::{EgressDefaultAction, EgressDestinationV1, EgressPolicyV1};

    let destination: SocketAddr = "93.184.216.34:443".parse().unwrap();
    let address_policy = |tls_interception| NetworkPolicyV1 {
        tls_interception,
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: Vec::new(),
        egress: EgressPolicyV1::new(
            EgressDefaultAction::Deny,
            vec![EgressDestinationV1::Ip(destination.ip())],
            Vec::new(),
        )
        .unwrap(),
        version: Default::default(),
    };
    // Off relays a late, absent or malformed ClientHello opaquely to an
    // address-permitted destination, so the guest can complete TLS inside the
    // relay. On denies every one of those at admission; the resweep must close
    // them. Only a flow the sniff proved plaintext keeps its relay.
    for (scan, cancelled) in [
        (ClientHelloScan::NoServerName, 1),
        (ClientHelloScan::Incomplete, 1),
        (ClientHelloScan::Malformed, 1),
        (ClientHelloScan::NotTls, 0),
    ] {
        let tracker = DnsAttributedTcpTracker::default();
        let token = CancellationToken::new();
        let _guard = tracker.register(
            destination,
            scan.clone(),
            &address_policy(Off),
            &DnsAllowCache::default(),
            token.clone(),
        );
        assert_eq!(
            tracker.cancel_denied(&address_policy(On), &DnsAllowCache::default()),
            cancelled,
            "{scan:?}"
        );
        assert_eq!(token.is_cancelled(), cancelled != 0, "{scan:?}");
    }
}

#[test]
fn exempt_tls_survives_dns_expiry_but_rechecks_current_policy() {
    use crate::dns::test_fixtures::expire_cache;
    use pplx_space_net_common::{EgressDefaultAction, EgressDestinationV1};

    let destination: SocketAddr = "192.0.2.10:443".parse().unwrap();
    let host = "sensor.example.test";
    let scan = ClientHelloScan::ServerName(host.parse().unwrap());
    for default_action in [EgressDefaultAction::Allow, EgressDefaultAction::Deny] {
        for change in [
            "exemption removed",
            "allow removed",
            "host denied",
            "IP denied",
            "subnet denied",
        ] {
            let mut policy = domain_policy(&[host], &[]);
            policy.egress.default_action = default_action;
            policy
                .tls_interception_exemptions
                .push(EgressDestinationV1::Domain(host.parse().unwrap()).into());
            let cache = DnsAllowCache::default();
            assert!(cache.observe_response(&dns_a_response(host, [192, 0, 2, 10], 60), &policy,));
            let tracker = DnsAttributedTcpTracker::default();
            let token = CancellationToken::new();
            let guard = tracker.register(destination, scan.clone(), &policy, &cache, token.clone());
            assert_eq!(guard.interception_mode, TlsInterception::Off);
            assert_eq!(tracker.cancel_denied(&policy, &cache), 0);

            // Expire the same cache the admission and resweep consult, without sleeping.
            expire_cache(&cache);
            assert!(!cache.contains_host_binding(destination.ip(), host));
            policy.egress.deny.push(EgressDestinationV1::Domain(
                "unrelated.example.test".parse().unwrap(),
            ));
            cache.retain_relevant_to_policy(&policy);
            assert_eq!(
                tracker.cancel_denied(&policy, &cache),
                0,
                "an unrelated policy update must preserve the established exempt flow ({default_action:?})"
            );
            assert!(!token.is_cancelled());
            assert_eq!(
                policy.tls_interception_for(
                    destination,
                    dns_attributed_host(destination, &scan, &cache)
                ),
                TlsInterception::On,
                "a new connection cannot reuse the established flow's DNS evidence"
            );

            // A live binding for another tenant on the same IP cannot preserve
            // this flow when its own hostname permission is removed.
            let other_host = "co-tenant.example.test";
            policy
                .egress
                .allow
                .push(EgressDestinationV1::Domain(other_host.parse().unwrap()));
            assert!(
                cache.observe_response(&dns_a_response(other_host, [192, 0, 2, 10], 60), &policy)
            );
            match change {
                "exemption removed" => policy.tls_interception_exemptions.clear(),
                "allow removed" => policy
                    .egress
                    .allow
                    .retain(|rule| *rule != EgressDestinationV1::Domain(host.parse().unwrap())),
                "host denied" => policy
                    .egress
                    .deny
                    .push(EgressDestinationV1::Domain(host.parse().unwrap())),
                "IP denied" => policy
                    .egress
                    .deny
                    .push(EgressDestinationV1::Ip(destination.ip())),
                "subnet denied" => policy
                    .egress
                    .deny
                    .push(EgressDestinationV1::Cidr("192.0.2.0/24".parse().unwrap())),
                _ => unreachable!(),
            }
            cache.retain_relevant_to_policy(&policy);
            let cancelled = usize::from(
                change != "allow removed" || default_action == EgressDefaultAction::Deny,
            );
            assert_eq!(
                tracker.cancel_denied(&policy, &cache),
                cancelled,
                "{default_action:?}: {change}"
            );
            assert_eq!(
                token.is_cancelled(),
                cancelled != 0,
                "{default_action:?}: {change}"
            );
        }
    }
}

#[test]
fn policy_resweep_rechecks_a_port_scoped_exemption_against_the_connection_port() {
    use pplx_space_net_common::{EgressDestinationV1, TlsInterceptionExemptionV1};

    let destination: SocketAddr = "192.0.2.10:5228".parse().unwrap();
    let scan = ClientHelloScan::ServerName("push.example.test".parse().unwrap());
    let exempt_on = |port| {
        let mut policy = v1_allow_all();
        policy.tls_interception = TlsInterception::On;
        policy.tls_interception_exemptions.push(
            TlsInterceptionExemptionV1::new(EgressDestinationV1::Ip(destination.ip()), Some(port))
                .unwrap(),
        );
        policy
    };
    let policy = exempt_on(5228);
    let cache = DnsAllowCache::default();
    let tracker = DnsAttributedTcpTracker::default();
    let token = CancellationToken::new();
    let guard = tracker.register(destination, scan, &policy, &cache, token.clone());
    assert_eq!(guard.interception_mode, TlsInterception::Off);
    assert_eq!(tracker.cancel_denied(&policy, &cache), 0);
    assert!(!token.is_cancelled());

    // Moving the exemption to another port would put the opaque flow under
    // interception, which it cannot join mid-stream.
    assert_eq!(tracker.cancel_denied(&exempt_on(5229), &cache), 1);
    assert!(token.is_cancelled());
}

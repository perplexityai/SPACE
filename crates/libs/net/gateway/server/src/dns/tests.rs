use super::*;
use pplx_space_net_common::{
    EgressDefaultAction, EgressDestinationV1, EgressPolicyV1, NetworkPolicyV1, NetworkPolicyVersion,
};

fn domain_allow_policy(deny: Vec<EgressDestinationV1>) -> NetworkPolicyV1 {
    policy(
        vec![EgressDestinationV1::Domain("example.com".parse().unwrap())],
        deny,
    )
}

fn policy(allow: Vec<EgressDestinationV1>, deny: Vec<EgressDestinationV1>) -> NetworkPolicyV1 {
    NetworkPolicyV1 {
        version: NetworkPolicyVersion::V1,
        tls_interception: Default::default(),
        tls_interception_exemptions: Vec::new(),
        network_credential_rules: vec![],
        egress: EgressPolicyV1::new(EgressDefaultAction::Deny, allow, deny).unwrap(),
    }
}

fn example_ip() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))
}

#[test]
fn observes_allowed_a_record() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &policy);

    assert!(cache.allows_ip(example_ip(), &policy));
}

#[test]
fn clamps_observed_ttl_to_cache_bounds() {
    for (response_ttl_secs, expected_ttl_secs) in [(1, 60), (120, 120), (600, 300)] {
        let policy = domain_allow_policy(vec![]);
        let cache = DnsAllowCache::default();
        let packet = dns_a_response("example.com", [93, 184, 216, 34], response_ttl_secs);
        let before_observation = Instant::now();

        cache.observe_response(&packet, &policy);

        let after_observation = Instant::now();
        let cached = cache.inner.load();
        let entry = &cached[&example_ip()][0];
        let expected_ttl = Duration::from_secs(expected_ttl_secs);
        assert!(entry.expires_at >= before_observation + expected_ttl);
        assert!(entry.expires_at <= after_observation + expected_ttl);
    }
}

#[test]
fn dns_allow_is_bound_to_the_specific_host() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &policy);

    assert!(cache.contains_host_binding(example_ip(), "example.com"));
    assert!(
        !cache.contains_host_binding(example_ip(), "co-tenant.example"),
        "an IP learned for one allowed CDN tenant must not prove another host"
    );
}

#[test]
fn reports_lookup_update_eviction_and_size_metrics() {
    let allow_policy = domain_allow_policy(vec![]);
    let deny_policy = NetworkPolicyV1::deny_all();
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);
    let counter_capture = pplx_space_metrics::test_capture::capture_counters_guard();
    let value_capture = pplx_space_metrics::test_capture::capture_values_guard();

    assert!(cache.observe_response(&packet, &allow_policy));
    assert!(cache.observe_response(&packet, &allow_policy));
    assert!(cache.allows_ip(example_ip(), &allow_policy));
    assert!(!cache.allows_ip(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)), &allow_policy));
    assert!(cache.contains_host_binding(example_ip(), "example.com"));
    assert!(!cache.contains_host_binding(example_ip(), "co-tenant.example"));
    cache.retain_relevant_to_policy(&deny_policy);

    assert_eq!(
        counter_capture.finish(),
        [
            pplx_space_metrics::test_capture::CapturedCounter {
                name: "gateway.dns_attribution_cache.update".to_string(),
                value: 1,
                tags: vec!["result:insert".to_string()],
            },
            pplx_space_metrics::test_capture::CapturedCounter {
                name: "gateway.dns_attribution_cache.update".to_string(),
                value: 1,
                tags: vec!["result:refresh".to_string()],
            },
            pplx_space_metrics::test_capture::CapturedCounter {
                name: "gateway.dns_attribution_cache.lookup".to_string(),
                value: 1,
                tags: vec!["kind:ip_allow".to_string(), "result:hit".to_string()],
            },
            pplx_space_metrics::test_capture::CapturedCounter {
                name: "gateway.dns_attribution_cache.lookup".to_string(),
                value: 1,
                tags: vec!["kind:ip_allow".to_string(), "result:miss".to_string()],
            },
            pplx_space_metrics::test_capture::CapturedCounter {
                name: "gateway.dns_attribution_cache.lookup".to_string(),
                value: 1,
                tags: vec!["kind:host_binding".to_string(), "result:hit".to_string(),],
            },
            pplx_space_metrics::test_capture::CapturedCounter {
                name: "gateway.dns_attribution_cache.lookup".to_string(),
                value: 1,
                tags: vec!["kind:host_binding".to_string(), "result:miss".to_string(),],
            },
            pplx_space_metrics::test_capture::CapturedCounter {
                name: "gateway.dns_attribution_cache.eviction".to_string(),
                value: 1,
                tags: vec!["reason:policy".to_string()],
            },
        ]
    );
    assert_eq!(
        value_capture.finish(),
        [
            pplx_space_metrics::test_capture::CapturedValue {
                name: "gateway.dns_attribution_cache.entries".to_string(),
                value: 1.0,
                tags: Vec::new(),
            },
            pplx_space_metrics::test_capture::CapturedValue {
                name: "gateway.dns_attribution_cache.entries".to_string(),
                value: 1.0,
                tags: Vec::new(),
            },
            pplx_space_metrics::test_capture::CapturedValue {
                name: "gateway.dns_attribution_cache.entries".to_string(),
                value: 0.0,
                tags: Vec::new(),
            },
        ]
    );
}

#[test]
fn shared_ip_retains_a_practical_set_of_exact_host_bindings() {
    let names = (0..16)
        .map(|index| format!("tenant-{index}.example"))
        .collect::<Vec<_>>();
    let allow = names
        .iter()
        .map(|name| EgressDestinationV1::Domain(name.parse().unwrap()))
        .collect();
    let policy = policy(allow, Vec::new());
    let cache = DnsAllowCache::default();

    for name in &names {
        cache.observe_response(&dns_a_response(name, [93, 184, 216, 34], 60), &policy);
    }

    for name in names {
        assert!(
            cache.contains_host_binding(example_ip(), &name),
            "the exact-name correctness gate must retain {name} on a shared edge IP"
        );
    }
}

#[test]
fn ignores_unrelated_additional_answer() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let packet = dns_two_a_response(
        "example.com",
        [93, 184, 216, 34],
        "stuffed.example.net",
        [203, 0, 113, 10],
        60,
    );

    cache.observe_response(&packet, &policy);

    assert!(cache.allows_ip(example_ip(), &policy));
    assert!(!cache.allows_ip(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)), &policy));
}

#[test]
fn observes_cname_chain_answer() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let packet = dns_cname_a_response("example.com", "edge.example.net", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &policy);

    assert!(cache.allows_ip(example_ip(), &policy));
}

#[test]
fn cname_from_unlisted_alias_to_allowed_target_is_cached() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let packet = dns_cname_a_response("alias.example", "example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &policy);

    assert!(cache.allows_ip(example_ip(), &policy));
}

#[test]
fn current_policy_is_checked_before_a_cached_allow_is_used() {
    let allow_policy = domain_allow_policy(vec![]);
    let explicit_deny_policy = domain_allow_policy(vec![EgressDestinationV1::Domain(
        "example.com".parse().unwrap(),
    )]);
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &allow_policy);

    assert!(cache.allows_ip(example_ip(), &allow_policy));
    assert!(
        !cache.allows_ip(example_ip(), &NetworkPolicyV1::deny_all()),
        "a stale cache entry must not grant access before the install hook prunes it"
    );
    assert!(
        !cache.allows_ip(example_ip(), &explicit_deny_policy),
        "an explicit domain deny must override a cached domain allow"
    );
}

#[test]
fn retain_keeps_still_allowed_entries() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &policy);
    cache.retain_relevant_to_policy(&policy);

    assert!(cache.allows_ip(example_ip(), &policy));
}

#[test]
fn retain_removes_entries_disallowed_by_new_policy() {
    let allow_policy = domain_allow_policy(vec![]);
    let deny_policy = NetworkPolicyV1::deny_all();
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &allow_policy);
    cache.retain_relevant_to_policy(&deny_policy);

    assert!(!cache.allows_ip(example_ip(), &deny_policy));
}

#[test]
fn retain_removes_ip_denied_entries() {
    let allow_policy = domain_allow_policy(vec![]);
    let deny_ip_policy = domain_allow_policy(vec![EgressDestinationV1::Cidr(
        "93.184.216.0/24".parse().unwrap(),
    )]);
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &allow_policy);
    cache.retain_relevant_to_policy(&deny_ip_policy);

    assert!(!cache.allows_ip(example_ip(), &deny_ip_policy));
}

#[test]
fn observe_response_does_not_cache_denied_ip() {
    let policy = domain_allow_policy(vec![EgressDestinationV1::Cidr(
        "93.184.216.0/24".parse().unwrap(),
    )]);
    let cache = DnsAllowCache::default();
    let packet = dns_a_response("example.com", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &policy);

    assert!(!cache.allows_ip(example_ip(), &policy));
}

#[test]
fn observe_response_ignores_unsuccessful_or_truncated_responses() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let nxdomain = dns_a_response_with_flags("example.com", [93, 184, 216, 34], 60, 0x8183);
    let servfail = dns_a_response_with_flags("example.com", [93, 184, 216, 34], 60, 0x8182);
    let truncated = dns_a_response_with_flags("example.com", [93, 184, 216, 34], 60, 0x8380);

    assert!(!cache.observe_response(&nxdomain, &policy));
    assert!(!cache.observe_response(&servfail, &policy));
    assert!(!cache.observe_response(&truncated, &policy));

    assert!(!cache.allows_ip(example_ip(), &policy));
}

#[test]
fn retain_keeps_cname_entries_by_original_allowed_domain() {
    let policy = domain_allow_policy(vec![]);
    let cache = DnsAllowCache::default();
    let packet = dns_cname_a_response("example.com", "edge.example.net", [93, 184, 216, 34], 60);

    cache.observe_response(&packet, &policy);
    cache.retain_relevant_to_policy(&policy);

    assert!(cache.allows_ip(example_ip(), &policy));
}

fn dns_a_response(name: &str, ip: [u8; 4], ttl: u32) -> Vec<u8> {
    dns_a_response_with_flags(name, ip, ttl, 0x8180)
}

fn dns_a_response_with_flags(name: &str, ip: [u8; 4], ttl: u32, flags: u16) -> Vec<u8> {
    let mut out = vec![
        0x12, 0x34, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
    ];
    out[2..4].copy_from_slice(&flags.to_be_bytes());
    write_name(&mut out, name);
    out.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
    write_name(&mut out, name);
    out.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
    out.extend_from_slice(&ttl.to_be_bytes());
    out.extend_from_slice(&[0x00, 0x04]);
    out.extend_from_slice(&ip);
    out
}

fn dns_two_a_response(
    first_name: &str,
    first_ip: [u8; 4],
    second_name: &str,
    second_ip: [u8; 4],
    ttl: u32,
) -> Vec<u8> {
    let mut out = vec![
        0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
    ];
    write_name(&mut out, first_name);
    out.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
    write_a_answer(&mut out, first_name, first_ip, ttl);
    write_a_answer(&mut out, second_name, second_ip, ttl);
    out
}

fn dns_cname_a_response(name: &str, cname: &str, ip: [u8; 4], ttl: u32) -> Vec<u8> {
    let mut out = vec![
        0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
    ];
    write_name(&mut out, name);
    out.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
    write_name(&mut out, name);
    out.extend_from_slice(&[0x00, 0x05, 0x00, 0x01]);
    out.extend_from_slice(&ttl.to_be_bytes());
    let len_pos = out.len();
    out.extend_from_slice(&[0x00, 0x00]);
    let before = out.len();
    write_name(&mut out, cname);
    let rdlen = u16::try_from(out.len() - before).unwrap();
    out[len_pos..len_pos + 2].copy_from_slice(&rdlen.to_be_bytes());
    write_a_answer(&mut out, cname, ip, ttl);
    out
}

fn write_a_answer(out: &mut Vec<u8>, name: &str, ip: [u8; 4], ttl: u32) {
    write_name(out, name);
    out.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
    out.extend_from_slice(&ttl.to_be_bytes());
    out.extend_from_slice(&[0x00, 0x04]);
    out.extend_from_slice(&ip);
}

fn write_name(out: &mut Vec<u8>, name: &str) {
    for label in name.split('.') {
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0);
}

#[test]
fn exemption_dns_evidence_never_grants_egress() {
    let host = "sensor.example.test";
    let ip: IpAddr = "192.0.2.10".parse().unwrap();
    for mut policy in [NetworkPolicyV1::deny_all(), NetworkPolicyV1::allow_all()] {
        policy
            .tls_interception_exemptions
            .push(EgressDestinationV1::Domain(host.parse().unwrap()).into());
        let cache = DnsAllowCache::default();
        assert!(cache.observe_response(&dns_a_response(host, [192, 0, 2, 10], 60), &policy));
        cache.retain_relevant_to_policy(&policy);
        assert!(cache.contains_host_binding(ip, host));
        assert!(!cache.allows_ip(ip, &policy));
        assert_eq!(
            policy.egress_decision_for_ip_with_dns_allow(ip, cache.allows_ip(ip, &policy)),
            policy.egress_decision_for_ip(ip)
        );
        policy.tls_interception_exemptions.clear();
        cache.retain_relevant_to_policy(&policy);
        assert!(!cache.contains_host_binding(ip, host));
    }
}

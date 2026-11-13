//! Real Linux network namespaces; both tests need CAP_NET_ADMIN and
//! CAP_SYS_ADMIN, iproute2 and nftables, and no VM. The benchmark is
//! explicitly opt-in, in a disposable Linux network environment:
//! SPACE_RUN_CONTAINMENT_BENCHMARK=1 cargo test -p pplx-space-node-mgr --lib \
//!     linux_containment_benchmark -- --ignored --nocapture
use super::super::*;
use std::process::Command;
use std::time::Instant;

fn command(args: &[&str]) -> String {
    let output = Command::new(args[0]).args(&args[1..]).output().unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[derive(Default)]
struct Namespaces(Vec<String>);

impl Drop for Namespaces {
    fn drop(&mut self) {
        for namespace in &self.0 {
            // Delete only namespaces successfully created by this test.
            let output = Command::new("ip")
                .args(["netns", "del", namespace])
                .output();
            if !output.as_ref().is_ok_and(|out| out.status.success()) {
                eprintln!("failed to remove benchmark namespace {namespace}: {output:?}");
            }
        }
    }
}

#[test]
#[ignore = "opt-in real Linux namespace benchmark"]
fn linux_containment_benchmark() {
    if std::env::var("SPACE_RUN_CONTAINMENT_BENCHMARK").as_deref() != Ok("1") {
        return;
    }
    let prefix = format!("containment-{}", uuid::Uuid::now_v7().simple());
    let mut namespaces = Namespaces::default();
    for index in 0..128 {
        let namespace = format!("{prefix}-{index}");
        command(&["ip", "netns", "add", &namespace]);
        namespaces.0.push(namespace.clone());
        // A synthetic link named tap0 exercises the real namespace-local netdev
        // hook and nft subprocesses without requiring a guest or /dev/net/tun.
        command(&[
            "ip", "-n", &namespace, "link", "add", "tap0", "type", "dummy",
        ]);
        command(&[
            "ip", "netns", "exec", &namespace, "nft", "add", "table", "ip", "sandbox",
        ]);
        command(&[
            "ip", "netns", "exec", &namespace, "nft", "add", "chain", "ip", "sandbox", "forward",
        ]);
        command(&[
            "ip", "netns", "exec", &namespace, "nft", "add", "table", "ip", "proxy",
        ]);
        command(&[
            "ip",
            "netns",
            "exec",
            &namespace,
            "nft",
            "add",
            "chain",
            "ip",
            "proxy",
            "prerouting",
        ]);
    }
    let shutdown = CancellationToken::new();
    // Prime the already-contained restart case; alternate order to reduce cache bias.
    assert_eq!(
        contain_namespaces(&namespaces.0, CONCURRENCY, &shutdown, contain_namespace).unwrap(),
        128
    );
    for round in 0..3 {
        for workers in if round % 2 == 0 { [1, 4] } else { [4, 1] } {
            let started = Instant::now();
            assert_eq!(
                contain_namespaces(
                    &namespaces.0,
                    NonZeroUsize::new(workers).unwrap(),
                    &shutdown,
                    contain_namespace
                )
                .unwrap(),
                128
            );
            println!(
                "containment benchmark: namespaces=128 workers={workers} elapsed_ms={:.1}",
                started.elapsed().as_secs_f64() * 1000.0
            );
        }
    }
    // Every namespace must retain the restricted local reply rule after parallel startup.
    for namespace in &namespaces.0 {
        let rules: serde_json::Value = serde_json::from_str(&command(&[
            "ip",
            "netns",
            "exec",
            namespace,
            "nft",
            "-j",
            "list",
            "chain",
            "ip",
            "proxy",
            "prerouting",
        ]))
        .unwrap();
        let count = rules["nftables"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry.get("rule").is_some())
            .count();
        assert_eq!(
            count, 1,
            "containment must not duplicate the local reply exception"
        );
    }
    let names = namespaces.0.clone();
    drop(namespaces);
    for name in names {
        assert!(!std::path::Path::new("/run/netns").join(name).exists());
    }
}

fn rule_comments(snapshot: &firewall::RulesetSnapshot, table: &str, chain: &str) -> Vec<String> {
    snapshot.chain("ip", table, chain).unwrap()["nftables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["rule"]["comment"].as_str().map(str::to_string))
        .collect()
}

#[test]
fn containment_checks_and_repairs_the_state_its_own_writes_leave() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let test_namespace = crate::netns_tests::TestNamespace::create();
    let namespace = test_namespace.name().to_string();
    firewall::install_namespace_rules(
        &namespace,
        "vpeer-000001",
        "10.12.0.1",
        net::GUEST_IP,
        firewall::packaged_pre_tproxy_guardrails_policy_for_test(),
    )
    .unwrap();
    firewall::setup_netgtw_tproxy_rules(&namespace, 100_001, "vpeer-000001", false).unwrap();
    // Break the L2 policy, which only the containment write itself repairs,
    // and add the legacy reply exceptions the repair removes.
    let nft = |rule: &str| {
        command(&[
            "ip",
            "netns",
            "exec",
            &namespace,
            "sh",
            "-c",
            &format!("nft {rule}"),
        ]);
    };
    nft("flush chain netdev sandbox_l2 ingress");
    nft(
        "insert rule ip sandbox forward ct state established,related accept comment ct-inbound-replies",
    );
    nft(
        "insert rule ip proxy prerouting iifname tap0 ct state established,related return comment skip-tproxy-inbound-replies",
    );

    assert!(contain_namespace(&namespace).unwrap());

    let snapshot = firewall::RulesetSnapshot::read(&namespace).unwrap();
    firewall::validate_installed_guest_l2_ingress_policy(&snapshot, &namespace).unwrap();
    let forward = rule_comments(&snapshot, "sandbox", "forward");
    assert!(
        !forward
            .iter()
            .any(|comment| comment == "ct-inbound-replies"),
        "{forward:?}"
    );
    let proxy = rule_comments(&snapshot, "proxy", "prerouting");
    assert!(
        !proxy
            .iter()
            .any(|comment| comment == "skip-tproxy-inbound-replies"),
        "{proxy:?}"
    );
    assert_eq!(
        proxy
            .iter()
            .filter(|comment| *comment == "skip-tproxy-local-service-replies")
            .count(),
        1,
        "{proxy:?}"
    );
    // The contained namespace needs no further repair.
    assert!(!firewall::constrain_direct_inbound_reply_path_from(&snapshot, &namespace).unwrap());
}

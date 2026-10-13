//! Explicitly opt-in: run only in a disposable Linux network environment with
//! CAP_NET_ADMIN and CAP_SYS_ADMIN, iproute2 and nftables. No VM is required.
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

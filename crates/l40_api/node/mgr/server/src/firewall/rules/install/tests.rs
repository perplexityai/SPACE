//! Installation against nftables in disposable network namespaces. Needs
//! root, iproute2 and nftables; listed in the nextest privileged group.
use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use pplx_space_firewall_rules_protocol::decode_revision;
use serde_json::{Value, json};

use super::{InstallOutcome, install, submit};
use crate::firewall::rules::{TableConstruction, render};
use crate::firewall::{apply_nft_json, nft_output};
use crate::netns_tests::TestNamespace;

const PRIVILEGE: &str = "create network namespaces and nftables tables";

fn pinned(namespace: &TestNamespace) -> File {
    File::open(format!("/run/netns/{}", namespace.name())).unwrap()
}

fn construction(rules: &str) -> TableConstruction {
    render(&decode_revision(&format!(r#"{{"schema_version":1,"rules":[{rules}]}}"#)).unwrap())
}

fn udp_port(port: u16) -> TableConstruction {
    construction(&format!(
        r#"{{"id":"dns","all_of":[{{"left":{{"payload":{{"protocol":"udp","field":"dport"}}}},"op":"==","right":{port}}}]}}"#
    ))
}

fn list(namespace: &TestNamespace, args: &[&str]) -> Option<Value> {
    let output = nft_output(Some(namespace.name()), args).unwrap();
    output
        .status
        .success()
        .then(|| serde_json::from_slice(&output.stdout).unwrap())
}

fn ruleset(namespace: &TestNamespace) -> Value {
    list(namespace, &["-j", "list", "ruleset"]).unwrap()
}

/// The owned table as listed, without kernel handles, in the form a
/// construction adds it; None while the table is absent.
fn owned_table(namespace: &TestNamespace) -> Option<Vec<Value>> {
    list(
        namespace,
        &["-j", "list", "table", "ip", "space_firewall_rules"],
    )
    .map(as_added)
}

fn as_added(listing: Value) -> Vec<Value> {
    let objects = listing["nftables"].as_array().unwrap().iter();
    let mut added = Vec::new();
    for object in objects.filter(|object| object.get("metainfo").is_none()) {
        let mut object = object.clone();
        for body in object.as_object_mut().unwrap().values_mut() {
            body.as_object_mut().unwrap().remove("handle");
        }
        added.push(json!({"add": object}));
    }
    added
}

/// Runs `f` on a thread moved into a fresh network namespace. Threads and
/// processes `f` starts inherit it, so a call that never enters its target
/// changes that namespace rather than the runner's.
fn contained<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET).unwrap();
                f()
            })
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

fn committed(construction: &TableConstruction, netns: &File) {
    let outcome = contained(|| install(construction, netns));
    assert!(matches!(outcome, InstallOutcome::Committed), "{outcome:?}");
}

// Catches a first install or replacement that fails, duplicates rules, leaves
// old rules behind, or reaches another namespace or a baseline table.
#[test]
fn install_creates_and_replaces_only_the_owned_table() {
    if !pplx_space_test_env::root_or_skip(PRIVILEGE) {
        return;
    }
    let target = TestNamespace::create();
    let other = TestNamespace::create();
    let netns = pinned(&target);
    let other_table = json!({"nftables": udp_port(99).commands()}).to_string();
    apply_nft_json(&other_table, Some(other.name())).unwrap();
    apply_nft_json(
        r#"{"nftables":[{"add":{"table":{"family":"ip","name":"sandbox"}}},
            {"add":{"chain":{"family":"ip","table":"sandbox","name":"keep",
              "type":"filter","hook":"forward","prio":0}}}]}"#,
        Some(target.name()),
    )
    .unwrap();
    let other_before = ruleset(&other);
    let baseline = ["-j", "list", "table", "ip", "sandbox"];
    let baseline_before = list(&target, &baseline).unwrap();
    assert_eq!(owned_table(&target), None);

    let empty = construction("");
    for desired in [udp_port(53), udp_port(54), udp_port(54), empty] {
        committed(&desired, &netns);
        assert_eq!(owned_table(&target).as_deref(), Some(desired.commands()));
    }
    assert_eq!(ruleset(&other), other_before);
    assert_eq!(list(&target, &baseline), Some(baseline_before));
}

// Catches a replacement split into separate delete and rebuild commits: a
// late kernel refusal must leave the old table and its handles in place. This
// is the usual Rejected case, not a promise Rejected makes.
#[test]
fn late_kernel_refusal_leaves_the_previous_table() {
    if !pplx_space_test_env::root_or_skip(PRIVILEGE) {
        return;
    }
    let target = TestNamespace::create();
    let netns = pinned(&target);
    committed(&udp_port(53), &netns);
    let before = ruleset(&target);

    // Only the kernel checks rule handles, so nft submits the whole batch and
    // the kernel refuses it after the table delete and rebuild.
    let mut replacement = udp_port(54);
    replacement.0.push(json!({"delete": {"rule": {
        "family": "ip", "table": "space_firewall_rules", "chain": "guest_drop",
        "handle": 999_999,
    }}}));
    let outcome = contained(|| install(&replacement, &netns));
    // nft's wording for a refusal from the kernel, not from its own checks.
    assert!(
        matches!(&outcome, InstallOutcome::Rejected { stderr } if stderr.contains("Could not process rule")),
        "{outcome:?}"
    );
    assert_eq!(ruleset(&target), before);
}

// Catches an installer that waits without bound, claims an outcome after a
// timeout, or leaves the timed-out child running.
#[test]
fn timeout_after_dispatch_is_unknown_and_reaps_the_child() {
    if !pplx_space_test_env::root_or_skip(PRIVILEGE) {
        return;
    }
    let target = TestNamespace::create();
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let fake = dir.path().join("nft");
    // Reads the whole batch, then outlives the budget as the same process.
    std::fs::write(
        &fake,
        format!(
            "#!/bin/sh\ncat >/dev/null\necho $$ >{}\nexec sleep 60\n",
            pid_file.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let outcome = contained(|| {
        submit(
            &udp_port(53),
            &pinned(&target),
            fake.to_str().unwrap(),
            Duration::from_millis(500),
        )
    });
    let InstallOutcome::Unknown { error } = outcome else {
        panic!("expected Unknown, got {outcome:?}");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut, "{error}");
    let pid =
        std::fs::read_to_string(&pid_file).expect("fake nft read the batch before the deadline");
    let pid = pid.trim();
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "fake nft {pid} outlived the installer"
    );
}

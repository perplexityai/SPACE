//! The coordinator against nftables in disposable network namespaces. Needs
//! root, iproute2 and nftables; listed in the nextest privileged group.

use super::*;
use crate::netns_tests::{TestNamespace, contained};

const PRIVILEGE: &str = "create network namespaces and nftables tables";

/// Production nft, with the slot's namespace entry redirected to a test one.
struct Redirected(String);

impl SlotHost for Redirected {
    fn namespace_path(&self, _: &SlotInfo) -> PathBuf {
        format!("/run/netns/{}", self.0).into()
    }

    fn nft(
        &self,
        netns: &File,
        args: &[&str],
        stdin: &[u8],
        budget: Duration,
    ) -> io::Result<Output> {
        Live.nft(netns, args, stdin, budget)
    }
}

fn nft(namespace: &TestNamespace, args: &[&str], stdin: &[u8]) -> Value {
    let netns = File::open(format!("/run/netns/{}", namespace.name())).unwrap();
    let output = contained(|| {
        crate::firewall::rules::install::nft(&netns, args, stdin, crate::host_command::PROBE_BUDGET)
    })
    .unwrap();
    assert!(output.status.success(), "nft {args:?}: {output:?}");
    if output.stdout.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&output.stdout).unwrap()
    }
}

fn add(namespace: &TestNamespace, commands: Value) {
    nft(
        namespace,
        &["-j", "-f", "-"],
        json!({"nftables": commands}).to_string().as_bytes(),
    );
}

/// The owned table as listed, without handles or metainfo.
fn owned_table(namespace: &TestNamespace) -> Vec<Value> {
    let listing = nft(
        namespace,
        &["-j", "list", "table", "ip", "space_firewall_rules"],
        b"",
    );
    listing["nftables"].as_array().unwrap()[1..]
        .iter()
        .map(|object| {
            let mut object = object.clone();
            for body in object.as_object_mut().unwrap().values_mut() {
                body.as_object_mut().unwrap().remove("handle");
            }
            object
        })
        .collect()
}

// Catches an apply that misses the slot's namespace, leaves the previous owned
// table, touches the baseline table or a sibling namespace, or reports evidence
// for an object other than the one it pinned.
#[test]
fn applies_the_accepted_revision_only_to_the_slot_namespace() {
    if !pplx_space_test_env::root_or_skip(PRIVILEGE) {
        return;
    }
    let target = TestNamespace::create();
    let sibling = TestNamespace::create();
    let stale = json!(
        installed(WEB)
            .into_iter()
            .map(|object| json!({"add": object}))
            .collect::<Vec<_>>()
    );
    add(&target, stale.clone());
    add(&sibling, stale);
    add(
        &target,
        json!([{"add": {"table": {"family": "ip", "name": "sandbox"}}},
               {"add": {"chain": {"family": "ip", "table": "sandbox", "name": "keep",
                 "type": "filter", "hook": "forward", "prio": 0}}}]),
    );
    let baseline = ["-j", "list", "table", "ip", "sandbox"];
    let baseline_before = nft(&target, &baseline, b"");
    let sibling_before = nft(&sibling, &["-j", "list", "ruleset"], b"");

    let (pool, _jobs) = pool(1);
    let (_dir, store) = store(Some((12, DNS)));
    leased(&pool, 0);
    let host = Arc::new(Redirected(target.name().to_string()));
    let request = SlotFirewallRequest {
        node_id: NODE,
        generation: 12,
        slot_id: 0,
    };
    // A blocking thread the runtime starts inherits this contained namespace.
    let report = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(pool.apply_firewall_rules_with(&store, request, host))
            .unwrap()
    });
    let attempt = attempted(report);
    assert!(attempt.verified(), "{attempt:?}");
    assert!(matches!(attempt.submitted, InstallOutcome::Committed));
    let entry = format!("/run/netns/{}", target.name());
    assert_eq!(
        Some(attempt.namespace),
        identity(std::fs::metadata(entry)).ok()
    );
    assert_eq!(owned_table(&target), installed(DNS));
    assert_eq!(nft(&target, &baseline, b""), baseline_before);
    assert_eq!(
        nft(&sibling, &["-j", "list", "ruleset"], b""),
        sibling_before
    );
}

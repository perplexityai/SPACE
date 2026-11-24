//! The coordinator against nftables in disposable network namespaces. Needs
//! root, iproute2 and nftables; listed in the nextest privileged group.

use super::*;
use crate::netns_tests::{TestNamespace, contained};

/// Each slot's namespace entry, redirected to the test one at its index.
pub(crate) struct Redirected(pub(crate) Vec<String>);

impl SlotHost for Redirected {
    fn namespace_path(&self, slot: &SlotInfo) -> PathBuf {
        format!("/run/netns/{}", self.0[slot.slot as usize]).into()
    }
}

pub(crate) fn list(namespace: &TestNamespace, args: &[&str]) -> Value {
    let netns = File::open(format!("/run/netns/{}", namespace.name())).unwrap();
    let budget = crate::host_command::PROBE_BUDGET;
    let output = contained(|| crate::firewall::rules::nft(&netns, args, b"", budget)).unwrap();
    assert!(output.status.success(), "nft {args:?}: {output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The owned table in `namespace`, without the handles nft assigns.
pub(crate) fn owned_table(namespace: &TestNamespace) -> Vec<Value> {
    let table = ["-j", "list", "table", "ip", "space_firewall_rules"];
    let mut listed = list(namespace, &table)["nftables"].as_array().unwrap()[1..].to_vec();
    for object in &mut listed {
        for body in object.as_object_mut().unwrap().values_mut() {
            body.as_object_mut().unwrap().remove("handle");
        }
    }
    listed
}

// Catches an apply that misses the slot's namespace or touches a sibling one.
#[test]
fn applies_the_accepted_revision_only_to_the_slot_namespace() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let target = TestNamespace::create();
    let sibling = TestNamespace::create();
    let sibling_before = list(&sibling, &["-j", "list", "ruleset"]);
    let (pool, _jobs) = pool(1);
    let (_dir, store) = store(Some(12));
    set_slot(&pool, 0, SlotState::Leased, 1);
    let host = Arc::new(Redirected(vec![target.name().to_string()]));
    let request = SlotFirewallRequest {
        node_id: NODE,
        generation: 12,
        slot_id: 0,
        expected: None,
    };
    // A blocking thread the runtime starts inherits this contained namespace.
    let report = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(pool.apply_firewall_rules_with(
                &store,
                request,
                host,
                Arc::new(crate::firewall::rules::nft),
            ))
            .unwrap()
    });
    let attempt = attempted(report);
    assert!(attempt.verified(), "{attempt:?}");
    let entry = format!("/run/netns/{}", target.name());
    assert_eq!(
        Some(attempt.namespace),
        identity(std::fs::metadata(entry)).ok()
    );

    assert_eq!(owned_table(&target), installed(12));
    assert_eq!(list(&sibling, &["-j", "list", "ruleset"]), sibling_before);
}

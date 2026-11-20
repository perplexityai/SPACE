//! One delivery through nftables in disposable network namespaces. Needs
//! root, iproute2 and nftables; listed in the nextest privileged group.

use super::*;
use crate::netns_tests::{TestNamespace, contained};
use crate::slot_pool::firewall_rules::tests::linux::{Redirected, list, owned_table};

// Catches a delivery that misses the bound slot's namespace or touches another.
#[test]
fn a_delivery_applies_only_to_the_bound_namespace() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let target = TestNamespace::create();
    let sibling = TestNamespace::create();
    let sibling_before = list(&sibling, &["-j", "list", "ruleset"]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    // A blocking thread the runtime starts inherits this contained namespace.
    let replied = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let mut service = service_for_unit_tests().await;
                service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
                set_slot(&service.pool, SLOT, SlotState::Leased, 1);
                let service = service
                    .with_firewall_sync(FirewallSyncFixture {
                        node_id: NODE,
                        slot_id: SLOT,
                        assignment_path: path.clone(),
                        host: Arc::new(Redirected(target.name().to_string())),
                        nft: Arc::new(crate::firewall::rules::nft),
                        wait: WAIT,
                    })
                    .unwrap();
                reply(sync(&service, delivery(12)).await)
            })
    });
    let committed = FirewallAssignmentAcceptance::Committed;
    assert_eq!(replied, (12, committed, Outcome::Matched));
    let reopened = FirewallAssignments::open(&path).unwrap();
    assert_eq!(reopened.current().unwrap().unwrap().generation, 12);
    assert_eq!(owned_table(&target), installed(12));
    assert_eq!(list(&sibling, &["-j", "list", "ruleset"]), sibling_before);
}

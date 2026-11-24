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
                        slots: BoundSlots::One(SLOT),
                        assignment_path: path.clone(),
                        host: Arc::new(Redirected(vec![
                            sibling.name().to_string(),
                            target.name().to_string(),
                        ])),
                        nft: Arc::new(crate::firewall::rules::nft),
                        wait: WAIT,
                        manager_epoch: EPOCH,
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

// Catches a node-wide delivery that misses a leased or the idle slot's
// namespace, or touches an ineligible one.
#[test]
fn a_delivery_applies_to_every_eligible_namespace() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let eligible = [(); 3].map(|()| TestNamespace::create());
    let dirty = TestNamespace::create();
    let dirty_before = list(&dirty, &["-j", "list", "ruleset"]);
    let names = eligible
        .iter()
        .chain([&dirty])
        .map(|namespace| namespace.name().to_string());
    let host = Arc::new(Redirected(names.collect()));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    // A blocking thread the runtime starts inherits this contained namespace.
    let passed = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let mut service = service_for_unit_tests().await;
                service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(4));
                let states = [SlotState::Leased, SlotState::Leased, SlotState::Ready];
                for (slot, state) in (0..).zip(states.into_iter().chain([SlotState::Dirty])) {
                    set_slot(&service.pool, slot, state, 1);
                }
                let service = service
                    .with_firewall_sync(FirewallSyncFixture {
                        node_id: NODE,
                        slots: BoundSlots::All,
                        assignment_path: path.clone(),
                        host,
                        nft: Arc::new(crate::firewall::rules::nft),
                        wait: WAIT,
                        manager_epoch: EPOCH,
                    })
                    .unwrap();
                sync_all(&service, 12).await;
                passed(&service, 12).await
            })
    });
    let matched = Some(Outcome::Matched);
    assert_eq!(passed.unwrap(), [(0, matched), (1, matched), (2, matched)]);
    for namespace in &eligible {
        assert_eq!(owned_table(namespace), installed(12), "{namespace:?}");
    }
    assert_eq!(list(&dirty, &["-j", "list", "ruleset"]), dirty_before);
}

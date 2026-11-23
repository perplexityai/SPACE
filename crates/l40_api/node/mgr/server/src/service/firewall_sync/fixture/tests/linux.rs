//! Deliveries and observations through nftables in disposable network
//! namespaces. Needs root, iproute2 and nftables; listed in the nextest
//! privileged group.

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

// Catches an observation whose verdict disagrees with what nft lists in a real
// sandbox namespace, or that writes to it.
#[test]
fn a_new_round_reads_drift_that_a_retried_round_replays() {
    use crate::slot_pool::{Census, Failure, FirewallObservation, SlotEvidence, SlotOutcome};
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let [leased, idle] = [(); 2].map(|()| TestNamespace::create());
    let host = FakeHost::new(None);
    let namespaces = [&leased, &idle].map(|namespace| format!("/run/netns/{}", namespace.name()));
    let pinned = [0, 1].map(|slot| host.point(slot, &namespaces[slot as usize]));
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = writes.clone();
    let nft = move |netns: &File, args: &[&str], stdin: &[u8], budget| {
        if args.contains(&"-f") {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        crate::firewall::rules::nft(netns, args, stdin, budget)
    };
    let written = || writes.load(std::sync::atomic::Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    // A blocking thread the runtime starts inherits this contained namespace.
    let (round_7, retried, round_8, observing) = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let mut service = service_for_unit_tests().await;
                service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
                set_slot(&service.pool, 0, SlotState::Leased, 1);
                set_slot(&service.pool, 1, SlotState::Ready, 1);
                let service = service
                    .with_firewall_sync(FirewallSyncFixture {
                        node_id: NODE,
                        slots: BoundSlots::All,
                        assignment_path: path.clone(),
                        host: host.clone(),
                        nft: Arc::new(nft),
                        wait: WAIT,
                    })
                    .unwrap();
                sync_all(&service, 12).await;
                let matched = Some(Outcome::Matched);
                assert_eq!(
                    passed(&service, 12).await.unwrap(),
                    [(0, matched), (1, matched)]
                );
                let applied = written();
                let (sync, pool) = (service.firewall_sync.clone().unwrap(), &service.pool);
                let round_7 = bounded(sync.observe(pool, NODE, 12, 7).unwrap()).await;
                let idle = File::open(&namespaces[1]).unwrap();
                let flush = ["flush", "chain", "ip", "space_firewall_rules", "guest_drop"];
                let budget = crate::host_command::PROBE_BUDGET;
                let flushed = crate::firewall::rules::nft(&idle, &flush, b"", budget).unwrap();
                assert!(flushed.status.success(), "{flushed:?}");
                let retried = bounded(sync.observe(pool, NODE, 12, 7).unwrap()).await;
                let round_8 = bounded(sync.observe(pool, NODE, 12, 8).unwrap()).await;
                (round_7, retried, round_8, written() - applied)
            })
    });
    assert!(Arc::ptr_eq(&round_7, &retried));
    let slot = |slot_id: u32, sandbox: Option<&str>, outcome| SlotEvidence {
        slot_id,
        target: SlotTarget {
            slot_generation: 1,
            sandbox_id: sandbox.map(Into::into),
            namespace: Some(pinned[slot_id as usize]),
        },
        outcome,
    };
    let observation = |idle| FirewallObservation {
        node_id: NODE,
        generation: 12,
        revision_id: Uuid::from_u128(12),
        census: Census::default(),
        slots: vec![
            slot(0, Some("sandbox-0"), SlotOutcome::Verified),
            slot(1, None, idle),
        ],
    };
    let mismatch = SlotOutcome::Failed(Failure::RulesMismatch);
    assert_eq!(
        round_7.answer.as_ref().unwrap(),
        &observation(SlotOutcome::Verified)
    );
    assert_eq!(round_8.answer.as_ref().unwrap(), &observation(mismatch));
    assert_eq!(observing, 0, "an observation wrote nft");
    assert_eq!(owned_table(&leased), installed(12));
    // The table and chain, without the flushed rule.
    assert_eq!(
        owned_table(&idle),
        installed(12)[..2],
        "the drift was repaired"
    );
}

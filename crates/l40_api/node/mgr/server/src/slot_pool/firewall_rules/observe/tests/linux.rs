//! Observation against nftables in disposable network namespaces. Needs root,
//! iproute2 and nftables; listed in the nextest privileged group.

use super::*;
use crate::firewall::rules::install::{InstallOutcome, install_with};
use crate::firewall::rules::render;
use crate::netns_tests::{TestNamespace, contained};
use crate::slot_pool::firewall_rules::tests::linux::{Redirected, owned_table};

// Catches a verdict that disagrees with what nft lists in a real sandbox
// namespace, or an observation that repairs the drift it reads.
#[test]
fn reads_real_tables_and_leaves_drift_alone() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let [matching, drifted] = [(); 2].map(|()| TestNamespace::create());
    let (pool, _jobs) = pool(2);
    let (_dir, store) = store(Some(12));
    for slot_id in [0, 1] {
        set_slot(&pool, slot_id, SlotState::Leased, 1);
    }
    let revision = store.current().unwrap().unwrap().revision.clone();
    let budget = crate::host_command::PROBE_BUDGET;
    for namespace in [&matching, &drifted] {
        let netns = File::open(format!("/run/netns/{}", namespace.name())).unwrap();
        let outcome = contained(|| install_with(&render(&revision), &netns, &nft));
        assert!(matches!(outcome, InstallOutcome::Committed), "{outcome:?}");
    }
    let netns = File::open(format!("/run/netns/{}", drifted.name())).unwrap();
    let flush = ["flush", "chain", "ip", "space_firewall_rules", "guest_drop"];
    let flushed = contained(|| nft(&netns, &flush, b"", budget)).unwrap();
    assert!(flushed.status.success(), "{flushed:?}");
    let names = [&matching, &drifted].map(|namespace| namespace.name().to_string());
    let host = Arc::new(Redirected(names.into()));
    // A blocking thread the runtime starts inherits this contained namespace.
    let observed = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(pool.observe_firewall_rules_with(
                &store,
                (NODE, 12),
                host,
                Arc::new(nft),
                |_, _| false,
                SWEEP_BUDGET,
            ))
            .unwrap()
    });
    let outcomes: Vec<_> = observed.slots.iter().map(|slot| slot.outcome).collect();
    let mismatch = SlotOutcome::Failed(Failure::RulesMismatch);
    assert_eq!(outcomes, [SlotOutcome::Verified, mismatch]);
    // The table and chain, without the flushed rule.
    assert_eq!(owned_table(&drifted), installed(12)[..2]);
}

//! Observation against the slot apply's fake nft, whose namespace links sit in
//! the fake host's own declared directory.

use super::*;
use crate::slot_pool::firewall_rules::NamespaceIdentity;
use crate::slot_pool::firewall_rules::tests::{
    FakeHost, Fault, NODE, accept, bounded, busy, fake_nft, heal, hook, installed, poison, pool,
    set_slot, store,
};
use crate::slot_pool::{SlotInfo, SlotState};

mod linux;

async fn observe(
    pool: &Arc<NetworkPool>,
    store: &Arc<FirewallAssignments>,
    host: Arc<dyn SlotHost>,
    nft: &Arc<Nft<'static>>,
    budget: Duration,
) -> Result<FirewallObservation, Refusal> {
    let not_applying = |_: u32, _: &SlotTarget| false;
    let observed = pool.observe_firewall_rules_with(
        store,
        (NODE, 12),
        host,
        nft.clone(),
        not_applying,
        budget,
    );
    bounded(observed).await
}

fn slot(
    slot_id: u32,
    leased: bool,
    namespace: Option<NamespaceIdentity>,
    outcome: SlotOutcome,
) -> SlotEvidence {
    let target = SlotTarget {
        slot_generation: 1,
        sandbox_id: leased.then(|| format!("sandbox-{slot_id}")),
        namespace,
    };
    SlotEvidence {
        slot_id,
        target,
        outcome,
    }
}

/// The fake host's links, but no declared directory, as on a live host.
struct Undeclared(Arc<FakeHost>);

impl SlotHost for Undeclared {
    fn namespace_path(&self, slot: &SlotInfo) -> PathBuf {
        self.0.namespace_path(slot)
    }
}

// Catches a census that ignores a missing, unreadable or extra managed
// namespace, a failed inventory, an undeclared directory or the deadline, a
// record left out, a slot mid-deletion, unlistable or busy misreported, or nft
// written.
#[tokio::test]
async fn the_census_accounts_for_every_record_and_managed_namespace() {
    use SlotState::{Dirty, Leased, Missing, Ready};
    let (pool, _jobs) = pool(6);
    let (_dir, store) = store(Some(12));
    let host = FakeHost::new(None);
    for (slot_id, state) in (0..).zip([Leased, Leased, Ready, Dirty, Missing, Leased]) {
        set_slot(&pool, slot_id, state, 1);
    }
    let verified = host.name(0, "net");
    host.tables.lock().unwrap().insert(verified, installed(12));
    // Slot 1 has no namespace; slot 2's entry is mid-deletion, a plain file.
    std::fs::write(host.link(2), b"").unwrap();
    // A transitional or setup-interrupted slot keeps its namespace.
    host.name(3, "uts");
    host.name(4, "ipc");
    // Slot 5's entry can't be opened at all.
    std::os::unix::fs::symlink(host.link(5), host.link(5)).unwrap();
    host.name(9, "cgroup");
    let _busy = busy(&pool, 0);
    let nft = fake_nft(&host);

    let ineligible = SlotOutcome::Unknown(Unknown::Ineligible);
    let missing = SlotOutcome::Unknown(Unknown::NamespaceMissing);
    let slots = |first: SlotOutcome| {
        vec![
            slot(0, true, Some(verified), first),
            slot(1, true, None, missing),
            slot(2, false, None, missing),
            slot(3, true, None, ineligible),
            slot(4, true, None, ineligible),
            slot(
                5,
                true,
                None,
                SlotOutcome::Unknown(Unknown::NamespaceUnreadable),
            ),
        ]
    };
    let observed = observe(&pool, &store, host.clone(), &nft, SWEEP_BUDGET).await;
    let expected = FirewallObservation {
        node_id: NODE,
        generation: 12,
        revision_id: Uuid::from_u128(12),
        census: Census {
            gaps: vec![
                CensusGap::Missing { slot_id: 1 },
                CensusGap::Missing { slot_id: 2 },
                CensusGap::Unread { slot_id: 5 },
                CensusGap::Extra {
                    entry: host.link(9),
                },
            ],
        },
        slots: slots(SlotOutcome::Verified),
    };
    assert_eq!(observed.unwrap(), expected);

    let undeclared = Arc::new(Undeclared(host.clone()));
    let unlistable = fake_nft(&FakeHost::new(Some(Fault::Unlistable)));
    let observed = observe(&pool, &store, undeclared, &unlistable, SWEEP_BUDGET).await;
    let observed = observed.unwrap();
    let gaps = [
        CensusGap::Missing { slot_id: 1 },
        CensusGap::Missing { slot_id: 2 },
        CensusGap::Unread { slot_id: 5 },
        CensusGap::Unowned,
    ];
    let unreadable = SlotOutcome::Unknown(Unknown::TableUnreadable);
    assert_eq!(
        (observed.census.gaps, observed.slots),
        (gaps.to_vec(), slots(unreadable))
    );

    // Past the deadline, no eligible slot is read.
    let observed = observe(&pool, &store, host.clone(), &nft, Duration::ZERO).await;
    let observed = observed.unwrap();
    let unconfirmed = SlotOutcome::Unknown(Unknown::Unconfirmed);
    let unreached = [
        slot(0, true, None, unconfirmed),
        slot(1, true, None, unconfirmed),
        slot(2, false, None, unconfirmed),
        slot(3, true, None, ineligible),
        slot(4, true, None, ineligible),
        slot(5, true, None, unconfirmed),
    ];
    let mut gaps: Vec<_> = [0, 1, 2, 5]
        .map(|slot_id| CensusGap::Unread { slot_id })
        .into();
    gaps.push(CensusGap::Extra {
        entry: host.link(9),
    });
    assert_eq!(
        (observed.census.gaps, observed.slots),
        (gaps, unreached.to_vec())
    );

    poison(&pool);
    let observed = observe(&pool, &store, host.clone(), &nft, SWEEP_BUDGET).await;
    let observed = observed.unwrap();
    let failed = (vec![CensusGap::InventoryFailed], Vec::new());
    assert_eq!((observed.census.gaps, observed.slots), failed);
    assert_eq!(host.installs(), []);
}

// Catches a bound overrun, a missing marker or a wrong count, or a different
// or reordered prefix kept.
#[test]
fn gaps_past_the_bound_keep_a_prefix_and_end_in_a_count() {
    let unread = |slot_id| CensusGap::Unread { slot_id };
    for (pushed, omitted) in [(2, None), (3, Some(1)), (7, Some(5))] {
        let mut gaps = Gaps::new(3);
        for slot_id in 0..pushed {
            gaps.push(unread(slot_id));
        }
        let mut expected = vec![unread(0), unread(1)];
        expected.extend(omitted.map(|omitted| CensusGap::Truncated { omitted }));
        assert_eq!(gaps.finish(), expected, "{pushed} gaps");
    }
}

// Catches a reading kept or counted after its namespace or slot changed, a
// slot gap whose slot gives another reason, a census judged before the last
// read, or a superseded generation's readings returned.
#[tokio::test]
async fn changes_before_finalizing_leave_no_reading_standing() {
    use SlotState::{Dirty, Leased, Ready};
    let (pool, _jobs) = pool(4);
    let (_dir, store) = store(Some(12));
    let host = FakeHost::new(None);
    for (slot_id, state) in (0..).zip([Leased, Leased, Dirty, Leased]) {
        set_slot(&pool, slot_id, state, 1);
    }
    let namespaces = [(0, "net"), (1, "uts"), (2, "ipc"), (3, "cgroup")]
        .map(|(slot_id, kind)| host.name(slot_id, kind));
    for (namespace, generation) in namespaces.iter().zip([12, 11, 12, 12]) {
        host.tables
            .lock()
            .unwrap()
            .insert(*namespace, installed(generation));
    }
    let (slots, fake) = (pool.clone(), host.clone());
    hook(&host.observed, move || {
        fake.name(0, "mnt");
        set_slot(&slots, 1, Leased, 2);
        // Reset after the read.
        set_slot(&slots, 2, Ready, 1);
        std::fs::remove_file(fake.link(3)).unwrap();
    });
    let nft = fake_nft(&host);
    let observed = observe(&pool, &store, host.clone(), &nft, SWEEP_BUDGET).await;
    let observed = observed.unwrap();
    let unconfirmed = SlotOutcome::Unknown(Unknown::Unconfirmed);
    let outcomes: Vec<_> = observed
        .slots
        .iter()
        .map(|slot| (slot.outcome, slot.target.slot_generation))
        .collect();
    let missing = SlotOutcome::Unknown(Unknown::NamespaceMissing);
    // Each slot gap's slot at its final lifecycle.
    let final_lifecycles = [
        (unconfirmed, 1),
        (unconfirmed, 2),
        (unconfirmed, 1),
        (missing, 1),
    ];
    assert_eq!(outcomes, final_lifecycles);
    let gaps = [
        CensusGap::Unread { slot_id: 0 },
        CensusGap::Unread { slot_id: 1 },
        CensusGap::Unread { slot_id: 2 },
        CensusGap::Missing { slot_id: 3 },
    ];
    assert_eq!(observed.census.gaps, gaps);

    let later = store.clone();
    hook(&host.observed, move || accept(&later, 13));
    let superseded = observe(&pool, &store, host.clone(), &nft, SWEEP_BUDGET).await;
    assert!(
        matches!(
            superseded,
            Err(Refusal::Assignment(NotRunnable::NotCurrent {
                accepted: 13
            }))
        ),
        "{superseded:?}"
    );
    assert_eq!(host.installs(), []);
}

// Catches an unread slot that names no slot entry, or one at a lifecycle
// other than its final one or with another reason, when only the final
// inventory lists it.
#[tokio::test]
async fn a_slot_only_the_final_inventory_lists_is_unknown_with_its_gap() {
    use SlotState::{Dirty, Leased, Ready};
    let (pool, _jobs) = pool(3);
    let (_dir, store) = store(Some(12));
    let host = FakeHost::new(None);
    for ((slot_id, state), generation) in (0..).zip([Leased, Dirty, Ready]).zip([3, 1, 4]) {
        set_slot(&pool, slot_id, state, generation);
        host.name(slot_id, ["net", "uts", "ipc"][slot_id as usize]);
    }
    // The first inventory read fails, and the final one succeeds.
    poison(&pool);
    let healed = pool.clone();
    hook(&host.observed, move || heal(&healed));
    let nft = fake_nft(&host);
    let observed = observe(&pool, &store, host.clone(), &nft, SWEEP_BUDGET).await;
    let observed = observed.unwrap();
    let unread = |slot_id, slot_generation, sandbox_id: Option<&str>| SlotEvidence {
        slot_id,
        target: SlotTarget {
            slot_generation,
            sandbox_id: sandbox_id.map(str::to_owned),
            namespace: None,
        },
        outcome: SlotOutcome::Unknown(Unknown::Unconfirmed),
    };
    let gaps = [
        CensusGap::Unread { slot_id: 0 },
        CensusGap::Unread { slot_id: 2 },
    ];
    assert_eq!(observed.census.gaps, gaps);
    assert_eq!(
        observed.slots,
        [unread(0, 3, Some("sandbox-0")), unread(2, 4, None)]
    );
}

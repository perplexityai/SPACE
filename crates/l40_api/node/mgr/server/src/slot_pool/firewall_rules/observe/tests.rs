//! Observation against the slot apply's fake nft, whose namespace links sit in
//! the fake host's own declared directory.

use rusqlite::params;

use super::*;
use crate::slot_pool::firewall_rules::NamespaceIdentity;
use crate::slot_pool::firewall_rules::tests::{
    FakeHost, NODE, accept, bounded, busy, hook, installed, poison, pool, set_slot, store,
};
use crate::slot_pool::{SlotInfo, SlotState};

fn fake_nft(host: &Arc<FakeHost>) -> Arc<Nft<'static>> {
    let fake = host.clone();
    Arc::new(move |netns: &File, args: &[&str], stdin: &[u8], _| {
        fake.reply(netns, args, stdin)
            .ok_or_else(|| io::ErrorKind::TimedOut.into())
    })
}

async fn observe(
    pool: &Arc<NetworkPool>,
    store: &Arc<FirewallAssignments>,
    host: Arc<dyn SlotHost>,
    nft: &Arc<Nft<'static>>,
) -> Result<FirewallObservation, Refusal> {
    let observed =
        pool.observe_firewall_rules_with(store, NODE, 12, host, nft.clone(), |_, _| false);
    bounded(observed).await
}

/// A slot no sandbox holds, at generation 1.
fn idle(pool: &NetworkPool, slot_id: u32, state: SlotState, health: &str) {
    let update = "UPDATE network_slots SET state = ?1, health = ?2, generation = 1,
                  sandbox_id = NULL WHERE slot_id = ?3";
    let params = params![state.as_str(), health, slot_id];
    pool.db.lock().unwrap().execute(update, params).unwrap();
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

// Catches a census that ignores a missing or extra managed namespace, a failed
// inventory or an undeclared directory, a record left out, an unreadable or
// busy slot misreported, or nft written.
#[tokio::test]
async fn the_census_accounts_for_every_record_and_managed_namespace() {
    use SlotState::{Dirty, Leased, Missing, Ready};
    let (pool, _jobs) = pool(6);
    let (_dir, store) = store(Some(12));
    let host = FakeHost::new(None);
    for (slot_id, state) in (0..).zip([Leased, Leased, Ready, Dirty]) {
        set_slot(&pool, slot_id, state, 1);
    }
    idle(&pool, 4, Missing, "degraded");
    idle(&pool, 5, Missing, "unknown");
    let verified = host.name(0, "net");
    host.tables.lock().unwrap().insert(verified, installed(12));
    // Slot 1 has no namespace; slot 2's entry is not one.
    std::fs::write(host.link(2), b"").unwrap();
    // A transitional or setup-interrupted slot keeps its namespace.
    host.name(3, "uts");
    host.name(4, "ipc");
    host.name(9, "cgroup");
    let _busy = busy(&pool, 0);
    let nft = fake_nft(&host);

    let ineligible = SlotOutcome::Unknown(Unknown::Ineligible);
    let slots = vec![
        slot(0, true, Some(verified), SlotOutcome::Verified),
        slot(
            1,
            true,
            None,
            SlotOutcome::Unknown(Unknown::NamespaceMissing),
        ),
        slot(
            2,
            false,
            None,
            SlotOutcome::Unknown(Unknown::NamespaceUnreadable),
        ),
        slot(3, true, None, ineligible),
        slot(4, false, None, ineligible),
        slot(5, false, None, ineligible),
    ];
    let observed = observe(&pool, &store, host.clone(), &nft).await.unwrap();
    let expected = FirewallObservation {
        node_id: NODE,
        generation: 12,
        revision_id: Uuid::from_u128(12),
        census: Census {
            gaps: vec![
                CensusGap::Missing { slot_id: 1 },
                CensusGap::Extra {
                    entry: host.link(9),
                },
            ],
        },
        slots: slots.clone(),
    };
    assert_eq!(observed, expected);

    let undeclared = Arc::new(Undeclared(host.clone()));
    let observed = observe(&pool, &store, undeclared, &nft).await.unwrap();
    let gaps = [CensusGap::Missing { slot_id: 1 }, CensusGap::Unowned];
    assert_eq!(
        (observed.census.gaps, observed.slots),
        (gaps.to_vec(), slots)
    );

    poison(&pool);
    let observed = observe(&pool, &store, host.clone(), &nft).await.unwrap();
    let failed = (vec![CensusGap::InventoryFailed], Vec::new());
    assert_eq!((observed.census.gaps, observed.slots), failed);
    assert_eq!(host.installs(), []);
}

// Catches a reading kept after its namespace or slot changed, a census judged
// before the last read, or a superseded generation's readings returned.
#[tokio::test]
async fn changes_before_finalizing_leave_no_reading_standing() {
    use SlotState::{Leased, Ready};
    let (pool, _jobs) = pool(4);
    let (_dir, store) = store(Some(12));
    let host = FakeHost::new(None);
    for slot_id in [0, 1, 3] {
        set_slot(&pool, slot_id, Leased, 1);
    }
    idle(&pool, 2, Ready, "unknown");
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
        // Adopted after the read, as startup reconciliation does.
        idle(&slots, 2, Ready, "healthy");
        std::fs::remove_file(fake.link(3)).unwrap();
    });
    let nft = fake_nft(&host);
    let observed = observe(&pool, &store, host.clone(), &nft).await.unwrap();
    let unconfirmed = SlotOutcome::Unknown(Unknown::Unconfirmed);
    let outcomes: Vec<_> = observed.slots.iter().map(|slot| slot.outcome).collect();
    let ineligible = SlotOutcome::Unknown(Unknown::Ineligible);
    assert_eq!(
        outcomes,
        [unconfirmed, unconfirmed, ineligible, unconfirmed]
    );
    let gaps = [
        CensusGap::Unread { slot_id: 1 },
        CensusGap::Unread { slot_id: 2 },
        CensusGap::Missing { slot_id: 3 },
    ];
    assert_eq!(observed.census.gaps, gaps);

    let later = store.clone();
    hook(&host.observed, move || accept(&later, 13));
    let superseded = observe(&pool, &store, host.clone(), &nft).await;
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

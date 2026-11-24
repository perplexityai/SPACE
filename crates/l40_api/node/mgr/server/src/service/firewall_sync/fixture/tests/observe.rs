//! Verification rounds through the binding, against the slot apply's fake nft.

use super::super::rounds::{Observed, RoundRefused};
use super::*;
use crate::slot_pool::SlotOutcome;
use crate::slot_pool::firewall_rules::observe::{Failure, Refusal};

fn outcomes(observed: &Observed) -> Vec<SlotOutcome> {
    let observation = observed.answer.as_ref().unwrap();
    observation.slots.iter().map(|slot| slot.outcome).collect()
}

// Catches a round swept twice, a final round answered afresh, a new round
// served from the cache, a stale or conflicting round answered, other than
// the last eight rounds kept, or a round evicted while queued still swept.
#[tokio::test]
async fn a_round_is_observed_once_and_never_recreated() {
    use SlotState::{Leased, Ready};
    let bound = bind_slots(BoundSlots::All, None, None, WAIT, &[Leased, Ready]).await;
    sync_all(&bound.service, 12).await;
    passed(&bound.service, 12).await.unwrap();
    let (sync, pool) = (
        bound.service.firewall_sync.clone().unwrap(),
        &bound.service.pool,
    );
    let calls = || bound.host.calls().len();
    let before = calls();

    let (entered, release) = gate(&bound.host.observed);
    let first = sync.observe(pool, NODE, 12, 7).unwrap();
    bounded(entered).await.unwrap();
    let joined = sync.observe(pool, NODE, 12, 7).unwrap();
    release.send(()).unwrap();
    let (round_7, joined) = (bounded(first).await, bounded(joined).await);
    assert!(Arc::ptr_eq(&round_7, &joined));
    let observation = round_7.answer.as_ref().unwrap();
    assert!(observation.census.complete(), "{observation:?}");
    assert_eq!(outcomes(&round_7), [SlotOutcome::Verified; 2]);
    // Two listings for each slot.
    let swept = calls();
    assert_eq!(swept - before, 4);

    let idle = bound.namespaces[1];
    bound
        .host
        .tables
        .lock()
        .unwrap()
        .insert(idle, installed(11));
    let retried = bounded(sync.observe(pool, NODE, 12, 7).unwrap()).await;
    assert!(Arc::ptr_eq(&round_7, &retried));
    assert_eq!(calls(), swept);
    let refused = |generation, round| sync.observe(pool, NODE, generation, round).err();
    assert_eq!(
        refused(13, 7),
        Some(RoundRefused::Conflict { generation: 12 })
    );
    let other = NodeId::from_uuid(Uuid::from_u128(2));
    assert_eq!(
        sync.observe(pool, other, 12, 9).err(),
        Some(RoundRefused::WrongNode)
    );

    let round_8 = bounded(sync.observe(pool, NODE, 12, 8).unwrap()).await;
    let mismatch = SlotOutcome::Failed(Failure::RulesMismatch);
    assert_eq!(outcomes(&round_8), [SlotOutcome::Verified, mismatch]);
    assert_eq!(round_8.round, 8);
    // Never seen, but older than the newest.
    assert_eq!(refused(12, 6), Some(RoundRefused::Stale));
    let through_8 = calls();

    // 10 queues behind 9's sweep, and 11 to 18 evict both before it runs.
    let (entered, release) = gate(&bound.host.observed);
    let round_9 = sync.observe(pool, NODE, 12, 9).unwrap();
    bounded(entered).await.unwrap();
    let queued: Vec<_> = (10..=18)
        .map(|round| sync.observe(pool, NODE, 12, round).unwrap())
        .collect();
    release.send(()).unwrap();
    bounded(round_9).await;
    let queued = bounded(futures_util::future::join_all(queued)).await;
    assert!(
        matches!(queued[0].answer, Err(Refusal::Unfinished)),
        "{:?}",
        queued[0]
    );
    // Every round from 9 to 18 but 10.
    let evicted = calls();
    assert_eq!(evicted - through_8, 9 * 4);
    assert_eq!(refused(12, 10), Some(RoundRefused::Stale));
    let kept = bounded(sync.observe(pool, NODE, 12, 11).unwrap()).await;
    assert!(Arc::ptr_eq(&kept, &queued[1]));
    assert_eq!(calls(), evicted);
    assert_eq!(bound.host.installs().len(), 2);
}

// Catches Applying claimed without pending apply work for the target or for a
// matching table, or a mismatch the pass will still apply reported as Failed.
#[tokio::test]
async fn only_pending_apply_work_is_applying() {
    use SlotState::Leased;
    let bound = bind_slots(BoundSlots::All, None, None, WAIT, &[Leased, Leased]).await;
    let (sync, pool) = (
        bound.service.firewall_sync.clone().unwrap(),
        &bound.service.pool,
    );
    let target = |slot_generation| SlotTarget {
        slot_generation,
        sandbox_id: Some("sandbox-0".into()),
        namespace: None,
    };
    // Armed and not yet listed: the worker hasn't run on this runtime.
    sync.wanted.send_modify(|wanted| {
        arm(wanted, 12);
    });
    assert!(sync.applying(12, 0, &target(1)));
    assert!(!sync.applying(11, 0, &target(1)));
    sync.wanted.send_replace(None);

    // 12 pauses on slot 0's lookup, its pass listed and nothing applied.
    let (entered, release) = gate(&bound.host.looked_up);
    sync_all(&bound.service, 12).await;
    bounded(entered).await.unwrap();
    assert!(!sync.applying(12, 0, &target(2)), "another occupant");
    // Slot 1 already matches: pending, but verified.
    let matching = bound.namespaces[1];
    bound
        .host
        .tables
        .lock()
        .unwrap()
        .insert(matching, installed(12));
    let pending = bounded(sync.observe(pool, NODE, 12, 1).unwrap()).await;
    assert_eq!(
        outcomes(&pending),
        [SlotOutcome::Applying, SlotOutcome::Verified]
    );
    release.send(()).unwrap();
}

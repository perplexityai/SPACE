//! A neutral `SlotGuard` and the armed `ResetClaim` wrapper share one
//! per-slot registry entry. Only the wrapper may wake the reset scheduler,
//! and only after its entry is gone.

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Neutral,
    Reset,
}

impl Kind {
    const ALL: [Self; 2] = [Self::Neutral, Self::Reset];

    fn acquire(self, pool: &std::sync::Arc<NetworkPool>, slot_id: u32) -> Option<Owner> {
        match self {
            Self::Neutral => SlotGuard::try_acquire(pool, slot_id).map(Owner::Neutral),
            Self::Reset => ResetClaim::acquire(pool, slot_id).map(Owner::Reset),
        }
    }

    /// Wakeups an armed final release of this kind produces.
    fn release_wakeups(self) -> usize {
        match self {
            Self::Neutral => 0,
            Self::Reset => 1,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code, reason = "held only for its drop")]
enum Owner {
    Neutral(SlotGuard),
    Reset(std::sync::Arc<ResetClaim>),
}

/// The channel is wider than production's so each finalization is countable.
fn pool_with_wakeups(slots: u32) -> (std::sync::Arc<NetworkPool>, mpsc::Receiver<()>) {
    let mut pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(slots));
    let (sender, wakeups) = mpsc::channel(8);
    std::sync::Arc::get_mut(&mut pool).unwrap().job_tx = sender;
    (pool, wakeups)
}

fn wakeups(receiver: &mut mpsc::Receiver<()>) -> usize {
    std::iter::from_fn(|| receiver.try_recv().ok()).count()
}

fn held(pool: &NetworkPool, slot_id: u32) -> bool {
    pool.active_resets.lock().unwrap().contains(&slot_id)
}

fn assert_held(pool: &std::sync::Arc<NetworkPool>, slot_id: u32, context: &str) {
    assert!(held(pool, slot_id), "{context}");
    for kind in Kind::ALL {
        assert!(kind.acquire(pool, slot_id).is_none(), "{context}: {kind:?}");
    }
    // The failed attempts above did not unregister the owner.
    assert!(held(pool, slot_id), "{context}");
}

/// Reacquires through a neutral guard, which itself never wakes anything.
fn assert_free(pool: &std::sync::Arc<NetworkPool>, slot_id: u32, context: &str) {
    assert!(!held(pool, slot_id), "{context}");
    drop(SlotGuard::try_acquire(pool, slot_id).expect(context));
    assert!(!held(pool, slot_id), "{context}");
}

#[test]
fn neutral_and_reset_owners_exclude_each_other_on_one_slot() {
    for first in Kind::ALL {
        for second in Kind::ALL {
            let context = format!("{first:?} then {second:?}");
            let (pool, mut jobs) = pool_with_wakeups(1);
            let owner = first.acquire(&pool, 0).expect(&context);
            assert!(second.acquire(&pool, 0).is_none(), "{context}");
            assert_held(&pool, 0, &context);
            assert_eq!(
                wakeups(&mut jobs),
                0,
                "{context}: failed attempts nudge nothing"
            );
            drop(owner);
            assert_free(&pool, 0, &context);
            assert_eq!(wakeups(&mut jobs), first.release_wakeups(), "{context}");
        }
    }
}

#[test]
fn owners_of_different_slots_are_independent() {
    for first in Kind::ALL {
        for second in Kind::ALL {
            for release_first in [true, false] {
                let context =
                    format!("{first:?} on 0, {second:?} on 1, release first {release_first}");
                let (pool, _jobs) = pool_with_wakeups(2);
                let zero = first.acquire(&pool, 0).expect(&context);
                let one = second.acquire(&pool, 1).expect(&context);
                let (released, kept) = if release_first { (0, 1) } else { (1, 0) };
                let kept_owner = if release_first {
                    drop(zero);
                    one
                } else {
                    drop(one);
                    zero
                };
                assert_free(&pool, released, &context);
                assert_held(&pool, kept, &context);
                drop(kept_owner);
                assert_free(&pool, kept, &context);
            }
        }
    }
}

#[test]
fn neutral_release_frees_the_slot_without_a_wakeup() {
    let (pool, mut jobs) = pool_with_wakeups(1);
    pool.nudge_topup();
    assert_eq!(wakeups(&mut jobs), 1, "prior wakeups drained");
    let guard = SlotGuard::try_acquire(&pool, 0).unwrap();
    assert_held(&pool, 0, "neutral guard held");
    drop(guard);
    assert_free(&pool, 0, "neutral guard released");
    assert_eq!(
        wakeups(&mut jobs),
        0,
        "neutral release must not wake the scheduler"
    );
}

#[test]
fn armed_reset_release_frees_the_slot_then_wakes_the_scheduler() {
    let (pool, mut jobs) = pool_with_wakeups(1);
    let claim = ResetClaim::acquire(&pool, 0).unwrap();
    assert_held(&pool, 0, "armed claim held");
    assert_eq!(wakeups(&mut jobs), 0, "holding does not wake");
    drop(claim);
    assert_free(&pool, 0, "armed claim released");
    assert_eq!(wakeups(&mut jobs), 1);
}

#[test]
fn final_release_removes_the_entry_before_notifying() {
    for armed in [true, false] {
        let (pool, mut jobs) = pool_with_wakeups(1);
        let guard = SlotGuard::try_acquire(&pool, 0).unwrap();
        let mut notified = false;
        release_then_notify(guard, armed, |pool, slot_id| {
            assert_eq!(slot_id, 0);
            assert!(
                !held(&pool, slot_id),
                "notified while the slot was still claimed"
            );
            notified = true;
        });
        assert_eq!(notified, armed);
        assert_free(&pool, 0, "released");
        assert_eq!(
            wakeups(&mut jobs),
            0,
            "the injected notifier replaced the nudge"
        );
    }
}

#[test]
fn disarming_never_releases_early_and_suppresses_only_this_retry() {
    let (pool, mut jobs) = pool_with_wakeups(1);
    let claim = ResetClaim::acquire(&pool, 0).unwrap();
    claim.disarm();
    assert_held(&pool, 0, "disarmed once");
    claim.disarm();
    assert_held(&pool, 0, "disarmed twice");
    assert_eq!(wakeups(&mut jobs), 0);
    drop(claim);
    assert_free(&pool, 0, "disarmed claim released");
    assert_eq!(wakeups(&mut jobs), 0, "disarmed release must not wake");
}

#[tokio::test]
async fn releasing_an_owner_leaves_a_colliding_resets_queued_wakeup() {
    // Shows only that the release neither erases nor consumes the wakeup the
    // collision already queued. No worker drains it here, so this does not show
    // a prompt reset: the scheduler would skip the still-held slot, and a
    // neutral release adds no wakeup of its own (see `SlotGuard`).
    for kind in Kind::ALL {
        let (pool, mut jobs) = pool_with_wakeups(1);
        // An ephemeral row reaches host networking only through the teardown.
        pool.db
            .lock()
            .unwrap()
            .execute_batch(
                "UPDATE network_slots
                 SET state = 'dirty', generation = 7, lease_kind = 'ephemeral'",
            )
            .unwrap();
        let owner = kind.acquire(&pool, 0).unwrap();
        // A colliding reset queues its wakeup while the owner still holds the slot.
        let (teardown, teardowns) = refusing_teardown!();
        reset_slot_with(pool.clone(), 0, teardown).await;
        assert_eq!(teardowns.load(Ordering::SeqCst), 0, "{kind:?}");
        assert_eq!(record(&pool, 0).state, SlotState::Dirty, "{kind:?}");
        if let Owner::Reset(claim) = &owner {
            claim.disarm();
            claim.disarm();
        }
        assert_held(&pool, 0, &format!("{kind:?} after the collision"));
        drop(owner);
        assert_free(&pool, 0, &format!("{kind:?} released"));
        // The channel does not coalesce here: exactly the collision's wakeup
        // survives, and the release added none.
        assert_eq!(wakeups(&mut jobs), 1, "{kind:?}");
        assert_eq!(
            reset_candidates(&pool, u32::MAX).unwrap().entries,
            vec![(0, 7)],
            "{kind:?}: the durable work is still discoverable"
        );
    }
}

#[test]
fn reset_claim_clones_hold_the_slot_until_the_last_owner_finalizes_once() {
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for order in orders {
        for disarm_through in [None, Some(0), Some(1), Some(2)] {
            let context = format!("drop order {order:?}, disarm through {disarm_through:?}");
            let (pool, mut jobs) = pool_with_wakeups(1);
            let claim = ResetClaim::acquire(&pool, 0).unwrap();
            let mut clones: Vec<_> = vec![Some(claim.clone()), Some(claim.clone()), Some(claim)];
            if let Some(index) = disarm_through {
                clones[index].as_ref().unwrap().disarm();
            }
            for (dropped, index) in order.into_iter().enumerate() {
                assert_held(&pool, 0, &context);
                drop(clones[index].take().unwrap());
                if dropped < 2 {
                    assert_eq!(
                        wakeups(&mut jobs),
                        0,
                        "{context}: only the last owner finalizes"
                    );
                }
            }
            assert_free(&pool, 0, &context);
            assert_eq!(
                wakeups(&mut jobs),
                usize::from(disarm_through.is_none()),
                "{context}"
            );
        }
    }
}

#[test]
fn early_return_and_unwind_release_with_each_kinds_retry_behavior() {
    fn return_early(pool: &std::sync::Arc<NetworkPool>, kind: Kind) -> Result<(), ()> {
        let _owner = kind.acquire(pool, 0).ok_or(())?;
        Err(())
    }

    for kind in Kind::ALL {
        let (pool, mut jobs) = pool_with_wakeups(1);
        assert!(return_early(&pool, kind).is_err());
        assert_free(&pool, 0, &format!("{kind:?} early return"));
        assert_eq!(
            wakeups(&mut jobs),
            kind.release_wakeups(),
            "{kind:?} early return"
        );

        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _owner = kind.acquire(&pool, 0).unwrap();
            panic!("injected unwind through an owned {kind:?} guard");
        }));
        assert!(unwound.is_err());
        assert_free(&pool, 0, &format!("{kind:?} unwind"));
        assert_eq!(
            wakeups(&mut jobs),
            kind.release_wakeups(),
            "{kind:?} unwind"
        );
    }
}

#[test]
fn guards_still_exclude_and_release_through_a_poisoned_registry() {
    // Covers the guards only. The scheduler's discovery scans return
    // `LockPoisoned`, so discovery stops after poisoning; that predates `SlotGuard`.
    for kind in Kind::ALL {
        let (pool, mut jobs) = pool_with_wakeups(1);
        let poisoner = pool.clone();
        std::thread::spawn(move || {
            let _active = poisoner.active_resets.lock().unwrap();
            panic!("injected registry poison");
        })
        .join()
        .unwrap_err();
        assert!(pool.active_resets.is_poisoned());
        let owner = kind.acquire(&pool, 0).unwrap();
        for other in Kind::ALL {
            assert!(other.acquire(&pool, 0).is_none(), "{kind:?} then {other:?}");
        }
        drop(owner);
        assert!(
            SlotGuard::try_acquire(&pool, 0).is_some(),
            "{kind:?} released through poison"
        );
        assert_eq!(wakeups(&mut jobs), kind.release_wakeups(), "{kind:?}");
    }
}

fn record(pool: &NetworkPool, slot_id: u32) -> SlotRecord {
    query_record_by_slot(&pool.db.lock().unwrap(), slot_id)
        .unwrap()
        .unwrap()
}

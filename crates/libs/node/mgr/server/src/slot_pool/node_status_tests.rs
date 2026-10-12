//! Node-ctl republishes node status when the pool marks it dirty, so every
//! change to leases or pool configuration must mark it, and a refused change,
//! which leaves status as it was, must not. These run on a Tokio runtime, where
//! the production sink would dial node-ctl; each pool records its marks instead.

use std::sync::Arc;

use super::*;
use crate::node_state_sync::tests::RecordingNodeStatusDirtySink;

#[derive(Clone, Copy, Debug)]
enum Change {
    Allocate,
    AllocateWhileDraining,
    AllocateWithoutReadySlot,
    Release,
    ReleaseWithoutLease,
    UpdateConfig,
    UnpublishableConfigUpdate,
}

const SANDBOX: &str = "019d84bd-280d-7331-99c6-b63377f3a3a2";

fn reservation() -> Option<NewLeaseMemoryReservation> {
    Some(NewLeaseMemoryReservation {
        memory_reservation_mib: 1024,
    })
}

#[rstest::rstest]
#[case::allocation(Change::Allocate, true)]
#[case::allocation_while_draining(Change::AllocateWhileDraining, false)]
#[case::allocation_without_ready_slot(Change::AllocateWithoutReadySlot, false)]
#[case::release(Change::Release, true)]
#[case::release_without_lease(Change::ReleaseWithoutLease, false)]
#[case::config_update(Change::UpdateConfig, true)]
#[case::unpublishable_config_update(Change::UnpublishableConfigUpdate, false)]
#[tokio::test]
async fn node_status_changes_mark_node_ctl_dirty(#[case] change: Change, #[case] applied: bool) {
    let marks = Arc::new(RecordingNodeStatusDirtySink::default());
    let pool = NetworkPool::open_in_memory_with_node_status_sink_for_test(
        NetworkPoolConfig::new(1),
        marks.clone(),
    );
    // Without a ready slot, a pooled allocation reaches the store and finds
    // nothing to lease.
    if !matches!(change, Change::AllocateWithoutReadySlot) {
        pool.set_all_slots_ready_for_test();
    }

    match change {
        Change::Allocate | Change::AllocateWhileDraining | Change::AllocateWithoutReadySlot => {
            if matches!(change, Change::AllocateWhileDraining) {
                pool.set_draining(true).unwrap();
            }
            marks.take();
            let allocated = pool.allocate(SANDBOX, None, reservation()).await;
            match change {
                Change::Allocate => assert!(allocated.is_ok(), "{allocated:?}"),
                _ => assert!(
                    matches!(allocated, Err(AllocateSlotError::ResourceExhausted(_))),
                    "{allocated:?}"
                ),
            }
            assert_eq!(pool.get(SANDBOX).unwrap().is_some(), applied);
        }
        Change::Release | Change::ReleaseWithoutLease => {
            if applied {
                pool.allocate(SANDBOX, None, reservation()).await.unwrap();
            }
            marks.take();
            let released = pool.release(SANDBOX).unwrap();
            assert_eq!(released.is_some(), applied, "{released:?}");
            assert!(pool.get(SANDBOX).unwrap().is_none());
            let expected = if applied {
                SlotState::Dirty
            } else {
                SlotState::Ready
            };
            assert_eq!(pool.all_records().unwrap()[0].state, expected);
        }
        Change::UpdateConfig | Change::UnpublishableConfigUpdate => {
            marks.take();
            // A ready-slot target above `max_slots` makes the update
            // unpublishable.
            let update = NetworkPoolConfigUpdate {
                max_slots: Some(2),
                target_ready_slots: (!applied).then_some(99),
                ..Default::default()
            };
            let updated = pool.update_config(update);
            assert_eq!(updated.is_ok(), applied, "{updated:?}");
            assert_eq!(pool.config().max_slots, if applied { 2 } else { 1 });
        }
    }

    let recorded = marks.take();
    if applied {
        assert!(
            recorded > 0,
            "{change:?} changed node status without marking it"
        );
    } else {
        assert_eq!(recorded, 0, "{change:?} was refused but marked node status");
    }
}

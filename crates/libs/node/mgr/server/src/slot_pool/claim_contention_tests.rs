//! Startup recovery and slot reset share the per-sandbox mutation claim with
//! node-manager RPCs. While any RPC holds that claim, recovery must defer
//! without mutating the lease and must keep its retry obligation.

use super::*;

/// The two claim kinds an RPC can hold: shared startup work (Prepare, Launch,
/// LoadSnapshot) and exclusive cleanup/reset ownership.
#[derive(Clone, Copy, Debug)]
enum Holder {
    StartupWork,
    CleanupOrReset,
}

impl Holder {
    const ALL: [Self; 2] = [Self::StartupWork, Self::CleanupOrReset];

    fn claim(self, pool: &NetworkPool, sandbox_id: &str) -> std::sync::Arc<SandboxMutationClaim> {
        match self {
            Self::StartupWork => pool.try_claim_sandbox_work(sandbox_id),
            Self::CleanupOrReset => pool.try_claim_sandbox_mutation(sandbox_id),
        }
        .expect("fixture claim on an unowned sandbox")
    }
}

/// Replace the discarded fill-worker channel so recovery wakeups are visible.
fn pool_with_wakeups(
    config: NetworkPoolConfig,
) -> (std::sync::Arc<NetworkPool>, mpsc::Receiver<()>) {
    let mut pool = NetworkPool::open_in_memory_for_test(config);
    let (sender, wakeups) = mpsc::channel(1);
    std::sync::Arc::get_mut(&mut pool).unwrap().job_tx = sender;
    (pool, wakeups)
}

fn slot(pool: &NetworkPool, slot_id: u32) -> SlotRecord {
    query_record_by_slot(&pool.db.lock().unwrap(), slot_id)
        .unwrap()
        .unwrap()
}

fn drain(wakeups: &mut mpsc::Receiver<()>) {
    while wakeups.try_recv().is_ok() {}
}

#[derive(Clone, Copy, Debug)]
enum StartupProbeResult {
    RuntimeGone,
    HealthDegraded,
}

#[test]
fn startup_lease_updates_defer_claimed_sandboxes_and_apply_once_released() {
    for holder in Holder::ALL {
        for result in [
            StartupProbeResult::RuntimeGone,
            StartupProbeResult::HealthDegraded,
        ] {
            let context = format!("{holder:?} / {result:?}");
            let (pool, mut wakeups) = pool_with_wakeups(NetworkPoolConfig::new(1));
            pool.set_all_slots_ready_for_test();
            let sandbox_id = SandboxId::new().to_string();
            pool.allocate_pooled_slot(
                &sandbox_id,
                None,
                Some(NewLeaseMemoryReservation {
                    memory_reservation_mib: 8320,
                }),
            )
            .unwrap();
            {
                let conn = pool.db.lock().unwrap();
                *pool.startup_leases.lock().unwrap() = query_startup_leases(&conn).unwrap();
            }
            let leased = slot(&pool, 0);
            let update = || match result {
                StartupProbeResult::RuntimeGone => StartupLeaseUpdate::Gone(leased.info.clone()),
                StartupProbeResult::HealthDegraded => {
                    StartupLeaseUpdate::Health(LeaseHealthValidationUpdate {
                        slot_id: leased.info.slot,
                        generation: leased.info.generation,
                        sandbox_id: sandbox_id.clone(),
                        health: SlotHealth::Degraded,
                        last_error: Some("synthetic startup probe failure".into()),
                    })
                }
            };
            drain(&mut wakeups);

            let claim = holder.claim(&pool, &sandbox_id);
            apply_startup_lease_updates(&pool, &[update()]).unwrap();
            let deferred = slot(&pool, 0);
            assert_eq!(deferred.state, SlotState::Leased, "{context}");
            assert_eq!(deferred.health, leased.health, "{context}");
            assert_eq!(deferred.last_error, leased.last_error, "{context}");
            assert_eq!(deferred.memory_reservation_mib, Some(8320), "{context}");
            assert!(wakeups.try_recv().is_err(), "{context}: no reset may queue");
            // The deferred pass is the only record that the probe must rerun.
            assert!(
                pool.lease_health_recovery_pending.load(Ordering::Relaxed),
                "{context}: a deferred startup update must request another pass"
            );
            assert!(
                pool.try_claim_sandbox_mutation(&sandbox_id).is_none(),
                "{context}: recovery must not disturb the RPC's claim"
            );
            drop(claim);

            apply_startup_lease_updates(&pool, &[update()]).unwrap();
            let applied = slot(&pool, 0);
            match result {
                StartupProbeResult::RuntimeGone => {
                    assert_eq!(applied.state, SlotState::Dirty, "{context}");
                    assert_eq!(applied.memory_reservation_mib, None, "{context}");
                    assert!(wakeups.try_recv().is_ok(), "{context}: reset must queue");
                }
                StartupProbeResult::HealthDegraded => {
                    assert_eq!(applied.state, SlotState::Leased, "{context}");
                    assert_eq!(applied.health, SlotHealth::Degraded, "{context}");
                    assert_eq!(
                        applied.last_error.as_deref(),
                        Some("synthetic startup probe failure"),
                        "{context}"
                    );
                }
            }
            drop(
                pool.try_claim_sandbox_mutation(&sandbox_id)
                    .expect("recovery must release its own claim"),
            );
        }
    }
}

#[test]
fn startup_leasing_reprobe_skips_claimed_sandboxes_and_keeps_the_obligation() {
    for holder in Holder::ALL {
        let pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(1));
        let sandbox_id = SandboxId::new().to_string();
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots SET state = 'leasing', sandbox_id = ?1,
                     generation = 7, memory_reservation_mib = 8320 WHERE slot_id = 0",
                params![sandbox_id],
            )
            .unwrap();
            *pool.startup_leasing.lock().unwrap() = query_startup_leasing(&conn).unwrap();
            *pool.startup_leases.lock().unwrap() = query_startup_leases(&conn).unwrap();
        }

        let claim = holder.claim(&pool, &sandbox_id);
        assert!(
            reprobe_startup_leasing_with(&pool, |_| {
                panic!("{holder:?}: a claimed sandbox must not be probed")
            })
            .unwrap(),
            "{holder:?}: a skipped lease remains pending"
        );
        let skipped = slot(&pool, 0);
        assert_eq!(skipped.state, SlotState::Leasing, "{holder:?}");
        assert_eq!(skipped.memory_reservation_mib, Some(8320), "{holder:?}");
        assert!(pool.try_claim_sandbox_mutation(&sandbox_id).is_none());
        drop(claim);

        assert!(!reprobe_startup_leasing_with(&pool, |_| LeaseLiveness::Live).unwrap());
        assert_eq!(slot(&pool, 0).state, SlotState::Leased, "{holder:?}");
        drop(pool.try_claim_sandbox_mutation(&sandbox_id).unwrap());
    }
}

/// Only the deferred exit is pinned here. An admitted reset reaches host
/// cleanup (cgroupfs, the fixed cache root and network teardown), so it stays
/// out of this unit layer.
#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn reset_slot_defers_a_claimed_sandbox_and_requeues_itself() {
    use std::os::unix::fs::MetadataExt;
    for holder in Holder::ALL {
        let sandbox_id = SandboxId::new().to_string();
        // Barrier for a regressed reset that ignores the claim: a sandbox
        // cache directory still owned by the lease UID fails the
        // retained-cache check before network teardown. The deferred path
        // never reaches it.
        let cache_root = tempfile::tempdir().unwrap();
        let sandbox_cache = cache_root.path().join(&sandbox_id);
        std::fs::create_dir(&sandbox_cache).unwrap();
        let lease_uid = std::fs::symlink_metadata(&sandbox_cache).unwrap().uid();
        let mut config = NetworkPoolConfig::new(1);
        config.sandbox_cache_dir = cache_root.path().to_path_buf();
        let (pool, mut wakeups) = pool_with_wakeups(config);
        pool.db
            .lock()
            .unwrap()
            .execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'dirty', health = 'unknown',
                     sandbox_id = ?1, sandbox_uid = ?2, pid = NULL, lease_kind = ?3
                 WHERE slot_id = 0",
                params![sandbox_id, lease_uid, LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
        drain(&mut wakeups);

        let claim = holder.claim(&pool, &sandbox_id);
        reset_slot(pool.clone(), 0).await;
        let deferred = slot(&pool, 0);
        assert_eq!(
            (
                deferred.state,
                deferred.info.generation,
                deferred.health,
                deferred.last_error.as_deref()
            ),
            (SlotState::Dirty, 1, SlotHealth::Unknown, None),
            "{holder:?}"
        );
        assert!(
            wakeups.try_recv().is_ok(),
            "{holder:?}: a deferred reset must schedule its own retry"
        );
        assert!(
            pool.try_claim_sandbox_mutation(&sandbox_id).is_none(),
            "{holder:?}: reset must not disturb the RPC's claim"
        );
        drop(claim);
        drop(
            pool.try_claim_sandbox_mutation(&sandbox_id)
                .expect("a deferred reset must not keep a sandbox claim"),
        );
    }
}

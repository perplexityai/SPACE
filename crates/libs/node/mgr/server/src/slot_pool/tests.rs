use super::db::*;
use super::reset::*;
use super::*;

#[cfg(test)]
mod slot_pool_tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn startup_reclamation_and_adoption_have_one_atomic_winner() {
        for adoption in [
            "none",
            "prepare-launch",
            "prepare-prefix",
            "allocate-pooled",
            "allocate-ephemeral",
            "pid",
            "pid-prefix",
            "pid-write-failure",
        ] {
            for recovery_first in [false, true] {
                let mut pool = test_pool(NetworkPoolConfig::new(1));
                let (sender, mut jobs) = mpsc::channel(1);
                std::sync::Arc::get_mut(&mut pool).unwrap().job_tx = sender;
                pool.set_all_slots_ready_for_test();
                let id = SandboxId::new().to_string();
                pool.allocate_pooled_slot(&id, None, memory_admission())
                    .unwrap();
                {
                    let conn = pool.db.lock().unwrap();
                    *pool.startup_leases.lock().unwrap() = query_startup_leases(&conn).unwrap();
                }
                let info = record(&pool, 0).info;
                if recovery_first {
                    apply_startup_lease_updates(&pool, &[StartupLeaseUpdate::Gone(info.clone())])
                        .unwrap();
                    assert!(pool.adopt_lease(&id, &info).unwrap().is_none());
                } else {
                    match adoption {
                        "prepare-launch" => {
                            pool.adopt_lease(&id, &info).unwrap().unwrap();
                        }
                        "prepare-prefix" => {
                            assert!(
                                pool.adopt_lease(id.get(..8).unwrap(), &info)
                                    .unwrap()
                                    .is_none()
                            );
                        }
                        "allocate-pooled" => {
                            pool.allocate_pooled_slot(&id, None, None).unwrap();
                        }
                        "allocate-ephemeral" => {
                            pool.claim_ephemeral_slot_for_setup(&id, None, None)
                                .unwrap();
                        }
                        "pid" => {
                            pool.set_pid(&id, std::process::id() + 1).unwrap();
                        }
                        "pid-prefix" => {
                            pool.set_pid(id.get(..8).unwrap(), std::process::id() + 1)
                                .unwrap_err();
                        }
                        "pid-write-failure" => {
                            pool.db.lock().unwrap().execute_batch(
                                "CREATE TRIGGER reject_pid BEFORE UPDATE OF pid ON network_slots
                                 BEGIN SELECT RAISE(ABORT, 'injected PID write failure'); END;",
                            ).unwrap();
                            pool.set_pid(&id, std::process::id() + 1).unwrap_err();
                            pool.db
                                .lock()
                                .unwrap()
                                .execute_batch("DROP TRIGGER reject_pid")
                                .unwrap();
                        }
                        _ => {}
                    }
                }
                // Complete a probe that started before adoption, including a
                // same-generation relaunch whose PID is not published yet.
                apply_startup_lease_updates(&pool, &[StartupLeaseUpdate::Gone(info)]).unwrap();
                let reclaimed = recovery_first
                    || matches!(
                        adoption,
                        "none" | "prepare-prefix" | "pid-prefix" | "pid-write-failure"
                    );
                let row = record(&pool, 0);
                assert_eq!(
                    row.state,
                    if reclaimed {
                        SlotState::Dirty
                    } else {
                        SlotState::Leased
                    }
                );
                assert_eq!(row.memory_reservation_mib.is_none(), reclaimed);
                if reclaimed {
                    assert!(matches!(jobs.try_recv(), Ok(())));
                }
                assert!(jobs.try_recv().is_err());
            }
        }
    }

    #[test]
    fn startup_leasing_pid_publication_retries_instead_of_losing_recovery() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        {
            let conn = pool.db.lock().unwrap();
            conn.execute_batch(
                "UPDATE network_slots SET state = 'leasing', sandbox_id = 'sandbox-a',
                generation = 7, memory_reservation_mib = 8320",
            )
            .unwrap();
            *pool.startup_leasing.lock().unwrap() = query_startup_leasing(&conn).unwrap();
            *pool.startup_leases.lock().unwrap() = query_startup_leases(&conn).unwrap();
        }
        assert!(matches!(
            pool.adopt_lease("sandbox-a", &record(&pool, 0).info),
            Err(SlotStoreError::LeaseNotReady(_))
        ));
        assert!(
            reprobe_startup_leasing_with(&pool, |_| {
                pool.set_pid("sandbox-a", std::process::id() + 1).unwrap();
                LeaseLiveness::Gone
            })
            .unwrap()
        );
        assert_eq!(record(&pool, 0).state, SlotState::Leasing);
        assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
        assert!(!reprobe_startup_leasing_with(&pool, |_| LeaseLiveness::Live).unwrap());
        assert_eq!(record(&pool, 0).state, SlotState::Leased);
    }

    #[test]
    fn unready_host_defers_live_lease_health_without_spending_retry_budget() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        pool.set_all_slots_ready_for_test();
        pool.allocate_pooled_slot(&SandboxId::new().to_string(), None, memory_admission())
            .unwrap();
        let info = record(&pool, 0).info;
        *pool.startup_leases.lock().unwrap() =
            query_startup_leases(&pool.db.lock().unwrap()).unwrap();
        for _ in 0..=MAX_SLOT_TRANSIENT_VALIDATION_RETRIES {
            let ValidationTaskResult::RuntimeUnknown(update) =
                validate_startup_runtime(info.clone(), false, LeaseLiveness::Live)
            else {
                panic!("host readiness must defer rather than validate the missing namespace");
            };
            apply_startup_lease_updates(&pool, &[update]).unwrap();
            let row = record(&pool, 0);
            assert_eq!(row.health, SlotHealth::Unknown);
            assert!(
                row.last_error
                    .unwrap()
                    .contains("host networking not ready")
            );
            assert_eq!(row.memory_reservation_mib, Some(8320));
            assert!(pool.validation_transient_failures.is_empty());
        }
    }

    #[tokio::test]
    async fn pool_shutdown_wakes_retries_and_stops_worker_without_starting_host_work() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        schedule_reset_retry(pool.clone(), 0);
        schedule_topup_retry(pool.clone());
        let (_sender, receiver) = mpsc::channel(1);
        spawn_worker(pool.clone(), receiver);
        assert!(!pool.task_tracker.is_empty());
        pool.shutdown_token.cancel();
        pool.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(1), pool.task_tracker.wait())
            .await
            .unwrap();
        assert!(claim_missing_for_create(&pool, false).is_none());
        assert!(claim_ready_for_destroy(&pool).unwrap().is_none());
        pool.claim_allocation(&SandboxId::new().to_string(), None, memory_admission())
            .expect_err("shutdown must reject allocation");
    }

    #[test]
    fn startup_leasing_reprobe_recovers_only_original_ownership() {
        for evidence in [
            LeaseLiveness::Live,
            LeaseLiveness::Gone,
            LeaseLiveness::Unknown,
        ] {
            for supersession in ["none", "generation", "release"] {
                let mut pool = test_pool(NetworkPoolConfig::new(2));
                let (sender, mut jobs) = mpsc::channel(1);
                std::sync::Arc::get_mut(&mut pool).unwrap().job_tx = sender;
                {
                    let conn = pool.db.lock().unwrap();
                    conn.execute_batch(
                        "UPDATE network_slots SET state = 'leasing', sandbox_id = 'sandbox-a',
                             generation = 7, memory_reservation_mib = 8320 WHERE slot_id = 0;",
                    )
                    .unwrap();
                    *pool.startup_leasing.lock().unwrap() = query_startup_leasing(&conn).unwrap();
                    *pool.startup_leases.lock().unwrap() = query_startup_leases(&conn).unwrap();
                    // A request admitted after startup is not recovery work.
                    conn.execute_batch(
                        "UPDATE network_slots SET state = 'leasing', sandbox_id = 'sandbox-new',
                             generation = 1, memory_reservation_mib = 4096 WHERE slot_id = 1;",
                    )
                    .unwrap();
                }
                assert!(reprobe_startup_leasing_with(&pool, |_| LeaseLiveness::Unknown).unwrap());
                assert_eq!(record(&pool, 0).state, SlotState::Leasing);
                assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
                assert!(jobs.try_recv().is_err());

                // A failed durable transition must keep its retry identity.
                if supersession == "none" && evidence != LeaseLiveness::Unknown {
                    pool.db
                        .lock()
                        .unwrap()
                        .execute_batch(
                            "CREATE TRIGGER reject_recovery BEFORE UPDATE ON network_slots
                         BEGIN SELECT RAISE(ABORT, 'injected recovery write failure'); END;",
                        )
                        .unwrap();
                    reprobe_startup_leasing_with(&pool, |_| evidence).unwrap_err();
                    assert_eq!(record(&pool, 0).state, SlotState::Leasing);
                    assert_eq!(pool.startup_leasing.lock().unwrap().len(), 1);
                    assert!(jobs.try_recv().is_err());
                    pool.db
                        .lock()
                        .unwrap()
                        .execute_batch("DROP TRIGGER reject_recovery")
                        .unwrap();
                }

                let retry = reprobe_startup_leasing_with(&pool, |info| {
                    assert_eq!(info.slot, 0);
                    // Supersede after the read/probe, before the guarded write.
                    let conn = pool.db.lock().unwrap();
                    match supersession {
                        "generation" => {
                            conn.execute_batch(
                                "UPDATE network_slots SET generation = 8 WHERE slot_id = 0",
                            )
                            .unwrap();
                        }
                        "release" => {
                            conn.execute_batch(
                                "UPDATE network_slots SET state = 'dirty' WHERE slot_id = 0",
                            )
                            .unwrap();
                        }
                        _ => {}
                    }
                    evidence
                })
                .unwrap();
                assert_eq!(retry, evidence == LeaseLiveness::Unknown);
                let recovered = record(&pool, 0);
                let expected = match (supersession, evidence) {
                    ("release", _) | ("none", LeaseLiveness::Gone) => SlotState::Dirty,
                    ("none", LeaseLiveness::Live) => SlotState::Leased,
                    _ => SlotState::Leasing,
                };
                assert_eq!(recovered.state, expected);
                let gone = supersession == "none" && evidence == LeaseLiveness::Gone;
                assert_eq!(
                    recovered.memory_reservation_mib,
                    if gone { None } else { Some(8320) }
                );
                if gone {
                    assert!(matches!(jobs.try_recv(), Ok(())));
                }
                assert!(jobs.try_recv().is_err());
                assert_eq!(record(&pool, 1).state, SlotState::Leasing);
                assert_eq!(record(&pool, 1).memory_reservation_mib, Some(4096));
                if retry {
                    // Release resolves uncertainty without probing a newer owner.
                    pool.db
                        .lock()
                        .unwrap()
                        .execute_batch("UPDATE network_slots SET state = 'dirty' WHERE slot_id = 0")
                        .unwrap();
                    assert!(
                        !reprobe_startup_leasing_with(&pool, |_| panic!("released lease probed"))
                            .unwrap()
                    );
                }
                assert!(pool.startup_leasing.lock().unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn unrelated_fill_wakeups_do_not_multiply_pending_retry_tasks() {
        let mut pool = test_pool(NetworkPoolConfig::new(1));
        let (sender, mut wake) = mpsc::channel(1);
        std::sync::Arc::get_mut(&mut pool).unwrap().job_tx = sender;
        pool.set_draining(true).unwrap();
        schedule_topup_retry(pool.clone());
        for _ in 0..5 {
            fill_ready_slots(pool.clone()).await;
            schedule_topup_retry(pool.clone());
        }
        assert_eq!(
            pool.task_tracker.len(),
            0,
            "retry requests must not create sleeper tasks"
        );
        for _ in 0..10_000 {
            pool.nudge_topup();
        }
        assert_eq!(wake.try_recv(), Ok(()));
        assert_eq!(wake.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        assert!(
            pool.maintenance_failed.load(Ordering::Relaxed),
            "ordinary refill hints must not erase failure backoff"
        );
        pool.shutdown_token.cancel();
        pool.task_tracker.close();
        tokio::time::timeout(Duration::from_secs(1), pool.task_tracker.wait())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn startup_health_recovery_does_not_reclaim_fresh_allocations() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        pool.set_all_slots_ready_for_test();
        let sandbox_id = SandboxId::new().to_string();
        pool.claim_allocation(&sandbox_id, None, memory_admission())
            .unwrap();
        assert_eq!(record(&pool, 0).state, SlotState::Leased);
        assert!(!validate_live_leased_slots(pool.clone()).await.unwrap());
        assert_eq!(record(&pool, 0).state, SlotState::Leased);
        assert_eq!(record(&pool, 0).health, SlotHealth::Healthy);
        assert!(record(&pool, 0).memory_reservation_mib.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn startup_health_recovery_retries_uncertainty_without_releasing_ownership() {
        for resolution in ["gone", "generation", "release"] {
            let mut pool = test_pool(NetworkPoolConfig::new(1));
            let (sender, mut jobs) = mpsc::channel(1);
            let (startup_leases, startup_leasing) = {
                let conn = pool.db.lock().unwrap();
                conn.execute(
                    "UPDATE network_slots SET state = 'leased', sandbox_id = ?1,
                         generation = 7, pid = 0, memory_reservation_mib = 8320 WHERE slot_id = 0",
                    [SandboxId::new().to_string()],
                )
                .unwrap();
                if resolution == "gone" {
                    conn.execute_batch("UPDATE network_slots SET state = 'leasing'")
                        .unwrap();
                }
                (
                    query_startup_leases(&conn).unwrap(),
                    query_startup_leasing(&conn).unwrap(),
                )
            };
            let mutable_pool = std::sync::Arc::get_mut(&mut pool).unwrap();
            mutable_pool.startup_leases = Mutex::new(startup_leases);
            *mutable_pool.startup_leasing.get_mut().unwrap() = startup_leasing;
            mutable_pool.job_tx = sender;
            assert!(!reprobe_startup_leasing_with(&pool, |_| LeaseLiveness::Live).unwrap());
            pool.lease_health_recovery_pending
                .store(true, Ordering::Relaxed);
            pool.set_draining(true).unwrap();
            // Runtime uncertainty must not exhaust the networking retry budget,
            // and drain must not suspend this recovery obligation.
            for _ in 0..=MAX_SLOT_TRANSIENT_VALIDATION_RETRIES {
                fill_ready_slots(pool.clone()).await;
                assert!(pool.lease_health_recovery_pending.load(Ordering::Relaxed));
                assert_eq!(record(&pool, 0).state, SlotState::Leased);
                assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
                assert!(pool.validation_transient_failures.is_empty());
                tokio::task::yield_now().await;
                tokio::time::advance(POOL_FILL_RETRY_DELAY).await;
                assert!(pool.maintenance_failed.swap(false, Ordering::Relaxed));
                assert!(jobs.try_recv().is_err());
            }
            {
                let conn = pool.db.lock().unwrap();
                match resolution {
                    "gone" => {
                        conn.execute_batch(
                            "UPDATE network_slots SET pid = NULL;
                            CREATE TRIGGER reject_recovery BEFORE UPDATE ON network_slots
                            BEGIN SELECT RAISE(ABORT, 'injected recovery write failure'); END;",
                        )
                        .unwrap();
                    }
                    "generation" => {
                        conn.execute_batch("UPDATE network_slots SET generation = 8, pid = NULL")
                            .unwrap();
                    }
                    "release" => {
                        conn.execute_batch("UPDATE network_slots SET state = 'dirty'")
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
            }
            if resolution == "gone" {
                validate_live_leased_slots(pool.clone()).await.unwrap_err();
                assert_eq!(record(&pool, 0).state, SlotState::Leased);
                assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
                assert!(jobs.try_recv().is_err());
                pool.db
                    .lock()
                    .unwrap()
                    .execute_batch("DROP TRIGGER reject_recovery")
                    .unwrap();
            }
            fill_ready_slots(pool.clone()).await;
            assert!(!pool.lease_health_recovery_pending.load(Ordering::Relaxed));
            if resolution == "gone" {
                assert_eq!(record(&pool, 0).state, SlotState::Dirty);
                assert_eq!(record(&pool, 0).health, SlotHealth::Unknown);
                assert_eq!(record(&pool, 0).memory_reservation_mib, None);
                assert!(matches!(jobs.try_recv(), Ok(())));
            } else {
                assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
                assert_eq!(record(&pool, 0).health, SlotHealth::Unknown);
                assert_eq!(
                    record(&pool, 0).state,
                    if resolution == "generation" {
                        SlotState::Leased
                    } else {
                        SlotState::Dirty
                    }
                );
            }
            assert!(jobs.try_recv().is_err());
            assert!(!validate_live_leased_slots(pool.clone()).await.unwrap());
            assert!(
                jobs.try_recv().is_err(),
                "completed recovery must not enqueue duplicate reclamation"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn startup_leasing_reprobe_retries_while_draining_and_stops_after_release() {
        let mut pool = test_pool(NetworkPoolConfig::new(1));
        let (sender, mut jobs) = mpsc::channel(1);
        std::sync::Arc::get_mut(&mut pool).unwrap().job_tx = sender;
        {
            let conn = pool.db.lock().unwrap();
            // Invalid tracked PID gives Unknown without relying on host processes.
            conn.execute(
                "UPDATE network_slots SET state = 'leasing', sandbox_id = ?1,
                     generation = 7, pid = 0, memory_reservation_mib = 8320 WHERE slot_id = 0",
                [SandboxId::new().to_string()],
            )
            .unwrap();
            *pool.startup_leasing.lock().unwrap() = query_startup_leasing(&conn).unwrap();
            *pool.startup_leases.lock().unwrap() = query_startup_leases(&conn).unwrap();
        }
        pool.set_draining(true).unwrap();
        fill_ready_slots(pool.clone()).await;
        assert_eq!(record(&pool, 0).state, SlotState::Leasing);
        assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
        assert!(pool.maintenance_failed.swap(false, Ordering::Relaxed));
        assert!(
            jobs.try_recv().is_err(),
            "periodic pump, not a sleeper, owns retry"
        );
        pool.db
            .lock()
            .unwrap()
            .execute_batch("UPDATE network_slots SET state = 'dirty' WHERE slot_id = 0")
            .unwrap();
        fill_ready_slots(pool.clone()).await;
        assert!(pool.startup_leasing.lock().unwrap().is_empty());
        tokio::time::advance(POOL_FILL_RETRY_DELAY).await;
        assert!(jobs.try_recv().is_err());
    }

    #[test]
    fn startup_and_reset_share_the_live_gone_unknown_policy() {
        for state in [
            SlotState::Leasing,
            SlotState::Leased,
            SlotState::Quarantined,
        ] {
            for liveness in [
                LeaseLiveness::Live,
                LeaseLiveness::Gone,
                LeaseLiveness::Unknown,
            ] {
                let record = SlotRecord {
                    info: SlotInfo {
                        sandbox_id: SandboxId::new().to_string(),
                        slot: 0,
                        generation: 7,
                        sandbox_uid: net::sandbox_uid_for_slot(0),
                        pid: None,
                        pod_netns: None,
                    },
                    state,
                    health: SlotHealth::Unknown,
                    lease_kind: LeaseKind::Pooled,
                    last_error: None,
                    memory_reservation_mib: Some(8320),
                };
                let blocker =
                    RuntimeBlocker::new(liveness, liveness::EvidenceSource::TrackedPid, None);
                assert_eq!(
                    assert_lease_process_stopped_with(&record.info, blocker).is_ok(),
                    liveness == LeaseLiveness::Gone
                );
                let updates = runtime_reconcile_updates(vec![record], |_| liveness);
                match (state, liveness) {
                    (_, LeaseLiveness::Gone) => assert!(matches!(
                        updates.as_slice(),
                        [RuntimeReconcileUpdate::DirtyLease { generation: 7, .. }]
                    )),
                    (SlotState::Leasing, LeaseLiveness::Live) => assert!(matches!(
                        updates.as_slice(),
                        [RuntimeReconcileUpdate::PromoteLeasing { generation: 7, .. }]
                    )),
                    _ => assert!(
                        updates.is_empty(),
                        "{state:?}/{liveness:?} must preserve ownership"
                    ),
                }
            }
        }
    }

    #[tokio::test]
    async fn ephemeral_collision_does_not_persist_an_unowned_leasing_row() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        let holder = ResetClaim::acquire(&pool, 0).unwrap();
        assert!(matches!(
            pool.claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission()),
            Err(AllocateSlotError::InProgress(_))
        ));
        let row = record(&pool, 0);
        assert_eq!(row.state, SlotState::Missing);
        assert!(row.info.sandbox_id.is_empty());
        holder.disarm();
    }

    #[tokio::test]
    async fn aborted_ephemeral_claim_does_not_schedule_reset() {
        for failure in ["admission", "execute", "unchanged", "commit"] {
            let pool = test_pool(NetworkPoolConfig::new(1));
            {
                let conn = pool.db.lock().unwrap();
                match failure {
                    "execute" => conn.execute_batch(
                        "CREATE TRIGGER reject_claim BEFORE UPDATE ON network_slots
                         BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
                    ).unwrap(),
                    "unchanged" => conn.execute_batch(
                        "CREATE TRIGGER ignore_claim BEFORE UPDATE ON network_slots
                         BEGIN SELECT RAISE(IGNORE); END;",
                    ).unwrap(),
                    "commit" => conn.execute_batch(
                        "CREATE TABLE parent (id INTEGER PRIMARY KEY);
                         CREATE TABLE child (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
                         CREATE TRIGGER fail_commit AFTER UPDATE ON network_slots
                         BEGIN INSERT INTO child VALUES (1); END;",
                    ).unwrap(),
                    _ => {},
                }
            }
            assert!(
                pool.claim_ephemeral_slot_for_setup(
                    "sandbox-a",
                    None,
                    if failure == "admission" {
                        None
                    } else {
                        memory_admission()
                    },
                )
                .is_err(),
                "{failure}"
            );
            assert_eq!(record(&pool, 0).state, SlotState::Missing, "{failure}");
            assert!(pool.active_resets.lock().unwrap().is_empty(), "{failure}");
            assert!(pool.task_tracker.is_empty(), "{failure}");
        }
    }

    #[tokio::test]
    async fn cancelled_ephemeral_waiter_retains_ownership_until_durable_completion() {
        for fail in [false, true] {
            let pool = test_pool(NetworkPoolConfig::new(1));
            let EphemeralSlotClaimResult::Claimed(claim) = pool
                .claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission())
                .unwrap()
            else {
                panic!("expected a new lease");
            };
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let worker = pool.clone();
            let waiter = tokio::spawn(async move {
                worker
                    .run_ephemeral_setup(claim, move || {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        if fail {
                            Err(EphemeralSetupError::Validation(
                                SlotValidationError::Missing("injected setup failure".into()),
                            ))
                        } else {
                            Ok(())
                        }
                    })
                    .await
            });
            entered_rx.await.unwrap();
            waiter.abort();
            assert!(waiter.await.unwrap_err().is_cancelled());
            assert!(ResetClaim::acquire(&pool, 0).is_none());
            assert_eq!(record(&pool, 0).state, SlotState::Leasing);
            release_tx.send(()).unwrap();
            pool.task_tracker.close();
            pool.task_tracker.wait().await;
            assert_eq!(
                record(&pool, 0).state,
                if fail {
                    SlotState::Quarantined
                } else {
                    SlotState::Leased
                }
            );
            assert!(pool.active_resets.lock().unwrap().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ephemeral_completion_storage_failure_retains_and_retries_cleanup_intent() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        let EphemeralSlotClaimResult::Claimed(claim) = pool
            .claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission())
            .unwrap()
        else {
            panic!("expected a new lease");
        };
        pool.db
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_completion BEFORE UPDATE ON network_slots
             BEGIN SELECT RAISE(FAIL, 'injected storage failure'); END;",
            )
            .unwrap();
        assert!(matches!(
            pool.mark_ephemeral_slot_leased(&claim),
            Err(AllocateSlotError::Store(_))
        ));
        {
            let recovery = pool.recover_ephemeral_persistence_failure(&claim);
            tokio::pin!(recovery);
            assert!(futures_util::poll!(&mut recovery).is_pending());
            assert_eq!(record(&pool, 0).state, SlotState::Leasing);
            assert!(ResetClaim::acquire(&pool, 0).is_none());
            pool.db
                .lock()
                .unwrap()
                .execute_batch("DROP TRIGGER reject_completion")
                .unwrap();
            tokio::time::advance(POOL_FILL_RETRY_DELAY).await;
            recovery.await;
        }
        let row = record(&pool, 0);
        assert_eq!(row.state, SlotState::Dirty);
        assert_eq!(row.info.sandbox_id, "sandbox-a");
        assert_eq!(row.memory_reservation_mib, None);
        drop(claim);
        pool.shutdown_token.cancel();
        pool.task_tracker.close();
        pool.task_tracker.wait().await;
    }

    #[tokio::test]
    async fn reset_collision_keeps_a_retry_after_the_holder_observes_a_leased_row() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        let holder = ResetClaim::acquire(&pool, 0).unwrap();
        holder.disarm();
        // The holder is about to exit after observing a non-resettable row.
        // A release can enqueue another attempt before that holder drops.
        pool.db
            .lock()
            .unwrap()
            .execute_batch("UPDATE network_slots SET state = 'dirty', generation = 7")
            .unwrap();
        reset_slot(pool.clone(), 0).await;
        assert!(
            reset_candidates(&pool, u32::MAX)
                .unwrap()
                .entries
                .is_empty()
        );
        drop(holder);
        assert_eq!(
            reset_candidates(&pool, u32::MAX).unwrap().entries,
            vec![(0, 7)]
        );
    }

    #[test]
    fn durable_work_scan_rotates_past_held_owners_without_reclaiming_them() {
        // More than one page of durable work, with the entire first page still
        // owned by blocking work whose async waiter may already have gone.
        for state in ["dirty", "resetting", "creating", "destroying"] {
            let pool = test_pool(NetworkPoolConfig::new(48));
            pool.db
                .lock()
                .unwrap()
                .execute(
                    "UPDATE network_slots SET state = ?1, generation = 7",
                    [state],
                )
                .unwrap();
            let owners: Vec<_> = (0..20)
                .map(|slot| ResetClaim::acquire(&pool, slot).unwrap())
                .collect();
            let mut cursor = u32::MAX;
            let mut discovered = std::collections::BTreeSet::new();
            for _ in 0..8 {
                if matches!(state, "dirty" | "resetting") {
                    let page = reset_candidates(&pool, cursor).unwrap();
                    cursor = page.next_cursor;
                    discovered.extend(page.entries.into_iter().map(|(slot, _)| slot));
                } else {
                    let (next, slots) = recover_abandoned_work(&pool, cursor).unwrap();
                    cursor = next;
                    discovered.extend(slots);
                }
            }
            for slot in 0..20 {
                assert_eq!(
                    record(&pool, slot).state,
                    state.parse::<SlotState>().unwrap()
                );
                assert!(!discovered.contains(&slot));
            }
            if state == "creating" {
                for slot in 20..48 {
                    let recovered = record(&pool, slot);
                    assert_eq!(recovered.state, SlotState::Missing);
                    assert_eq!(recovered.health, SlotHealth::Degraded);
                    assert_eq!(recovered.info.generation, 7);
                }
            } else {
                assert_eq!(discovered, (20..48).collect());
            }
            for owner in owners {
                owner.disarm();
            }
        }
    }

    #[tokio::test]
    async fn reset_claim_survives_cancelled_waiter_until_blocking_work_finishes() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let owned = pool.clone();
        let waiter = tokio::spawn(async move {
            let claim = ResetClaim::acquire(&owned, 0).unwrap();
            let blocking_claim = claim.clone();
            tokio::task::spawn_blocking(move || {
                let _claim = blocking_claim;
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                drop(_claim);
                finished_tx.send(()).unwrap();
            })
            .await
            .unwrap();
        });
        entered_rx.await.unwrap();
        assert!(ResetClaim::acquire(&pool, 0).is_none());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(
            ResetClaim::acquire(&pool, 0).is_none(),
            "host work still owns the slot"
        );
        release_tx.send(()).unwrap();
        finished_rx.await.unwrap();
        assert!(
            ResetClaim::acquire(&pool, 0).is_some(),
            "abandoned durable claim is recoverable"
        );
    }

    #[cfg(target_os = "linux")]
    struct SandboxMarkerProcess(std::process::Child);

    #[cfg(target_os = "linux")]
    impl SandboxMarkerProcess {
        fn spawn(sandbox_id: &str) -> Self {
            // Keep the marker in the tracked PID's command line. A shell may
            // exec a single external command and drop its own argv entirely.
            let marker = Self(
                std::process::Command::new("sh")
                    .arg("-c")
                    .arg("while :; do :; done")
                    .arg(sandbox_id)
                    .spawn()
                    .unwrap(),
            );
            // `spawn` returns once the child is inside exec, before the kernel
            // publishes the new argv. Until then `/proc/<pid>/cmdline` is
            // empty and the liveness probe reads the process as not owning
            // the sandbox, so wait for the token the probe matches.
            let cmdline = format!("/proc/{}/cmdline", marker.id());
            let give_up = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while !std::fs::read(&cmdline)
                .unwrap_or_default()
                .split(|byte| *byte == 0)
                .any(|arg| arg == sandbox_id.as_bytes())
            {
                assert!(
                    std::time::Instant::now() < give_up,
                    "{cmdline} never showed the {sandbox_id} marker"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            marker
        }

        fn id(&self) -> u32 {
            self.0.id()
        }
    }

    #[cfg(target_os = "linux")]
    impl Drop for SandboxMarkerProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn test_pool(config: NetworkPoolConfig) -> std::sync::Arc<NetworkPool> {
        NetworkPool::open_in_memory_for_test(config)
    }

    #[test]
    fn startup_preflight_rejects_unsafe_capacity_without_repairing_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slots.db");
        let config = NetworkPoolConfig::new(2);
        // Preflight must not create a missing authoritative store.
        NetworkPool::validate_startup(&config, &path).unwrap_err();
        assert!(!path.exists());
        let mut conn = Connection::open(&path).unwrap();
        init_in_memory_db(&mut conn, &config).unwrap();
        for state in ["leased", "ready", "creating", "future-state"] {
            conn.execute(
                "UPDATE network_slots SET state = ?1 WHERE slot_id = 1",
                [state],
            )
            .unwrap();
            NetworkPool::validate_startup(&NetworkPoolConfig::new(1), &path).unwrap_err();
            let persisted: String = conn
                .query_row(
                    "SELECT state FROM network_slots WHERE slot_id = 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(persisted, state);
        }
        conn.execute("UPDATE network_slots SET state = 'missing'", [])
            .unwrap();
        NetworkPool::validate_startup(&NetworkPoolConfig::new(1), &path).unwrap();
    }

    #[test]
    fn startup_repair_does_not_turn_partial_host_work_into_safe_shrink() {
        for state in [
            "creating",
            "destroying",
            "resetting",
            "missing",
            "old-schema",
        ] {
            let pool = test_pool(NetworkPoolConfig::new(2));
            let mut conn = pool.db.lock().unwrap();
            if state == "old-schema" {
                conn.execute_batch("UPDATE network_slots SET state = 'ready', schema_version = 0 WHERE slot_id = 1").unwrap();
            } else {
                conn.execute(
                    "UPDATE network_slots SET state = ?1, health = 'degraded' WHERE slot_id = 1",
                    [state],
                )
                .unwrap();
            }
            init_db(&mut conn, &NetworkPoolConfig::new(2)).unwrap();
            let pending = query_record_by_slot(&conn, 1).unwrap().unwrap();
            assert_eq!(
                pending.state,
                if state == "destroying" {
                    SlotState::Destroying
                } else {
                    SlotState::Missing
                },
                "{state}"
            );
            assert_eq!(pending.health, SlotHealth::Degraded, "{state}");
            assert!(
                init_db(&mut conn, &NetworkPoolConfig::new(1)).is_err(),
                "{state}"
            );
        }
    }

    #[test]
    fn startup_shrink_preserves_owned_slots() {
        for state in ["leasing", "leased", "dirty", "resetting", "quarantined"] {
            let pool = test_pool(NetworkPoolConfig::new(2));
            let mut conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots SET state = ?1, sandbox_id = 'sandbox-a',
                 sandbox_uid = 1234, generation = 7, memory_reservation_mib = 8320
                 WHERE slot_id = 1",
                params![state],
            )
            .unwrap();
            assert!(
                init_db(&mut conn, &NetworkPoolConfig::new(1)).is_err(),
                "{state}"
            );
            let row = query_record_by_slot(&conn, 1).unwrap().unwrap();
            assert_eq!(row.state.as_str(), state);
            assert_eq!(row.info.sandbox_id, "sandbox-a");
            assert_eq!(row.info.generation, 7);
            assert_eq!(row.memory_reservation_mib, Some(8320));
        }
        let pool = test_pool(NetworkPoolConfig::new(2));
        init_db(&mut pool.db.lock().unwrap(), &NetworkPoolConfig::new(1)).unwrap();
        // Unleased does not mean torn down: these states can still own netns,
        // veth and firewall resources even without a recorded pod namespace.
        for state in ["ready", "creating", "destroying"] {
            let pool = test_pool(NetworkPoolConfig::new(2));
            let mut conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots SET state = ?1 WHERE slot_id = 1",
                [state],
            )
            .unwrap();
            assert!(
                init_db(&mut conn, &NetworkPoolConfig::new(1)).is_err(),
                "{state}"
            );
            assert_eq!(
                query_record_by_slot(&conn, 1).unwrap().unwrap().state,
                state.parse::<SlotState>().unwrap()
            );
        }
    }

    #[test]
    fn store_read_failures_are_not_absence_or_empty_inventory() {
        let pool = test_pool(NetworkPoolConfig::new(1));
        assert!(pool.get("sandbox-a").unwrap().is_none());
        assert!(!pool.is_draining().unwrap());
        pool.db
            .lock()
            .unwrap()
            .execute_batch("DROP TABLE network_slots; DROP TABLE network_pool_meta;")
            .unwrap();
        pool.get("sandbox-a").unwrap_err();
        pool.get_record("sandbox-a").unwrap_err();
        pool.dirty_infos_for_sandbox("sandbox-a").unwrap_err();
        pool.all_records().unwrap_err();
        pool.counts().unwrap_err();
        pool.is_draining().unwrap_err();
        pool.release("sandbox-a").unwrap_err();
        assert!(matches!(
            pool.claim_allocation("sandbox-a", None, memory_admission()),
            Err(AllocateSlotError::Store(_))
        ));
    }

    #[test]
    fn unknown_persisted_vocabulary_blocks_startup_and_inventory() {
        for statement in [
            "UPDATE network_slots SET state = 'future-state'",
            "UPDATE network_slots SET health = 'future-health'",
            "UPDATE network_slots SET lease_kind = 'future-kind'",
        ] {
            let pool = test_pool(NetworkPoolConfig::new(1));
            pool.db.lock().unwrap().execute_batch(statement).unwrap();
            assert!(pool.all_records().is_err(), "{statement}");
            assert!(
                init_db(&mut pool.db.lock().unwrap(), &NetworkPoolConfig::new(1)).is_err(),
                "{statement}"
            );
        }
    }

    async fn worker_hook_guard() -> tokio::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

    fn record(pool: &NetworkPool, slot_id: u32) -> SlotRecord {
        let conn = pool.db.lock().unwrap();
        query_record_by_slot(&conn, slot_id).unwrap().unwrap()
    }

    fn cleanup_reset_attempts(pool: &NetworkPool, slot_id: u32) -> u32 {
        let conn = pool.db.lock().unwrap();
        conn.query_row(
            "SELECT cleanup_reset_attempts FROM network_slots WHERE slot_id = ?1",
            params![slot_id],
            |row| row.get::<_, u32>(0),
        )
        .unwrap()
    }

    fn memory_admission() -> Option<NewLeaseMemoryReservation> {
        Some(NewLeaseMemoryReservation {
            memory_reservation_mib: 8320,
        })
    }

    #[test]
    fn zero_warm_pool_config_is_valid() {
        let mut config = NetworkPoolConfig::new(4);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;

        validate_config(&config).unwrap();
        assert!(!config.warm_pool_enabled());
    }

    /// An update has to be answerable as publishable-or-not *before* a caller
    /// acts on it. The live slot-increase path creates kernel loop devices and
    /// `/dev` nodes for the new budget and never removes them, so validating
    /// only inside `update_config` would let a malformed admin request leave
    /// durable host state behind — repeatably, since the request can be retried.
    #[test]
    fn candidate_config_rejects_an_unpublishable_update_without_applying_it() {
        let mut config = NetworkPoolConfig::new(4);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);

        // Ready slots above the ceiling: rejected by the same validation
        // `update_config` applies.
        let invalid = NetworkPoolConfigUpdate {
            max_slots: Some(8),
            target_ready_slots: Some(99),
            retained_ready_slots: None,
            min_ready_slots: None,
        };
        assert!(
            pool.candidate_config(&invalid).is_err(),
            "an update that update_config would reject must be rejectable up front"
        );
        assert_eq!(
            pool.config().max_slots,
            4,
            "validating a candidate must not publish it"
        );

        // A shrink is refused live, so the increase path must not act on one.
        let shrink = NetworkPoolConfigUpdate {
            max_slots: Some(2),
            target_ready_slots: None,
            retained_ready_slots: None,
            min_ready_slots: None,
        };
        pool.candidate_config(&shrink).unwrap_err();

        // The publishable increase resolves to the budget that will be applied,
        // which is what the capacity step must size devices from.
        let valid = NetworkPoolConfigUpdate {
            max_slots: Some(8),
            target_ready_slots: None,
            retained_ready_slots: None,
            min_ready_slots: None,
        };
        let candidate = pool
            .candidate_config(&valid)
            .expect("a valid increase yields a candidate");
        assert_eq!(candidate.max_slots, 8);
        assert_eq!(
            pool.config().max_slots,
            4,
            "still not published until update_config runs"
        );
        assert_eq!(pool.update_config(valid).unwrap().max_slots, 8);
        assert_eq!(pool.config().max_slots, 8);
    }

    #[test]
    fn dynamic_claim_records_ephemeral_requested_namespace() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);

        let claim = pool
            .claim_ephemeral_slot_for_setup(
                "sandbox-a",
                Some("pod-a".to_string()),
                memory_admission(),
            )
            .unwrap();
        let EphemeralSlotClaimResult::Claimed(claim) = claim else {
            panic!("expected a new dynamic claim");
        };

        assert_eq!(claim.info.slot, 0);
        assert_eq!(claim.info.pod_netns.as_deref(), Some("pod-a"));
        let record = record(&pool, 0);
        assert_eq!(record.state, SlotState::Leasing);
        assert_eq!(record.lease_kind, LeaseKind::Ephemeral);
        assert_eq!(record.info.sandbox_id, "sandbox-a");
        assert_eq!(
            record.info.pod_netns, None,
            "old namespace remains authoritative until teardown"
        );
        assert_eq!(record.memory_reservation_mib, Some(8320));
    }

    #[test]
    fn dynamic_claim_can_borrow_idle_ready_slot() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET state = 'ready', health = 'healthy', pod_netns_path = 'old-pod',
                     lease_kind = ?1
                 WHERE slot_id = 0",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();
        }

        let claim = pool
            .claim_ephemeral_slot_for_setup(
                "sandbox-a",
                Some("pod-a".to_string()),
                memory_admission(),
            )
            .unwrap();
        let EphemeralSlotClaimResult::Claimed(claim) = claim else {
            panic!("expected a borrowed ready slot");
        };

        assert_eq!(claim.info.slot, 0);
        let current = record(&pool, 0);
        assert_eq!(current.state, SlotState::Leasing);
        assert_eq!(current.lease_kind, LeaseKind::Ephemeral);
        assert_eq!(current.info.pod_netns.as_deref(), Some("old-pod"));
        let error = pool
            .handoff_network_namespace_with(
                0,
                claim.info.generation,
                SlotState::Leasing,
                Some("pod-a"),
                |_, _, previous| {
                    assert_eq!(previous, Some("old-pod"));
                    Err(net::NetworkTeardownError::Remaining {
                        resource: "sveth".into(),
                    })
                },
            )
            .unwrap_err();
        pool.persist_ephemeral_setup_failure(
            &claim,
            SetupRetryKey::ephemeral(0),
            &EphemeralSetupError::Handoff(error),
        )
        .unwrap();
        drop(claim);
        {
            let conn = pool.db.lock().unwrap();
            reconcile_startup(&conn, &pool.config()).unwrap();
        }
        assert_eq!(record(&pool, 0).info.pod_netns.as_deref(), Some("old-pod"));
        let EphemeralSlotClaimResult::Claimed(claim) = pool
            .claim_ephemeral_slot_for_setup("sandbox-b", Some("pod-b".into()), memory_admission())
            .unwrap()
        else {
            panic!("expected retry claim")
        };
        // A superseded task cannot even enter teardown.
        assert!(
            pool.handoff_network_namespace_with(
                0,
                claim.info.generation - 1,
                SlotState::Leasing,
                Some("pod-b"),
                |_, _, _| panic!("stale teardown")
            )
            .is_err()
        );
        pool.handoff_network_namespace_with(
            0,
            claim.info.generation,
            SlotState::Leasing,
            Some("pod-b"),
            |_, _, previous| {
                assert_eq!(previous, Some("old-pod"));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(record(&pool, 0).info.pod_netns.as_deref(), Some("pod-b"));
        let error = EphemeralSetupError::Setup(net::NetworkSetupError::HostVeth {
            source: net::NetworkOpError::transient("setup failed"),
        });
        pool.persist_ephemeral_setup_failure(&claim, SetupRetryKey::ephemeral(0), &error)
            .unwrap();
        drop(claim);
        pool.update_config(NetworkPoolConfigUpdate {
            target_ready_slots: Some(1),
            retained_ready_slots: Some(1),
            ..Default::default()
        })
        .unwrap();
        let (slot, generation) = claim_missing_for_create(&pool, true).unwrap();
        assert_eq!(record(&pool, slot).info.pod_netns.as_deref(), Some("pod-b"));
        pool.handoff_network_namespace_with(
            slot,
            generation,
            SlotState::Creating,
            None,
            |_, _, previous| {
                assert_eq!(previous, Some("pod-b"));
                Ok(())
            },
        )
        .unwrap();
        assert!(record(&pool, slot).info.pod_netns.is_none());
    }

    #[test]
    fn active_lease_rejects_requested_namespace_mismatch() {
        let info = SlotInfo {
            sandbox_id: "sandbox-a".to_string(),
            slot: 0,
            generation: 1,
            sandbox_uid: net::sandbox_uid_for_slot(0),
            pid: None,
            pod_netns: Some("pod-a".to_string()),
        };
        let record = SlotRecord {
            info,
            state: SlotState::Leased,
            health: SlotHealth::Healthy,
            lease_kind: LeaseKind::Ephemeral,
            last_error: None,
            memory_reservation_mib: Some(8320),
        };

        active_record_to_allocate_result(&record, &Some("pod-b".to_string())).unwrap_err();
        assert!(
            active_record_to_allocate_result(&record, &Some("pod-a".to_string()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn reset_guard_preserves_a_live_vm_without_a_tracked_pid() {
        let info = SlotInfo {
            sandbox_id: "sandbox-a".to_string(),
            slot: 0,
            generation: 1,
            sandbox_uid: net::sandbox_uid_for_slot(0),
            pid: None,
            pod_netns: Some("pod-a".to_string()),
        };

        let blocker = RuntimeBlocker::new(
            LeaseLiveness::Live,
            liveness::EvidenceSource::VmCgroup,
            None,
        );
        let err = assert_lease_process_stopped_with(&info, blocker)
            .expect_err("live VM must block slot reset");

        assert!(matches!(
            err,
            SlotResetError::RuntimeNotGone {
                slot: 0,
                generation: 1,
                blocker: RuntimeBlocker {
                    liveness: LeaseLiveness::Live,
                    ..
                },
                ..
            }
        ));
        assert_eq!(err.disposition(), SlotResetDisposition::WaitForLeaseProcess);
    }

    #[test]
    fn dynamic_claim_retires_dead_lease_when_pod_namespace_disappeared() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, pid = NULL,
                     pod_netns_path = 'space-test-missing-netns-old',
                     lease_kind = ?2
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
        }

        let claim = pool
            .claim_ephemeral_slot_for_setup(
                "sandbox-a",
                Some("space-test-missing-netns-new".to_string()),
                memory_admission(),
            )
            .unwrap();
        let EphemeralSlotClaimResult::Claimed(claim) = claim else {
            panic!("expected a fresh claim after retiring stale lease");
        };

        assert_eq!(claim.info.slot, 1);
        assert_eq!(
            claim.info.pod_netns.as_deref(),
            Some("space-test-missing-netns-new")
        );
        let stale = record(&pool, 0);
        assert_eq!(stale.state, SlotState::Dirty);
        assert_eq!(stale.health, SlotHealth::Degraded);
        assert_eq!(stale.info.sandbox_id, "sandbox-a");
        assert!(
            stale
                .last_error
                .as_deref()
                .unwrap()
                .contains("retired stale network lease for sandbox sandbox-a")
        );
        assert_eq!(pool.get("sandbox-a").unwrap().unwrap().slot, 1);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dynamic_claim_keeps_live_lease_when_pod_namespace_mismatches() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        let child = SandboxMarkerProcess::spawn("sandbox-a");
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, pid = ?2,
                     pod_netns_path = 'space-test-missing-netns-old',
                     lease_kind = ?3
                 WHERE slot_id = 0",
                params![
                    net::sandbox_uid_for_slot(0),
                    child.id(),
                    LeaseKind::Ephemeral.as_str()
                ],
            )
            .unwrap();
        }

        let error = pool
            .claim_ephemeral_slot_for_setup(
                "sandbox-a",
                Some("space-test-missing-netns-new".to_string()),
                memory_admission(),
            )
            .unwrap_err();

        assert!(matches!(error, AllocateSlotError::FailedPrecondition(_)));
        assert_eq!(record(&pool, 0).state, SlotState::Leased);
    }

    #[test]
    fn allocation_ignores_previous_dirty_slot_for_same_sandbox() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, lease_kind = ?2,
                     memory_reservation_mib = 8320
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Pooled.as_str()],
            )
            .unwrap();
        }
        let released = pool.release("sandbox-a").unwrap().unwrap();
        assert_eq!(released.slot, 0);
        assert_eq!(record(&pool, 0).info.sandbox_id, "sandbox-a");
        assert_eq!(record(&pool, 0).memory_reservation_mib, None);

        let claim = pool
            .claim_ephemeral_slot_for_setup(
                "sandbox-a",
                Some("pod-a".to_string()),
                memory_admission(),
            )
            .unwrap();
        let EphemeralSlotClaimResult::Claimed(claim) = claim else {
            panic!("expected a fresh claim while previous slot resets");
        };

        assert_eq!(claim.info.slot, 1);
        assert_eq!(pool.get("sandbox-a").unwrap().unwrap().slot, 1);
        assert_eq!(record(&pool, 0).state, SlotState::Dirty);
        assert_eq!(record(&pool, 1).state, SlotState::Leasing);
    }

    #[test]
    fn admitted_new_lease_is_not_rejected_by_an_existing_persisted_cap() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);

        pool.claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission())
            .expect("first reservation fits");

        pool.claim_ephemeral_slot_for_setup("sandbox-b", None, memory_admission())
            .expect("measured admission happens before persisting the new cap");

        assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
        assert_eq!(record(&pool, 1).memory_reservation_mib, Some(8320));
    }

    #[test]
    fn active_legacy_lease_is_backfilled_with_configured_memory_reservation() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, lease_kind = ?2,
                     memory_reservation_mib = NULL
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();

            backfill_active_memory_reservations(&conn, 8320).unwrap();
        }

        assert_eq!(record(&pool, 0).memory_reservation_mib, Some(8320));
    }

    #[test]
    fn release_targets_current_lease_when_previous_slot_is_dirty() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'dirty', health = 'unknown',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, lease_kind = ?2
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Pooled.as_str()],
            )
            .unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, lease_kind = ?2
                 WHERE slot_id = 1",
                params![net::sandbox_uid_for_slot(1), LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
        }

        let released = pool.release("sandbox-a").unwrap().unwrap();

        assert_eq!(released.slot, 1);
        assert_eq!(record(&pool, 0).state, SlotState::Dirty);
        assert_eq!(record(&pool, 1).state, SlotState::Dirty);
        assert_eq!(record(&pool, 0).info.sandbox_id, "sandbox-a");
        assert_eq!(record(&pool, 1).info.sandbox_id, "sandbox-a");
    }

    #[test]
    fn startup_reconcile_does_not_dirty_pidless_lease_before_runtime_reconcile() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, pid = NULL,
                     lease_kind = ?2
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            reconcile_startup(&conn, &config).unwrap();
        }

        let lease = record(&pool, 0);
        assert_eq!(lease.state, SlotState::Leased);
        assert_eq!(lease.info.sandbox_id, "sandbox-a");
        assert_eq!(lease.info.pid, None);
    }

    #[test]
    fn runtime_reconcile_keeps_dead_lease_uid_reserved_while_allowing_new_lease() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let mut conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, pid = NULL,
                     lease_kind = ?2
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            reconcile_startup(&conn, &config).unwrap();
            reconcile_runtime_state(&mut conn, &config).unwrap();
        }

        let stale = record(&pool, 0);
        assert_eq!(stale.state, SlotState::Dirty);
        assert_eq!(stale.info.sandbox_id, "sandbox-a");
        assert_eq!(stale.info.sandbox_uid, net::sandbox_uid_for_slot(0));
        assert!(pool.get("sandbox-a").unwrap().is_none());

        let claim = pool
            .claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission())
            .unwrap();
        let EphemeralSlotClaimResult::Claimed(claim) = claim else {
            panic!("expected a fresh claim while reconciled dead lease is dirty");
        };

        assert_eq!(claim.info.slot, 1);
        assert_eq!(claim.info.sandbox_uid, net::sandbox_uid_for_slot(1));
        assert_eq!(pool.get("sandbox-a").unwrap().unwrap().slot, 1);
    }

    #[test]
    fn runtime_reconcile_dirties_interrupted_leasing_slot() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let mut conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'leasing', health = 'unknown',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, pid = NULL,
                     lease_kind = ?2
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();

            reconcile_startup(&conn, &config).unwrap();
            reconcile_runtime_state(&mut conn, &config).unwrap();
        }

        let stale = record(&pool, 0);
        assert_eq!(stale.state, SlotState::Dirty);
        assert_eq!(stale.info.sandbox_id, "sandbox-a");
        assert_eq!(stale.info.sandbox_uid, net::sandbox_uid_for_slot(0));
        assert!(pool.get("sandbox-a").unwrap().is_none());
    }

    #[test]
    fn runtime_reconcile_preserves_pidless_leasing_slot_with_live_vm_evidence() {
        let info = SlotInfo {
            sandbox_id: "sandbox-a".to_string(),
            slot: 0,
            generation: 1,
            sandbox_uid: net::sandbox_uid_for_slot(0),
            pid: None,
            pod_netns: Some("pod-a".to_string()),
        };
        let record = SlotRecord {
            info,
            state: SlotState::Leasing,
            health: SlotHealth::Unknown,
            lease_kind: LeaseKind::Ephemeral,
            last_error: None,
            memory_reservation_mib: Some(8320),
        };

        let updates = runtime_reconcile_updates(vec![record], |_| LeaseLiveness::Live);

        assert!(matches!(
            updates.as_slice(),
            [RuntimeReconcileUpdate::PromoteLeasing { slot_id: 0, .. }]
        ));
    }

    #[test]
    fn stale_create_completion_does_not_overwrite_reclaimed_slot() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 8, state = 'leased', health = 'healthy',
                     sandbox_id = 'sandbox-new', sandbox_uid = ?1, pid = 4242,
                     pod_netns_path = 'pod-new', lease_kind = ?2,
                     memory_reservation_mib = 4096, last_error = NULL
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
        }

        let stale_generation = 7;
        {
            let conn = pool.db.lock().unwrap();
            assert_eq!(
                persist_created_slot(&conn, &config, 0, stale_generation).unwrap(),
                0
            );
            assert_eq!(
                persist_transient_create_failure(&conn, 0, stale_generation, "transient").unwrap(),
                0
            );
            assert_eq!(
                persist_create_quarantine(&conn, 0, stale_generation, "quarantine").unwrap(),
                0
            );
        }

        let current = record(&pool, 0);
        assert_eq!(current.info.generation, 8);
        assert_eq!(current.state, SlotState::Leased);
        assert_eq!(current.health, SlotHealth::Healthy);
        assert_eq!(current.info.sandbox_id, "sandbox-new");
        assert_eq!(current.info.sandbox_uid, net::sandbox_uid_for_slot(0));
        assert_eq!(current.info.pid, Some(4242));
        assert_eq!(current.info.pod_netns.as_deref(), Some("pod-new"));
        assert_eq!(current.lease_kind, LeaseKind::Ephemeral);
        assert_eq!(current.memory_reservation_mib, Some(4096));
        assert_eq!(current.last_error, None);
    }

    #[test]
    fn runtime_reconcile_promotes_live_leasing_slot_with_persisted_pid() {
        let info = SlotInfo {
            sandbox_id: "sandbox-a".to_string(),
            slot: 0,
            generation: 1,
            sandbox_uid: net::sandbox_uid_for_slot(0),
            pid: Some(1234),
            pod_netns: Some("pod-a".to_string()),
        };
        let record = SlotRecord {
            info,
            state: SlotState::Leasing,
            health: SlotHealth::Unknown,
            lease_kind: LeaseKind::Ephemeral,
            last_error: None,
            memory_reservation_mib: Some(8320),
        };

        let updates = runtime_reconcile_updates(vec![record], |_| LeaseLiveness::Live);

        assert!(matches!(
            updates.as_slice(),
            [RuntimeReconcileUpdate::PromoteLeasing { slot_id: 0, .. }]
        ));
    }

    #[test]
    fn startup_reconcile_normalizes_interrupted_create_for_worker_recovery() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 1;
        config.retained_ready_slots = 1;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 7, state = 'creating', health = 'unknown',
                     pod_netns_path = NULL, lease_kind = ?1,
                     cleanup_reset_attempts = 3, last_reset_retry_at = 42
                 WHERE slot_id = 0",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            reconcile_startup(&conn, &config).unwrap();
            let candidates = query_startup_recoverable_idle_records(&conn, &config).unwrap();
            assert_eq!(candidates.len(), 1);
            assert_eq!(candidates[0].state, SlotState::Missing);
        }

        let recovered = record(&pool, 0);
        assert_eq!(recovered.state, SlotState::Missing);
        assert_eq!(recovered.health, SlotHealth::Degraded);
        assert_eq!(cleanup_reset_attempts(&pool, 0), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_recovery_uses_fresh_validation_error_for_kubernetes_slot() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 1;
        config.retained_ready_slots = 1;
        config.min_ready_slots = 0;
        config.pod_netns_path = Some("space-test-missing-pod-netns".to_string());
        let pool = test_pool(config.clone());
        let candidates = {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 4, state = 'creating', health = 'unknown',
                     pod_netns_path = 'space-test-missing-pod-netns', lease_kind = ?1,
                     last_error = 'root cause from interrupted create'
                 WHERE slot_id = 0",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            reconcile_startup(&conn, &config).unwrap();
            query_startup_recoverable_idle_records(&conn, &config).unwrap()
        };
        assert_eq!(candidates.len(), 1);

        let RecoverableValidationResult::Update(update) =
            validate_recoverable_idle_record_with(&config, candidates[0].clone(), |slot_id, _| {
                Err(SlotValidationError::Missing(format!(
                    "missing namespace {}",
                    NetId::from_slot(slot_id).ns()
                )))
            })
            .await
        else {
            panic!("missing slot namespace should produce a degraded update");
        };

        {
            let mut conn = pool.db.lock().unwrap();
            apply_ready_validation_updates(&mut conn, &config, &[update]).unwrap();
        }

        let recovered = record(&pool, 0);
        assert_eq!(recovered.state, SlotState::Missing);
        assert_eq!(recovered.health, SlotHealth::Degraded);
        assert_ne!(
            recovered.last_error.as_deref(),
            Some("root cause from interrupted create")
        );
        assert!(
            recovered
                .last_error
                .as_deref()
                .unwrap()
                .starts_with("missing namespace sns-")
        );
    }

    #[test]
    fn startup_recovery_unwedges_interrupted_idle_slot_without_warm_pool() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 4, state = 'creating', health = 'unknown',
                     pod_netns_path = NULL, lease_kind = ?1,
                     cleanup_reset_attempts = 3, last_reset_retry_at = 42,
                     last_error = 'original interrupted create error'
                 WHERE slot_id = 0",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            reconcile_startup(&conn, &config).unwrap();
        }

        let recovered = record(&pool, 0);
        assert_eq!(recovered.state, SlotState::Missing);
        assert_eq!(recovered.health, SlotHealth::Degraded);
        assert_eq!(cleanup_reset_attempts(&pool, 0), 0);
        assert_eq!(
            recovered.last_error.as_deref(),
            Some("original interrupted create error")
        );

        let claim = pool
            .claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission())
            .unwrap();
        assert!(matches!(claim, EphemeralSlotClaimResult::Claimed(_)));
    }

    #[tokio::test]
    async fn startup_reconcile_preserves_interrupted_destroy_cleanup_obligation() {
        for schema_version in [0, NETWORK_POOL_SCHEMA_VERSION] {
            let mut config = NetworkPoolConfig::new(1);
            config.target_ready_slots = 0;
            config.retained_ready_slots = 0;
            config.min_ready_slots = 0;
            let pool = test_pool(config.clone());
            {
                let conn = pool.db.lock().unwrap();
                conn.execute(
                    "UPDATE network_slots
                 SET generation = 2, state = 'ready', health = 'healthy',
                     lease_kind = ?1, pod_netns_path = 'old-pod'
                 WHERE slot_id = 0",
                    params![LeaseKind::Pooled.as_str()],
                )
                .unwrap();
            }
            assert_eq!(claim_ready_for_destroy(&pool).unwrap(), Some(0));
            {
                let conn = pool.db.lock().unwrap();
                conn.execute(
                    "UPDATE network_slots SET schema_version = ?1 WHERE slot_id = 0",
                    [schema_version],
                )
                .unwrap();
                reconcile_startup(&conn, &config).unwrap();
                let candidates = query_startup_recoverable_idle_records(&conn, &config).unwrap();
                assert!(candidates.is_empty());
            }

            let recovered = record(&pool, 0);
            assert_eq!(recovered.state, SlotState::Destroying);
            assert_eq!(recover_abandoned_work(&pool, u32::MAX).unwrap().1, vec![0]);
            super::super::worker::destroy_slot_with(pool.clone(), 0, |_, _, namespace| {
                assert_eq!(namespace, Some("old-pod"));
                Err(net::NetworkTeardownError::Remaining {
                    resource: "sveth".into(),
                })
            })
            .await;
            let pending = record(&pool, 0);
            assert_eq!(pending.state, SlotState::Destroying);
            assert_eq!(pending.info.pod_netns.as_deref(), Some("old-pod"));
            assert!(pending.last_error.unwrap().contains("sveth"));
            assert!(pool.maintenance_failed.load(Ordering::Relaxed));
            {
                let conn = pool.db.lock().unwrap();
                reconcile_startup(&conn, &config).unwrap();
            }
            assert_eq!(recover_abandoned_work(&pool, u32::MAX).unwrap().1, vec![0]);
            super::super::worker::destroy_slot_with(pool.clone(), 0, |_, _, namespace| {
                assert_eq!(namespace, Some("old-pod"));
                Ok(())
            })
            .await;
            assert_eq!(record(&pool, 0).state, SlotState::Missing);
            assert!(record(&pool, 0).info.pod_netns.is_none());
            assert!(claim_ready_for_destroy(&pool).unwrap().is_none());
            assert!(
                recover_abandoned_work(&pool, u32::MAX)
                    .unwrap()
                    .1
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn retirement_progresses_while_drained_without_rebuilding_capacity() {
        for (state, health) in [
            ("ready", "healthy"),
            ("ready", "unknown"),
            ("missing", "degraded"),
            ("creating", "unknown"),
        ] {
            let pool = test_pool(NetworkPoolConfig::new(2));
            pool.db.lock().unwrap().execute(
                "UPDATE network_slots SET state = ?1, health = ?2, pod_netns_path = 'old-pod' WHERE slot_id = 0",
                params![state, health],
            ).unwrap();
            pool.set_draining(true).unwrap();
            pool.update_config(NetworkPoolConfigUpdate {
                target_ready_slots: Some(0),
                retained_ready_slots: Some(0),
                min_ready_slots: Some(0),
                ..Default::default()
            })
            .unwrap();
            super::super::scheduler::reconcile_pool_with(
                pool.clone(),
                u32::MAX,
                |_, _, namespace| {
                    assert_eq!(namespace, Some("old-pod"));
                    Ok(())
                },
            )
            .await;
            let retired = record(&pool, 0);
            assert_eq!(retired.state, SlotState::Missing);
            assert_eq!(retired.health, SlotHealth::Unknown);
            assert!(retired.info.pod_netns.is_none());
            assert!(pool.is_draining().unwrap());
            assert!(matches!(
                pool.claim_allocation(&SandboxId::new().to_string(), None, memory_admission())
                    .unwrap_err(),
                AllocateSlotError::ResourceExhausted(_)
            ));
        }
    }

    #[test]
    fn recurring_adoption_does_not_revalidate_rebuild_candidates() {
        let mut config = NetworkPoolConfig::new(3);
        config.target_ready_slots = 1;
        config.retained_ready_slots = 1;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET state = 'ready', health = 'unknown', lease_kind = ?1
                 WHERE slot_id = 0",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET state = 'creating', health = 'unknown', lease_kind = ?1
                 WHERE slot_id = 1",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET state = 'missing', health = 'degraded', lease_kind = ?1
                 WHERE slot_id = 2",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            let recurring = query_adoptable_ready_records(&conn, &config).unwrap();
            assert_eq!(recurring.len(), 1);
            assert_eq!(recurring[0].info.slot, 0);

            let startup = query_startup_recoverable_idle_records(&conn, &config).unwrap();
            assert_eq!(startup.len(), 2);
        }
    }

    #[test]
    fn startup_adoption_marks_invalid_recoverable_slot_for_rebuild() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 1;
        config.retained_ready_slots = 1;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let mut conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 3, state = 'resetting', health = 'unknown',
                     pod_netns_path = NULL, lease_kind = ?1,
                     cleanup_reset_attempts = 4, last_reset_retry_at = 42
                 WHERE slot_id = 0",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            apply_ready_validation_updates(
                &mut conn,
                &config,
                &[ReadyValidationUpdate::Degraded {
                    slot_id: 0,
                    generation: 3,
                    previous_state: SlotState::Resetting,
                    error: "missing namespace sns-000000".to_string(),
                }],
            )
            .unwrap();
        }

        let degraded = record(&pool, 0);
        assert_eq!(degraded.state, SlotState::Missing);
        assert_eq!(degraded.health, SlotHealth::Degraded);
        assert_eq!(
            degraded.last_error.as_deref(),
            Some("missing namespace sns-000000")
        );
        assert_eq!(cleanup_reset_attempts(&pool, 0), 0);
    }

    #[test]
    fn rebuild_claim_clears_stale_cleanup_retry_budget() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 1;
        config.retained_ready_slots = 1;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET state = 'missing', health = 'degraded',
                     cleanup_reset_attempts = 4, last_reset_retry_at = 42
                 WHERE slot_id = 0",
                [],
            )
            .unwrap();
        }

        assert_eq!(claim_missing_for_create(&pool, true), Some((0, 0)));

        let claimed = record(&pool, 0);
        assert_eq!(claimed.state, SlotState::Creating);
        assert_eq!(cleanup_reset_attempts(&pool, 0), 0);
    }

    #[test]
    fn startup_adoption_write_is_generation_fenced() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 1;
        config.retained_ready_slots = 1;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        {
            let mut conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 9, state = 'creating', health = 'unknown',
                     pod_netns_path = NULL, lease_kind = ?1
                 WHERE slot_id = 0",
                params![LeaseKind::Pooled.as_str()],
            )
            .unwrap();

            apply_ready_validation_updates(
                &mut conn,
                &config,
                &[ReadyValidationUpdate::Healthy {
                    slot_id: 0,
                    generation: 8,
                    previous_state: SlotState::Creating,
                }],
            )
            .unwrap();
        }

        let unchanged = record(&pool, 0);
        assert_eq!(unchanged.state, SlotState::Creating);
        assert_eq!(unchanged.health, SlotHealth::Unknown);
    }

    #[test]
    fn live_lease_health_validation_updates_are_generation_fenced() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 7, state = 'leased', health = 'unknown',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, lease_kind = ?2
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();

            apply_live_lease_validation_updates(
                &conn,
                &[LeaseHealthValidationUpdate {
                    slot_id: 0,
                    generation: 7,
                    sandbox_id: "sandbox-a".to_string(),
                    health: SlotHealth::Degraded,
                    last_error: Some("missing route for 10.12.128.1".to_string()),
                }],
            )
            .unwrap();

            apply_live_lease_validation_updates(
                &conn,
                &[LeaseHealthValidationUpdate {
                    slot_id: 0,
                    generation: 6,
                    sandbox_id: "sandbox-a".to_string(),
                    health: SlotHealth::Healthy,
                    last_error: None,
                }],
            )
            .unwrap();
        }

        let record = record(&pool, 0);
        assert_eq!(record.state, SlotState::Leased);
        assert_eq!(record.health, SlotHealth::Degraded);
        assert_eq!(
            record.last_error.as_deref(),
            Some("missing route for 10.12.128.1")
        );
    }

    #[test]
    fn cleanup_reset_attempts_are_persisted_in_slot_row() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        let reset_key = {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 5, state = 'resetting', health = 'unknown',
                     sandbox_id = 'sandbox-a', sandbox_uid = ?1, lease_kind = ?2
                 WHERE slot_id = 0",
                params![net::sandbox_uid_for_slot(0), LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
            ResetKey::from_record(&query_record_by_slot(&conn, 0).unwrap().unwrap())
        };

        {
            let conn = pool.db.lock().unwrap();
            assert_eq!(record_cleanup_reset_failure(&conn, &reset_key).unwrap(), 1);
            assert_eq!(record_cleanup_reset_failure(&conn, &reset_key).unwrap(), 2);
        }

        assert_eq!(cleanup_reset_attempts(&pool, 0), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fill_failure_waits_for_launched_setup_and_repair_work_before_retry() {
        let _guard = worker_hook_guard().await;
        set_test_host_ready_for_pool_fill(true);
        assert_fill_failure_waits_for_launched_work(false).await;
        assert_fill_failure_waits_for_launched_work(true).await;
        set_test_host_ready_for_pool_fill(false);
        set_test_create_slot_outcomes(Vec::new());
    }

    async fn assert_fill_failure_waits_for_launched_work(repair: bool) {
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        set_test_create_slot_outcomes(vec![
            TestCreateSlotOutcome {
                delay: Duration::from_millis(1),
                success: false,
                completed: std::sync::Arc::clone(&completed),
            },
            TestCreateSlotOutcome {
                delay: Duration::from_millis(20),
                success: true,
                completed: std::sync::Arc::clone(&completed),
            },
        ]);

        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 2;
        config.retained_ready_slots = 2;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        if repair {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET state = 'missing', health = 'degraded'
                 WHERE slot_id IN (0, 1)",
                [],
            )
            .unwrap();
        }

        fill_ready_slots(pool).await;
        assert_eq!(completed.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_recovery_db_failure_keeps_recovery_pending() {
        let _guard = worker_hook_guard().await;
        set_test_host_ready_for_pool_fill(true);
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        pool.startup_recovery_pending
            .store(true, std::sync::atomic::Ordering::SeqCst);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute("DROP TABLE network_slots", []).unwrap();
        }

        fill_ready_slots(pool.clone()).await;
        set_test_host_ready_for_pool_fill(false);

        assert!(
            pool.startup_recovery_pending
                .load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[test]
    fn cleanup_reset_db_failure_counter_reaches_quarantine_cap() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        let reset_key = ResetKey {
            slot_id: 0,
            generation: 1,
            sandbox_id: Some("sandbox-a".to_string()),
        };

        for attempt in 1..=MAX_SLOT_CLEANUP_RESET_RETRIES {
            assert_eq!(record_cleanup_reset_db_failure(&pool, &reset_key), attempt);
        }

        clear_cleanup_reset_db_failure(&pool, &reset_key);
        assert_eq!(record_cleanup_reset_db_failure(&pool, &reset_key), 1);
    }

    #[test]
    fn transient_validation_cap_degrades_idle_slot() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config.clone());
        let key = ValidationRetryKey {
            domain: ValidationRetryDomain::StartupIdle,
            slot_id: 0,
            generation: 0,
            sandbox_id: None,
        };

        for attempt in 1..MAX_SLOT_TRANSIENT_VALIDATION_RETRIES {
            assert!(matches!(
                transient_validation_decision(&pool, &key, "netlink timeout".to_string()),
                TransientValidationDecision::Retry { attempts } if attempts == attempt
            ));
        }
        let (attempts, error) =
            match transient_validation_decision(&pool, &key, "netlink timeout".to_string()) {
                TransientValidationDecision::Exhausted { attempts, error } => (attempts, error),
                TransientValidationDecision::Retry { .. } => {
                    panic!("transient validation should exhaust at retry cap")
                }
            };
        {
            let mut conn = pool.db.lock().unwrap();
            apply_ready_validation_updates(
                &mut conn,
                &config,
                &[ReadyValidationUpdate::Degraded {
                    slot_id: 0,
                    generation: 0,
                    previous_state: SlotState::Missing,
                    error: format!(
                        "transient validation failed after {attempts} attempts: {error}"
                    ),
                }],
            )
            .unwrap();
        }

        let degraded = record(&pool, 0);
        assert_eq!(degraded.state, SlotState::Missing);
        assert_eq!(degraded.health, SlotHealth::Degraded);
        assert!(degraded.last_error.unwrap().contains("netlink timeout"));
        assert!(!pool.validation_transient_failures.contains_key(&key));
    }

    #[test]
    fn validation_transient_failure_sweep_drops_stale_generations() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        let stale_key = ValidationRetryKey {
            domain: ValidationRetryDomain::StartupIdle,
            slot_id: 0,
            generation: 1,
            sandbox_id: None,
        };
        let current_key = ValidationRetryKey {
            domain: ValidationRetryDomain::StartupIdle,
            slot_id: 0,
            generation: 2,
            sandbox_id: None,
        };
        let other_domain_key = ValidationRetryKey {
            domain: ValidationRetryDomain::LiveLease,
            slot_id: 0,
            generation: 1,
            sandbox_id: Some("sandbox-a".to_string()),
        };

        assert_eq!(record_validation_transient_failure(&pool, &stale_key), 1);
        assert_eq!(record_validation_transient_failure(&pool, &current_key), 1);
        assert_eq!(
            record_validation_transient_failure(&pool, &other_domain_key),
            1
        );
        retain_validation_transient_failures(
            &pool,
            ValidationRetryDomain::StartupIdle,
            &HashSet::from([current_key.clone()]),
        );

        assert!(!pool.validation_transient_failures.contains_key(&stale_key));
        assert!(
            pool.validation_transient_failures
                .contains_key(&current_key)
        );
        assert!(
            pool.validation_transient_failures
                .contains_key(&other_domain_key)
        );
    }

    #[test]
    fn ephemeral_transient_setup_failure_retries_across_claim_generations() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        let key = SetupRetryKey::ephemeral(0);
        let error = EphemeralSetupError::Setup(net::NetworkSetupError::HostVeth {
            source: net::NetworkOpError::transient("netlink operation timed out"),
        });

        for attempt in 1..MAX_SLOT_TRANSIENT_SETUP_RETRIES {
            let claim = pool
                .claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission())
                .unwrap();
            let EphemeralSlotClaimResult::Claimed(claim) = claim else {
                panic!("expected dynamic claim");
            };
            assert_eq!(claim.info.generation, attempt as u64);

            pool.persist_ephemeral_setup_failure(&claim, key, &error)
                .unwrap();

            let retryable = record(&pool, 0);
            assert_eq!(retryable.state, SlotState::Missing);
            assert_eq!(retryable.health, SlotHealth::Degraded);
            assert_eq!(*pool.transient_setup_failures.get(&key).unwrap(), attempt);
        }

        let claim = pool
            .claim_ephemeral_slot_for_setup("sandbox-a", None, memory_admission())
            .unwrap();
        let EphemeralSlotClaimResult::Claimed(claim) = claim else {
            panic!("expected dynamic claim");
        };
        pool.persist_ephemeral_setup_failure(&claim, key, &error)
            .unwrap();

        let quarantined = record(&pool, 0);
        assert_eq!(quarantined.state, SlotState::Quarantined);
        assert_eq!(quarantined.health, SlotHealth::Invalid);
        assert!(
            quarantined
                .last_error
                .unwrap()
                .contains("exhausted transient retries")
        );
        assert!(!pool.transient_setup_failures.contains_key(&key));
    }

    #[test]
    fn claim_missing_for_create_sweeps_stale_setup_failure_keys() {
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);
        let stale_generation = SetupRetryKey::pooled(0, 1);
        let current_generation = SetupRetryKey::pooled(0, 2);
        let stale_ephemeral = SetupRetryKey::ephemeral(0);
        let other_slot = SetupRetryKey::pooled(1, 1);

        assert_eq!(
            record_transient_setup_failure_for_key(&pool, stale_generation),
            1
        );
        assert_eq!(
            record_transient_setup_failure_for_key(&pool, current_generation),
            1
        );
        assert_eq!(
            record_transient_setup_failure_for_key(&pool, stale_ephemeral),
            1
        );
        assert_eq!(record_transient_setup_failure_for_key(&pool, other_slot), 1);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 2, state = 'missing', health = 'unknown'
                 WHERE slot_id = 0",
                [],
            )
            .unwrap();
        }

        pool.update_config(NetworkPoolConfigUpdate {
            target_ready_slots: Some(1),
            retained_ready_slots: Some(1),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(claim_missing_for_create(&pool, false), Some((0, 2)));

        assert!(
            !pool
                .transient_setup_failures
                .contains_key(&stale_generation)
        );
        assert!(
            pool.transient_setup_failures
                .contains_key(&current_generation)
        );
        assert!(!pool.transient_setup_failures.contains_key(&stale_ephemeral));
        assert!(pool.transient_setup_failures.contains_key(&other_slot));
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn reset_retry_accounting_db_failure_schedules_retry_without_quarantine() {
        let sandbox_id = "sandbox-retry-accounting-db-failure";
        let temp_root = std::env::temp_dir().join(format!(
            "space-slot-pool-retry-accounting-db-failure-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let sandbox_cache = temp_root.join(sandbox_id);
        std::fs::create_dir_all(&sandbox_cache).unwrap();
        let retained_file = sandbox_cache.join("owned-by-old-uid");
        std::fs::write(&retained_file, b"stale cache").unwrap();
        let lease_uid = std::fs::symlink_metadata(&retained_file).unwrap().uid();

        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        config.sandbox_cache_dir = temp_root.clone();
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'dirty', health = 'unknown',
                     sandbox_id = ?1, sandbox_uid = ?2, pid = NULL, lease_kind = ?3
                 WHERE slot_id = 0",
                params![sandbox_id, lease_uid, LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
            conn.execute(
                "ALTER TABLE network_slots DROP COLUMN cleanup_reset_attempts",
                [],
            )
            .unwrap();
        }

        reset_slot(std::sync::Arc::clone(&pool), 0).await;

        let stale = record(&pool, 0);
        assert_eq!(stale.state, SlotState::Resetting);
        assert_eq!(stale.health, SlotHealth::Unknown);
        assert!(
            reset_candidates(&pool, 0)
                .unwrap()
                .entries
                .contains(&(0, 1))
        );

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn reset_slot_keeps_dirty_when_retained_cache_is_still_owned_by_lease_uid() {
        let sandbox_id = "sandbox-retained-cache";
        let temp_root = std::env::temp_dir().join(format!(
            "space-slot-pool-cache-gate-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let sandbox_cache = temp_root.join(sandbox_id);
        std::fs::create_dir_all(&sandbox_cache).unwrap();
        let retained_file = sandbox_cache.join("owned-by-old-uid");
        std::fs::write(&retained_file, b"stale cache").unwrap();
        let lease_uid = std::fs::symlink_metadata(&retained_file).unwrap().uid();

        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        config.sandbox_cache_dir = temp_root.clone();
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'dirty', health = 'unknown',
                     sandbox_id = ?1, sandbox_uid = ?2, pid = NULL, lease_kind = ?3
                 WHERE slot_id = 0",
                params![sandbox_id, lease_uid, LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
        }

        reset_slot(std::sync::Arc::clone(&pool), 0).await;

        let stale = record(&pool, 0);
        assert_eq!(stale.state, SlotState::Dirty);
        assert_eq!(stale.health, SlotHealth::Degraded);
        assert_eq!(stale.info.sandbox_id, sandbox_id);
        assert_eq!(stale.info.sandbox_uid, lease_uid);
        let error = stale
            .last_error
            .as_deref()
            .expect("expected retained-cache cleanup error");
        assert!(error.contains("cleanup required before resetting slot 0 generation 1"));
        assert!(error.contains(sandbox_id));
        assert!(error.contains(&format!("sandbox uid {lease_uid}")));
        assert!(error.contains(&sandbox_cache.display().to_string()));

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "current_thread")]
    async fn reset_slot_stops_waiting_for_sandbox_runtime_once_it_runs_on_a_newer_slot() {
        let sandbox_id = SandboxId::new().to_string();
        // The sandbox's cgroups and fc.pid show a running VM, and nothing
        // remains of the released slot's own runtime.
        super::liveness::set_test_runtime_evidence(
            &sandbox_id,
            super::liveness::TestRuntimeEvidence {
                sandbox: LeaseLiveness::Live,
                slot: None,
            },
        );
        // A file owned by the released lease UID stops the reset just after
        // the runtime check, so `last_error` shows which gate it reached.
        let cache_root = tempfile::tempdir().unwrap();
        let retained = cache_root.path().join(&sandbox_id);
        std::fs::create_dir_all(&retained).unwrap();
        std::fs::write(retained.join("owned-by-old-uid"), b"stale cache").unwrap();
        let lease_uid = std::fs::metadata(retained.join("owned-by-old-uid"))
            .unwrap()
            .uid();
        let mut config = NetworkPoolConfig::new(2);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        config.sandbox_cache_dir = cache_root.path().to_path_buf();
        let pool = test_pool(config);
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

        // With no newer lease that VM may still be the released lease's.
        reset_slot(pool.clone(), 0).await;
        let waiting = record(&pool, 0);
        assert_eq!(waiting.state, SlotState::Dirty);
        let error = waiting.last_error.as_deref().unwrap();
        assert!(error.contains("runtime is Live"), "{error}");

        // A wake on this node leases the sandbox a fresh slot, whose VM now
        // owns the sandbox-wide evidence.
        let EphemeralSlotClaimResult::Claimed(successor) = pool
            .claim_ephemeral_slot_for_setup(&sandbox_id, None, memory_admission())
            .unwrap()
        else {
            panic!("expected a fresh claim while the previous slot resets");
        };
        assert_eq!(successor.info.slot, 1);

        reset_slot(pool.clone(), 0).await;
        let error = record(&pool, 0).last_error.unwrap();
        assert!(
            error.contains("cleanup required before resetting slot 0 generation 1"),
            "{error}"
        );
        assert_eq!(record(&pool, 1).state, SlotState::Leasing);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    async fn reset_slot_quarantines_after_cleanup_retry_exhaustion() {
        let sandbox_id = "sandbox-retained-cache-exhaustion";
        let temp_root = std::env::temp_dir().join(format!(
            "space-slot-pool-cache-exhaustion-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let sandbox_cache = temp_root.join(sandbox_id);
        std::fs::create_dir_all(&sandbox_cache).unwrap();
        let retained_file = sandbox_cache.join("owned-by-old-uid");
        std::fs::write(&retained_file, b"stale cache").unwrap();
        let lease_uid = std::fs::symlink_metadata(&retained_file).unwrap().uid();

        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        config.sandbox_cache_dir = temp_root.clone();
        let pool = test_pool(config);
        {
            let conn = pool.db.lock().unwrap();
            conn.execute(
                "UPDATE network_slots
                 SET generation = 1, state = 'dirty', health = 'unknown',
                     sandbox_id = ?1, sandbox_uid = ?2, pid = NULL, lease_kind = ?3
                 WHERE slot_id = 0",
                params![sandbox_id, lease_uid, LeaseKind::Ephemeral.as_str()],
            )
            .unwrap();
        }

        for _ in 1..MAX_SLOT_CLEANUP_RESET_RETRIES {
            reset_slot(std::sync::Arc::clone(&pool), 0).await;
            let stale = record(&pool, 0);
            assert_eq!(stale.state, SlotState::Dirty);
            assert_eq!(stale.health, SlotHealth::Degraded);
        }

        reset_slot(std::sync::Arc::clone(&pool), 0).await;

        let quarantined = record(&pool, 0);
        let expected_error = format!(
            "network slot reset exhausted cleanup retries after {MAX_SLOT_CLEANUP_RESET_RETRIES} attempts: cleanup required before resetting slot 0 generation 1 for sandbox {sandbox_id}: retained cache artifact is still owned by sandbox uid {lease_uid}: {}",
            sandbox_cache.display()
        );
        assert_eq!(quarantined.state, SlotState::Quarantined);
        assert_eq!(quarantined.health, SlotHealth::Invalid);
        assert_eq!(quarantined.info.sandbox_id, sandbox_id);
        assert_eq!(quarantined.info.sandbox_uid, lease_uid);
        assert_eq!(quarantined.info.pid, None);
        assert_eq!(
            quarantined.last_error.as_deref(),
            Some(expected_error.as_str())
        );

        let _ = std::fs::remove_dir_all(temp_root);
    }

    #[test]
    fn set_pid_fails_without_active_lease() {
        let mut config = NetworkPoolConfig::new(1);
        config.target_ready_slots = 0;
        config.retained_ready_slots = 0;
        config.min_ready_slots = 0;
        let pool = test_pool(config);

        assert!(pool.set_pid("missing-sandbox", 1).is_err());
    }
}

/// A malformed packaged policy fails identically for every namespace on the
/// node, so it must fence the slot and retry rather than quarantine it: a
/// rebuild cannot repair a bad file, and quarantining takes the pool to zero.
#[test]
fn malformed_packaged_policy_fences_the_slot_instead_of_quarantining_it() {
    let parse = serde_json::from_str::<Vec<serde_json::Value>>("[{\"add\":]")
        .expect_err("a trailing comma must not parse");
    let error = super::reset::SlotValidationError::PackagedPolicy(
        crate::firewall::PackagedPolicyError(parse),
    );

    assert!(
        error.is_transient(),
        "a host-level policy fault must be retried, not quarantined: {error}"
    );
}

use super::*;

#[tokio::test]
async fn idempotency_store_rejects_expired_keys() {
    let store = IdempotencyStore::new("test");
    store.insert_expired(
        IdempotencyKey::parse("key").unwrap(),
        IdempotencyRecord::ProcessStarted {
            process_id: ProcessId::new(),
        },
    );
    let error = match store
        .reserve_or_get(IdempotencyKey::parse("key").unwrap())
        .await
    {
        Ok(_) => panic!("expired key should fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(error.message(), "idempotency_key expired");
}

#[tokio::test]
async fn idempotency_store_rejects_new_keys_at_record_limit() {
    let store = IdempotencyStore::new("test");
    for index in 0..MAX_IDEMPOTENCY_RECORDS {
        let key = IdempotencyKey::parse(format!("key-{index}")).unwrap();
        match store.reserve_or_get(key).await.unwrap() {
            IdempotencyLookup::Reserved(reservation) => {
                reservation
                    .begin()
                    .unwrap()
                    .complete(Ok(IdempotencyRecord::ProcessStarted {
                        process_id: ProcessId::new(),
                    }));
            }
            IdempotencyLookup::Completed(_) => panic!("new key unexpectedly completed"),
        }
    }

    let error = match store
        .reserve_or_get(IdempotencyKey::parse("one-too-many").unwrap())
        .await
    {
        Ok(_) => panic!("store should reject a new key at its record limit"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));
}

#[test]
fn idempotency_store_enforces_aggregate_key_byte_limit() {
    let key = IdempotencyKey::parse("new-key").unwrap();
    let mut state = IdempotencyState {
        key_bytes: MAX_IDEMPOTENCY_KEY_BYTES - key.as_str().len(),
        ..IdempotencyState::default()
    };
    assert!(state.can_insert(&key));
    state.key_bytes += 1;
    assert!(!state.can_insert(&key));
}

#[tokio::test]
async fn idempotency_store_evicts_tombstones_before_rejecting_new_work() {
    let store = IdempotencyStore::new("test");
    let now = Instant::now();
    {
        let mut state = store.state();
        for index in 0..MAX_IDEMPOTENCY_RECORDS {
            state.insert(
                IdempotencyKey::parse(format!("expired-{index}")).unwrap(),
                StoredIdempotencyRecord::Expired {
                    expired_at: now,
                    in_progress_generation: None,
                },
            );
        }
    }

    assert!(matches!(
        store
            .reserve_or_get(IdempotencyKey::parse("new-work").unwrap())
            .await
            .unwrap(),
        IdempotencyLookup::Reserved(_)
    ));
}

#[tokio::test]
async fn old_reservation_cannot_complete_a_reused_key() {
    let store = IdempotencyStore::new("test");
    let key = IdempotencyKey::parse("reused-key").unwrap();
    let old = match store.reserve_or_get(key.clone()).await.unwrap() {
        IdempotencyLookup::Reserved(reservation) => reservation.begin().unwrap(),
        IdempotencyLookup::Completed(_) => panic!("new key unexpectedly completed"),
    };

    {
        let mut state = store.state();
        state.records.insert(
            key.clone(),
            StoredIdempotencyRecord::Expired {
                expired_at: Instant::now() - IDEMPOTENCY_RECORD_TTL - Duration::from_secs(1),
                in_progress_generation: None,
            },
        );
        for index in 0..(MAX_IDEMPOTENCY_RECORDS - 1) {
            state.insert(
                IdempotencyKey::parse(format!("newer-expired-{index}")).unwrap(),
                StoredIdempotencyRecord::Expired {
                    expired_at: Instant::now(),
                    in_progress_generation: Some(Arc::new(Notify::new())),
                },
            );
        }
    }

    let pressure_key = IdempotencyKey::parse("capacity-pressure").unwrap();
    assert!(matches!(
        store.reserve_or_get(pressure_key.clone()).await.unwrap(),
        IdempotencyLookup::Reserved(_)
    ));
    store.state().records.insert(
        pressure_key,
        StoredIdempotencyRecord::Expired {
            expired_at: Instant::now(),
            in_progress_generation: None,
        },
    );

    let new = match store.reserve_or_get(key.clone()).await.unwrap() {
        IdempotencyLookup::Reserved(reservation) => reservation.begin().unwrap(),
        IdempotencyLookup::Completed(_) => panic!("reclaimed key unexpectedly completed"),
    };
    old.complete(Ok(IdempotencyRecord::ProcessStarted {
        process_id: ProcessId::new(),
    }));

    {
        let state = store.state();
        let StoredIdempotencyRecord::InProgress { notify, .. } = state
            .records
            .get(&key)
            .expect("new reservation disappeared")
        else {
            panic!("old reservation completed the new generation");
        };
        assert!(Arc::ptr_eq(notify, &new.claim.as_ref().unwrap().generation));
    }

    new.complete(Ok(IdempotencyRecord::ProcessStarted {
        process_id: ProcessId::parse("new-generation").unwrap(),
    }));
    match store.reserve_or_get(key).await.unwrap() {
        IdempotencyLookup::Completed(Ok(IdempotencyRecord::ProcessStarted { process_id })) => {
            assert_eq!(process_id.as_str(), "new-generation");
        }
        _ => panic!("new reservation result was not retained"),
    }
}

#[tokio::test]
async fn capacity_pressure_reclaims_an_expired_operation_after_its_reservation_is_dropped() {
    let store = IdempotencyStore::new("test");
    let abandoned_key = IdempotencyKey::parse("abandoned").unwrap();
    let reservation = match store.reserve_or_get(abandoned_key.clone()).await.unwrap() {
        IdempotencyLookup::Reserved(reservation) => reservation.begin().unwrap(),
        IdempotencyLookup::Completed(_) => panic!("new key unexpectedly completed"),
    };
    let now = Instant::now();
    {
        let mut state = store.state();
        let Some(StoredIdempotencyRecord::InProgress { created_at, .. }) =
            state.records.get_mut(&abandoned_key)
        else {
            panic!("reservation disappeared");
        };
        *created_at = now - IDEMPOTENCY_RECORD_TTL - Duration::from_secs(1);
        state.prune(now);
        assert!(matches!(
            state.records.get(&abandoned_key),
            Some(StoredIdempotencyRecord::Expired {
                in_progress_generation: Some(_),
                ..
            })
        ));
    }

    drop(reservation);
    {
        let mut state = store.state();
        assert!(matches!(
            state.records.get(&abandoned_key),
            Some(StoredIdempotencyRecord::Expired {
                in_progress_generation: None,
                ..
            })
        ));
        for index in 0..(MAX_IDEMPOTENCY_RECORDS - 1) {
            state.insert(
                IdempotencyKey::parse(format!("protected-{index}")).unwrap(),
                StoredIdempotencyRecord::Expired {
                    expired_at: now,
                    in_progress_generation: Some(Arc::new(Notify::new())),
                },
            );
        }
    }

    let replacement_key = IdempotencyKey::parse("replacement").unwrap();
    let replacement = store.reserve_or_get(replacement_key.clone()).await.unwrap();
    assert!(matches!(replacement, IdempotencyLookup::Reserved(_)));
    let state = store.state();
    assert!(!state.records.contains_key(&abandoned_key));
    assert!(state.records.contains_key(&replacement_key));
}

#[tokio::test]
async fn ownership_transitions_wake_waiters_without_repeating_uncertain_effects() {
    enum Finish {
        BeforeEffect,
        Interrupted,
        Success,
        Failure,
    }
    for finish in [
        Finish::BeforeEffect,
        Finish::Interrupted,
        Finish::Success,
        Finish::Failure,
    ] {
        let store = IdempotencyStore::new("test");
        let key = IdempotencyKey::parse("operation").unwrap();
        let reservation = match store.reserve_or_get(key.clone()).await.unwrap() {
            IdempotencyLookup::Reserved(reservation) => reservation,
            IdempotencyLookup::Completed(_) => panic!("new key unexpectedly completed"),
        };
        let mut waiter = Box::pin(store.reserve_or_get(key.clone()));
        assert!(futures_util::poll!(waiter.as_mut()).is_pending());
        match finish {
            Finish::BeforeEffect => drop(reservation),
            Finish::Interrupted => drop(reservation.begin().unwrap()),
            Finish::Success => {
                reservation
                    .begin()
                    .unwrap()
                    .complete(Ok(IdempotencyRecord::ProcessStarted {
                        process_id: ProcessId::new(),
                    }))
            }
            Finish::Failure => reservation
                .begin()
                .unwrap()
                .complete(Err(Status::permission_denied("denied"))),
        }
        let result = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("terminal transition failed to wake an existing waiter")
            .expect("lookup succeeds");
        match (finish, result) {
            (Finish::BeforeEffect, IdempotencyLookup::Reserved(reservation)) => {
                drop(reservation);
                assert_eq!(store.state().key_bytes, 0);
            }
            (Finish::Interrupted, IdempotencyLookup::Completed(Err(status))) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert_eq!(status.message(), "operation interrupted; outcome unknown");
            }
            (
                Finish::Success,
                IdempotencyLookup::Completed(Ok(IdempotencyRecord::ProcessStarted { .. })),
            ) => {}
            (Finish::Failure, IdempotencyLookup::Completed(Err(status))) => {
                assert_eq!(status.code(), tonic::Code::PermissionDenied);
                assert_eq!(status.message(), "denied");
            }
            _ => panic!("ownership transition produced the wrong retry policy"),
        }
    }
}

#[tokio::test]
async fn expired_reservations_cannot_begin_an_effect() {
    for prune in [false, true] {
        let store = IdempotencyStore::new("test");
        let key = IdempotencyKey::parse("expired-before-effect").unwrap();
        let reservation = match store.reserve_or_get(key.clone()).await.unwrap() {
            IdempotencyLookup::Reserved(reservation) => reservation,
            IdempotencyLookup::Completed(_) => panic!("new key unexpectedly completed"),
        };
        {
            let mut state = store.state();
            let Some(StoredIdempotencyRecord::InProgress { created_at, .. }) =
                state.records.get_mut(&key)
            else {
                panic!("reservation disappeared");
            };
            *created_at = Instant::now() - IDEMPOTENCY_RECORD_TTL - Duration::from_secs(1);
            if prune {
                state.prune(Instant::now());
            }
        }
        let status = match reservation.begin() {
            Err(status) => status,
            Ok(_) => panic!("expired reservation allowed an effect"),
        };
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(matches!(
            store.reserve_or_get(key).await.unwrap(),
            IdempotencyLookup::Reserved(_)
        ));
    }
}

#[tokio::test]
async fn capacity_pressure_cannot_reuse_an_expired_operation_that_is_still_running() {
    let store = IdempotencyStore::new("test");
    let key = IdempotencyKey::parse("still-running").unwrap();
    let reservation = match store.reserve_or_get(key.clone()).await.unwrap() {
        IdempotencyLookup::Reserved(reservation) => reservation.begin().unwrap(),
        IdempotencyLookup::Completed(_) => panic!("new key unexpectedly completed"),
    };
    let now = Instant::now();
    {
        let mut state = store.state();
        let Some(StoredIdempotencyRecord::InProgress { created_at, .. }) =
            state.records.get_mut(&key)
        else {
            panic!("reservation disappeared");
        };
        *created_at = now - IDEMPOTENCY_RECORD_TTL - Duration::from_secs(1);
        state.prune(now);
        for index in 0..(MAX_IDEMPOTENCY_RECORDS - 1) {
            state.insert(
                IdempotencyKey::parse(format!("expired-{index}")).unwrap(),
                StoredIdempotencyRecord::Expired {
                    expired_at: now,
                    in_progress_generation: None,
                },
            );
        }
    }

    assert!(matches!(
        store
            .reserve_or_get(IdempotencyKey::parse("capacity-pressure").unwrap())
            .await
            .unwrap(),
        IdempotencyLookup::Reserved(_)
    ));
    let error = match store.reserve_or_get(key.clone()).await {
        Ok(_) => panic!("an expired operation was allowed to execute twice"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    reservation.complete(Ok(IdempotencyRecord::ProcessStarted {
        process_id: ProcessId::new(),
    }));
    let state = store.state();
    assert!(matches!(
        state.records.get(&key),
        Some(StoredIdempotencyRecord::Expired {
            in_progress_generation: None,
            ..
        })
    ));
}

#[tokio::test]
async fn duplicate_waiters_are_bounded_and_release_slots() {
    let store = IdempotencyStore::with_lookup_limit(1);
    let key = IdempotencyKey::parse("bounded-waiters").unwrap();
    let reservation = match store.reserve_or_get(key.clone()).await.unwrap() {
        IdempotencyLookup::Reserved(reservation) => reservation.begin().unwrap(),
        IdempotencyLookup::Completed(_) => panic!("new key unexpectedly completed"),
    };

    let waiter = tokio::spawn({
        let store = store.clone();
        let key = key.clone();
        async move { store.reserve_or_get(key).await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.lookup_slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("duplicate waiter did not acquire its slot");

    let error = match store.reserve_or_get(key.clone()).await {
        Ok(_) => panic!("lookup above the waiter limit should fail"),
        Err(error) => error,
    };
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    assert!(pplx_space_common_grpc::is_refused_before_start(&error));

    waiter.abort();
    let _ = waiter.await;
    assert_eq!(store.lookup_slots.available_permits(), 1);

    let completed_waiter = tokio::spawn({
        let store = store.clone();
        async move { store.reserve_or_get(key).await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.lookup_slots.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("replacement waiter did not acquire its slot");
    reservation.complete(Ok(IdempotencyRecord::ProcessStarted {
        process_id: ProcessId::new(),
    }));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), completed_waiter)
            .await
            .expect("completed waiter did not wake")
            .expect("completed waiter task panicked")
            .expect("completed waiter returned an error"),
        IdempotencyLookup::Completed(Ok(IdempotencyRecord::ProcessStarted { .. }))
    ));
    assert_eq!(store.lookup_slots.available_permits(), 1);
}

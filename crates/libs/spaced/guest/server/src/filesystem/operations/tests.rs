use super::*;

fn identity(name: &str) -> GuestUserName {
    GuestUserName::parse(name).expect("identity")
}
fn key() -> Option<IdempotencyKey> {
    Some(IdempotencyKey::parse("operation").expect("key"))
}

async fn claim(store: &Operations) -> Reservation {
    match store
        .reserve(identity("root"), key(), Kind::Write)
        .await
        .expect("lookup")
    {
        Lookup::New(claim) => claim,
        Lookup::Replay(_) => panic!("expected new reservation"),
    }
}

#[tokio::test]
async fn ownership_transition_matrix() {
    for dispatched in [false, true] {
        let store = Operations::new();
        let reservation = claim(&store).await;
        if dispatched {
            drop(reservation.dispatch());
        } else {
            drop(reservation);
        }
        match store
            .reserve(identity("root"), key(), Kind::Write)
            .await
            .expect("retry")
        {
            Lookup::New(_) => assert!(!dispatched),
            Lookup::Replay(result) => {
                assert!(dispatched);
                assert_eq!(
                    result.expect_err("unknown").code(),
                    tonic::Code::FailedPrecondition
                );
            }
        }
    }
}

#[tokio::test]
async fn terminal_outcomes_wake_duplicates_and_check_kind_before_errors() {
    for success in [false, true] {
        let store = Operations::new();
        let execution = claim(&store).await.dispatch();
        let mut duplicate = Box::pin(store.reserve(identity("root"), key(), Kind::Write));
        assert!(futures_util::poll!(&mut duplicate).is_pending());
        let result = if success {
            Ok(outcome(Ok(mutation_result::Outcome::Write(
                pplx_space_spaced_common::proto::operations::WriteResponse { bytes_written: 4 },
            ))))
        } else {
            Err(unknown_outcome())
        };
        execution.complete(result.clone());
        let Lookup::Replay(replayed) = duplicate.await.expect("duplicate") else {
            panic!("duplicate executed");
        };
        assert_eq!(replayed.is_ok(), success);
        assert!(
            store
                .reserve(identity("root"), key(), Kind::Rename)
                .await
                .is_err()
        );
        assert!(matches!(
            store
                .reserve(identity("user"), key(), Kind::Write)
                .await
                .expect("separate identity"),
            Lookup::New(_)
        ));
    }
}

#[tokio::test]
async fn only_explicit_pre_execution_rejection_releases_waiting_duplicates() {
    for rejected in [true, false] {
        let store = Operations::new();
        let execution = claim(&store).await.dispatch();
        let mut duplicate = Box::pin(store.reserve(identity("root"), key(), Kind::Write));
        assert!(futures_util::poll!(&mut duplicate).is_pending());
        let failure = OperationFailure {
            code: tonic::Code::ResourceExhausted as i32,
            message: "capacity exhausted".into(),
            reason: String::new(),
        };
        execution.complete(Ok(MutationResult {
            outcome: Some(if rejected {
                mutation_result::Outcome::RejectedBeforeExecution(failure)
            } else {
                mutation_result::Outcome::Failure(failure)
            }),
        }));
        match duplicate.await.expect("duplicate wakes") {
            Lookup::New(_) => assert!(rejected, "partial effects must remain fenced"),
            Lookup::Replay(result) => {
                assert!(!rejected, "capacity rejection must not be retained");
                assert_eq!(
                    checked_outcome(result.expect("terminal reply"))
                        .expect_err("failure")
                        .code(),
                    tonic::Code::ResourceExhausted
                );
            }
        }
        let state = store.state.lock().expect("state");
        assert_eq!(state.records.len(), usize::from(!rejected));
        assert_eq!(
            state.bytes,
            state.records.keys().map(key_bytes).sum::<usize>()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn replay_expiry_does_not_evict_active_executions() {
    let store = Operations::new();
    let execution = claim(&store).await.dispatch();
    let mut duplicate = Box::pin(store.reserve(identity("root"), key(), Kind::Write));
    assert!(futures_util::poll!(&mut duplicate).is_pending());
    tokio::time::advance(RETENTION + Duration::from_secs(1)).await;
    assert!(
        matches!(duplicate.await, Err(status) if status.code() == tonic::Code::DeadlineExceeded)
    );
    let mut retry = Box::pin(store.reserve(identity("root"), key(), Kind::Write));
    assert!(
        futures_util::poll!(&mut retry).is_pending(),
        "waiter expiry must not release the running execution"
    );
    drop(retry);
    execution.complete(Err(unknown_outcome()));
    {
        let mut state = store.state.lock().expect("state");
        let record = state.records.values_mut().next().expect("record");
        record.result.as_mut().expect("completed").1 = Instant::now() - RETENTION;
    }
    assert!(matches!(
        store
            .reserve(identity("root"), key(), Kind::Write)
            .await
            .expect("expired lookup"),
        Lookup::New(_)
    ));
}

#[tokio::test]
async fn capacity_preserves_active_and_unexpired_terminal_records() {
    let store = Operations::new();
    let running = claim(&store).await.dispatch();
    for index in 1..MAX_RECORDS {
        let key = Some(IdempotencyKey::parse(format!("completed-{index}")).expect("key"));
        let Lookup::New(reservation) = store
            .reserve(identity("root"), key, Kind::Write)
            .await
            .expect("capacity")
        else {
            panic!("unexpected replay");
        };
        reservation.dispatch().complete(Err(unknown_outcome()));
    }
    let extra = Some(IdempotencyKey::parse("extra").expect("key"));
    // Nothing is recorded for the refused key: the caller may retry it.
    assert!(
        matches!(store.reserve(identity("root"), extra.clone(), Kind::Write).await, Err(status) if status.code() == tonic::Code::ResourceExhausted && pplx_space_common_grpc::is_refused_before_start(&status))
    );
    {
        let mut state = store.state.lock().expect("state");
        for record in state.records.values_mut() {
            if let Some((_, at)) = &mut record.result {
                *at = Instant::now() - RETENTION;
            }
        }
        state.last_pruned = None;
    }
    assert!(matches!(
        store
            .reserve(identity("root"), extra, Kind::Write)
            .await
            .expect("expired records reclaimed"),
        Lookup::New(_)
    ));
    let mut retry = Box::pin(store.reserve(identity("root"), key(), Kind::Write));
    assert!(
        futures_util::poll!(&mut retry).is_pending(),
        "capacity pressure must not evict an active generation"
    );
    running.complete(Err(unknown_outcome()));
    assert!(matches!(
        retry.await.expect("retry"),
        Lookup::Replay(Err(_))
    ));
}

#[tokio::test]
async fn poisoned_accounting_preserves_settled_and_active_retry_fences() {
    for active in [false, true] {
        let store = Operations::new();
        let execution = claim(&store).await.dispatch();
        let execution = if active {
            Some(execution)
        } else {
            execution.complete(Err(unknown_outcome()));
            None
        };
        let poisoned = store.state.clone();
        std::thread::spawn(move || {
            let mut state = poisoned.lock().expect("state");
            state.bytes = usize::MAX;
            panic!("interrupted accounting update");
        })
        .join()
        .expect_err("poisoned");
        drop(execution);
        let Lookup::Replay(result) = store
            .reserve(identity("root"), key(), Kind::Write)
            .await
            .expect("retry")
        else {
            panic!("poisoning must not authorize duplicate execution");
        };
        assert_eq!(
            result.expect_err("unknown outcome").code(),
            tonic::Code::FailedPrecondition
        );
        assert!(matches!(
            store
                .reserve(identity("user"), key(), Kind::Write)
                .await
                .expect("accounting repaired"),
            Lookup::New(_)
        ));
    }
}

// A refusal relayed from a worker keeps its marker and gets its retry hint
// back; a validation failure rejected before execution is not a refusal.
#[test]
fn relayed_refusals_keep_their_marker_and_retry_hint() {
    let refused = crate::refused(tonic::Status::resource_exhausted(
        "too many concurrent filesystem file operations",
    ));
    let status = checked_outcome(MutationResult {
        outcome: Some(mutation_result::Outcome::RejectedBeforeExecution(
            operation_failure(&refused),
        )),
    })
    .expect_err("refusal");
    assert!(pplx_space_common_grpc::is_refused_before_start(&status));
    assert!(pplx_space_common_grpc::retry_after(&status).is_some());

    let invalid = checked_outcome(MutationResult {
        outcome: Some(mutation_result::Outcome::RejectedBeforeExecution(
            operation_failure(&tonic::Status::invalid_argument("bad path")),
        )),
    })
    .expect_err("validation failure");
    assert!(!pplx_space_common_grpc::is_refused_before_start(&invalid));
    assert!(pplx_space_common_grpc::retry_after(&invalid).is_none());
}

use super::*;
fn sequence(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}
fn owner(ledger: &Arc<Ledger>, value: u64, binding: Binding) -> Owner {
    match ledger.reserve(sequence(value), binding).unwrap() {
        Reservation::Owner(owner) => owner,
        _ => panic!("new sequence has one owner"),
    }
}
#[tokio::test]
async fn receipts_bind_order_and_survive_owner_loss_without_reapplying() {
    let ledger = Arc::new(Ledger::default());
    let unstarted = owner(&ledger, 1, Binding::Close);
    let Reservation::Wait(waiter) = ledger.reserve(sequence(1), Binding::Close).unwrap() else {
        panic!("waiter")
    };
    drop(unstarted);
    assert!(pplx_space_common_grpc::is_refused_before_start(
        &wait(waiter).await.result().unwrap_err()
    ));
    // Unbegun cancellation leaves the sequence available; begun loss consumes it.
    let mut first = owner(&ledger, 1, Binding::Input([1; 32]));
    first.begin();
    for binding in [Binding::Input([2; 32]), Binding::Close, Binding::Signal(2)] {
        assert_eq!(
            ledger.reserve(sequence(1), binding).err().unwrap().code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(
        RecoveryReason::from_status(&ledger.reserve(sequence(2), Binding::Close).err().unwrap()),
        Some(RecoveryReason::MutationOutOfOrder)
    );
    let Reservation::Wait(waiter) = ledger
        .reserve(sequence(1), Binding::Input([1; 32]))
        .unwrap()
    else {
        panic!("waiter")
    };
    drop(first);
    assert_eq!(
        RecoveryReason::from_status(&wait(waiter).await.result().unwrap_err()),
        Some(RecoveryReason::MutationOutcomeUnknown)
    );
    let Reservation::Replay(outcome) = ledger
        .reserve(sequence(1), Binding::Input([1; 32]))
        .unwrap()
    else {
        panic!("receipt")
    };
    assert_eq!(
        RecoveryReason::from_status(&outcome.result().unwrap_err()),
        Some(RecoveryReason::MutationOutcomeUnknown)
    );
    for value in 2..=65 {
        let mut next = owner(&ledger, value, Binding::Close);
        next.begin();
        next.complete(Outcome::Applied);
    }
    assert_eq!(
        RecoveryReason::from_status(
            &ledger
                .reserve(sequence(1), Binding::Input([1; 32]))
                .err()
                .unwrap()
        ),
        Some(RecoveryReason::MutationReceiptExpired)
    );
    for value in 2..=65 {
        let Reservation::Replay(outcome) = ledger.reserve(sequence(value), Binding::Close).unwrap()
        else {
            panic!("retained receipt")
        };
        outcome.result().unwrap();
    }
    // The final integer is usable once; it never wraps to another operation.
    ledger.0.lock().unwrap().high_water = u64::MAX - 1;
    let mut last = owner(&ledger, u64::MAX, Binding::Close);
    last.begin();
    last.complete(Outcome::Applied);
    assert_eq!(
        RecoveryReason::from_status(&ledger.reserve(sequence(1), Binding::Close).err().unwrap()),
        Some(RecoveryReason::MutationReceiptExpired)
    );
}

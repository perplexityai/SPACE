use super::*;

use pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER;

const SANDBOX: &str = "sandbox-1";

/// Sealing again under the current epoch changes nothing; a later epoch
/// replaces it with a later seal; an earlier epoch is refused, to seal or to
/// cancel, and leaves the seal as it was; a zero or unstorable epoch is
/// refused.
#[test]
fn seal_epochs_only_move_forward() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());

    let first = seal.seal(&db, 7).unwrap();
    assert_eq!(first.epoch, 7);
    std::thread::sleep(Duration::from_millis(2));
    assert_eq!(seal.seal(&db, 7).unwrap(), first, "re-seal is idempotent");
    assert_eq!(seal.status().sealed, Some(first));

    let replaced = seal.seal(&db, 8).unwrap();
    assert_eq!(replaced.epoch, 8);
    assert!(
        replaced.since_ms > first.since_ms,
        "a replacing epoch starts a new seal: {replaced:?} after {first:?}"
    );

    let written = Some(ActivationSealRecord {
        sealed_epoch: Some(8),
        sealed_since_ms: replaced.since_ms,
        last_cancelled_epoch: None,
    });
    for refused in [
        seal.seal(&db, 7).unwrap_err(),
        seal.unseal(&db, 7).unwrap_err(),
    ] {
        assert_eq!(
            refused.code(),
            tonic::Code::FailedPrecondition,
            "{refused:?}"
        );
        assert_eq!(seal.status().sealed, Some(replaced));
        assert_eq!(
            crate::db::read::activation_seal(&db.lock().unwrap()).unwrap(),
            written,
            "a refused call writes nothing"
        );
    }

    for epoch in [0, u64::MAX] {
        assert_eq!(
            seal.seal(&db, epoch).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            seal.unseal(&db, epoch).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(seal.status().sealed, Some(replaced));
}

/// A cancel of the current epoch unseals and is remembered: no seal at or
/// below it is taken again, while a later epoch seals. A cancel that arrives
/// before its seal (an undrain racing the drain's first call) still refuses
/// that seal, and a later cancel unseals an earlier seal.
#[test]
fn a_cancelled_epoch_never_seals_again() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());

    seal.seal(&db, 5).unwrap();
    seal.unseal(&db, 5).unwrap();
    assert_eq!(seal.status().sealed, None);
    assert_eq!(seal.last_cancelled_epoch(), Some(5));
    seal.unseal(&db, 5).unwrap();
    for stale in [4, 5] {
        assert_eq!(
            seal.seal(&db, stale).unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "{stale}"
        );
    }
    assert_eq!(seal.status().sealed, None);

    seal.unseal(&db, 9).unwrap();
    assert_eq!(
        seal.seal(&db, 9).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(seal.status().sealed, None);

    assert_eq!(seal.seal(&db, 10).unwrap().epoch, 10);
    seal.unseal(&db, 12).unwrap();
    assert_eq!(seal.status().sealed, None);
    assert_eq!(seal.last_cancelled_epoch(), Some(12));

    let reloaded = ActivationSeal::load(&db.lock().unwrap());
    assert_eq!(
        reloaded.last_cancelled_epoch(),
        Some(12),
        "a restart keeps the cancel"
    );
    assert_eq!(
        reloaded.seal(&db, 11).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}

/// A cancel forgets the evacuations begun under it or earlier, and keeps a
/// later epoch's.
#[test]
fn a_cancel_forgets_its_evacuations() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    {
        let conn = db.lock().unwrap();
        crate::db::activation_seal::begin_drain_evacuation(
            &conn,
            "sbx-a",
            3,
            crate::db::activation_seal::DrainPrior::Started,
            0,
        )
        .unwrap();
        crate::db::activation_seal::begin_drain_evacuation(
            &conn,
            "sbx-b",
            6,
            crate::db::activation_seal::DrainPrior::Paused,
            0,
        )
        .unwrap();
    }
    seal.unseal(&db, 3).unwrap();
    let left: Vec<_> = crate::db::read::drain_evacuations(&db.lock().unwrap())
        .unwrap()
        .into_iter()
        .map(|row| row.sandbox_id)
        .collect();
    assert_eq!(left, ["sbx-b"]);
}

/// Each seal that begins gets a start later than every earlier one, even when
/// the wall clock reads earlier than the last seal's start (a clock stepped
/// back, or a re-seal within the same millisecond), so `(epoch, since)`
/// names one continuous seal.
#[test]
fn a_new_seal_always_starts_after_the_last_one() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let future_ms = u64::try_from(jiff::Timestamp::now().as_millisecond()).unwrap() + 3_600_000;
    write_activation_seal(
        &db.lock().unwrap(),
        ActivationSealRecord {
            sealed_epoch: Some(3),
            sealed_since_ms: future_ms,
            last_cancelled_epoch: None,
        },
    )
    .unwrap();
    let seal = ActivationSeal::load(&db.lock().unwrap());
    seal.unseal(&db, 3).unwrap();

    let resealed = seal.seal(&db, 4).unwrap();
    assert_eq!(resealed.since_ms, future_ms + 1);
    let replaced = seal.seal(&db, 5).unwrap();
    assert_eq!(replaced.since_ms, future_ms + 2);
}

/// A seal row that cannot be read refuses every activation rather than
/// reading as unsealed, and reports why.
#[test]
fn an_unreadable_seal_refuses_activations() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    db.lock()
        .unwrap()
        .execute_batch("DROP TABLE activation_seal")
        .unwrap();
    let seal = ActivationSeal::load(&db.lock().unwrap());
    assert!(seal.load_error().is_some());
    let refused = seal
        .admit(InFlightActivationKind::Restore, SANDBOX)
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains(ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
    assert_eq!(seal.seal(&db, 1).unwrap_err().code(), tonic::Code::Internal);
}

/// A seal with work in flight answers `quiesced: false` with that work, oldest
/// first, once its wait runs out, and `quiesced: true` once the work has
/// finished. A continuation counts like an activation.
#[tokio::test(start_paused = true)]
async fn seal_waits_for_in_flight_work_and_lists_what_is_left() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let seal = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
    let restore = seal
        .admit(InFlightActivationKind::Restore, "sandbox-a")
        .unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    let checkpoint = seal.continuation(InFlightActivationKind::Checkpoint, "sandbox-b");
    tokio::time::advance(Duration::from_secs(1)).await;

    let outcome = seal
        .seal_and_wait(&db, 5, Duration::from_secs(10), &CancellationToken::new())
        .await
        .unwrap();
    assert!(!outcome.quiesced);
    assert_eq!(
        outcome.status.in_flight,
        vec![
            InFlightActivation {
                kind: InFlightActivationKind::Restore,
                sandbox_id: "sandbox-a".into(),
                age: Duration::from_secs(15),
            },
            InFlightActivation {
                kind: InFlightActivationKind::Checkpoint,
                sandbox_id: "sandbox-b".into(),
                age: Duration::from_secs(11),
            },
        ]
    );

    let waiting = tokio::spawn({
        let seal = Arc::clone(&seal);
        let db = db.clone();
        async move {
            seal.seal_and_wait(&db, 5, MAX_SEAL_WAIT, &CancellationToken::new())
                .await
        }
    });
    tokio::time::advance(Duration::from_secs(1)).await;
    drop(restore);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(!waiting.is_finished(), "the checkpoint is still in flight");
    drop(checkpoint);
    let outcome = waiting.await.unwrap().unwrap();
    assert!(outcome.quiesced);
    assert!(outcome.status.in_flight.is_empty());
    assert_eq!(
        outcome.status.sealed.map(|sealed| sealed.epoch),
        Some(5),
        "the re-seal under the same epoch only waited"
    );

    let too_long = seal
        .seal_and_wait(
            &db,
            5,
            MAX_SEAL_WAIT + Duration::from_millis(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(too_long.code(), tonic::Code::InvalidArgument);
}

/// The token ends its work's in-flight count however the work ends: it
/// returns an error, it is cancelled, or it panics. A token that is never
/// dropped stays counted, so the service tests' leak check fails on it.
#[tokio::test]
async fn in_flight_tokens_are_released_on_error_cancellation_and_panic() {
    let seal = Arc::new(ActivationSeal::unsealed());

    let failed = {
        let seal = Arc::clone(&seal);
        move || -> Result<(), &'static str> {
            let _token = seal.admit(InFlightActivationKind::Cold, SANDBOX).unwrap();
            Err("launch failed")
        }
    };
    failed().unwrap_err();
    assert!(seal.status().in_flight.is_empty(), "error");

    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let cancelled = tokio::spawn({
        let seal = Arc::clone(&seal);
        async move {
            let _token = seal
                .admit(InFlightActivationKind::ResidentResume, SANDBOX)
                .unwrap();
            let _ = held_tx.send(());
            std::future::pending::<()>().await;
        }
    });
    held_rx.await.unwrap();
    assert_eq!(seal.status().in_flight.len(), 1);
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    assert!(seal.status().in_flight.is_empty(), "cancellation");

    let panicked = tokio::spawn({
        let seal = Arc::clone(&seal);
        async move {
            let _token = seal.continuation(InFlightActivationKind::StopFlush, SANDBOX);
            panic!("activation step panicked");
        }
    });
    assert!(panicked.await.unwrap_err().is_panic());
    assert!(seal.status().in_flight.is_empty(), "panic");

    seal.assert_quiesced();
    #[allow(clippy::mem_forget, reason = "the leak a forgotten token is")]
    std::mem::forget(
        seal.admit(InFlightActivationKind::Restore, SANDBOX)
            .unwrap(),
    );
    assert!(
        std::panic::catch_unwind(|| seal.assert_quiesced()).is_err(),
        "the leak check fails on a forgotten token"
    );
}

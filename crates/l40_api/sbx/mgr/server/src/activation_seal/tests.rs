use super::*;

use pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER;

const SANDBOX: &str = "sandbox-1";

fn record(db: &crate::db::SharedConnection) -> Option<ActivationSealRecord> {
    crate::db::read::activation_seal(&db.lock().unwrap()).unwrap()
}

/// Sealing again under the current epoch changes nothing; a later epoch
/// replaces it; an earlier epoch is refused, to seal or to cancel, and
/// leaves the seal as it was; a zero or unstorable epoch is refused.
#[test]
fn seal_epochs_only_move_forward() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());

    seal.seal(&db, 7).unwrap();
    seal.seal(&db, 7).unwrap();
    assert_eq!(seal.status().sealed_epoch, Some(7));
    seal.seal(&db, 8).unwrap();
    assert_eq!(seal.status().sealed_epoch, Some(8));

    let written = Some(ActivationSealRecord {
        epoch: 8,
        sealed: true,
    });
    assert_eq!(record(&db), written);
    for refused in [
        seal.seal(&db, 7).unwrap_err(),
        seal.unseal(&db, 7).unwrap_err(),
    ] {
        assert_eq!(
            refused.code(),
            tonic::Code::FailedPrecondition,
            "{refused:?}"
        );
        assert_eq!(seal.status().sealed_epoch, Some(8));
        assert_eq!(record(&db), written, "a refused call writes nothing");
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
    assert_eq!(seal.status().sealed_epoch, Some(8));
}

/// A cancel of the current epoch unseals and is remembered: no seal at or
/// below it is taken again, while a later epoch seals. A cancel that arrives
/// before its seal (an undrain racing the drain's first call) still refuses
/// that seal, a cancel below the remembered one keeps it, and a later cancel
/// unseals an earlier seal. A restart keeps the cancel.
#[test]
fn a_cancelled_epoch_never_seals_again() {
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    let cancelled = |epoch| {
        Some(ActivationSealRecord {
            epoch,
            sealed: false,
        })
    };

    seal.seal(&db, 5).unwrap();
    seal.unseal(&db, 5).unwrap();
    assert_eq!(seal.status().sealed_epoch, None);
    assert_eq!(record(&db), cancelled(5));
    seal.unseal(&db, 5).unwrap();
    for stale in [4, 5] {
        assert_eq!(
            seal.seal(&db, stale).unwrap_err().code(),
            tonic::Code::FailedPrecondition,
            "{stale}"
        );
    }
    assert_eq!(seal.status().sealed_epoch, None);

    seal.unseal(&db, 9).unwrap();
    assert_eq!(
        seal.seal(&db, 9).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    seal.unseal(&db, 3).unwrap();
    assert_eq!(record(&db), cancelled(9));

    seal.seal(&db, 10).unwrap();
    seal.unseal(&db, 12).unwrap();
    assert_eq!(seal.status().sealed_epoch, None);
    assert_eq!(record(&db), cancelled(12));

    let reloaded = ActivationSeal::load(&db.lock().unwrap());
    assert_eq!(
        reloaded.seal(&db, 11).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    reloaded.seal(&db, 13).unwrap();
    assert_eq!(reloaded.status().sealed_epoch, Some(13));
}

/// A drain suspend is recorded only while the node is sealed under the
/// drain's epoch: not before the seal, not under another epoch, and not once
/// the epoch is cancelled, whose cancel forgets what was recorded.
#[test]
fn a_drain_suspend_is_recorded_only_under_its_own_seal() {
    use crate::db::activation_seal::DrainPrior;
    let db = crate::db::SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    let recorded = || -> Vec<String> {
        crate::db::read::drain_evacuations(&db.lock().unwrap())
            .unwrap()
            .into_iter()
            .map(|row| row.sandbox_id)
            .collect()
    };

    assert!(
        !seal
            .record_drain_suspend(&db, 5, "sbx-a", DrainPrior::Started)
            .unwrap()
    );
    seal.seal(&db, 5).unwrap();
    assert!(
        !seal
            .record_drain_suspend(&db, 4, "sbx-a", DrainPrior::Started)
            .unwrap()
    );
    assert_eq!(recorded(), Vec::<String>::new());
    assert!(
        seal.record_drain_suspend(&db, 5, "sbx-a", DrainPrior::Started)
            .unwrap()
    );
    assert_eq!(recorded(), ["sbx-a"]);

    seal.unseal(&db, 5).unwrap();
    assert_eq!(recorded(), Vec::<String>::new());
    assert!(
        !seal
            .record_drain_suspend(&db, 5, "sbx-b", DrainPrior::Paused)
            .unwrap()
    );
    assert_eq!(recorded(), Vec::<String>::new());
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
    let refused = seal.admit("restore", SANDBOX).unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains(ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
    assert_eq!(seal.seal(&db, 1).unwrap_err().code(), tonic::Code::Internal);
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
            let _token = seal.admit("cold", SANDBOX).unwrap();
            Err("launch failed")
        }
    };
    failed().unwrap_err();
    assert!(seal.status().in_flight.is_empty(), "error");

    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let cancelled = tokio::spawn({
        let seal = Arc::clone(&seal);
        async move {
            let _token = seal.admit("resident_resume", SANDBOX).unwrap();
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
            let _token = seal.admit("cold", SANDBOX).unwrap();
            panic!("activation step panicked");
        }
    });
    assert!(panicked.await.unwrap_err().is_panic());
    assert!(seal.status().in_flight.is_empty(), "panic");

    seal.assert_quiesced();
    #[allow(clippy::mem_forget, reason = "the leak a forgotten token is")]
    std::mem::forget(seal.admit("restore", SANDBOX).unwrap());
    assert!(
        std::panic::catch_unwind(|| seal.assert_quiesced()).is_err(),
        "the leak check fails on a forgotten token"
    );
}

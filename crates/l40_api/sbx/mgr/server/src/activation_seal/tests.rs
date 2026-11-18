use pplx_space_observability::test_capture::{CapturedEvent, RecordingVisitor, field_value};
use tracing_subscriber::layer::SubscriberExt as _;

use super::*;

const SANDBOX: &str = "sandbox-1";

fn record(db: &crate::db::SharedConnection) -> Option<ActivationSealRecord> {
    crate::db::read::activation_seal(&db.lock().unwrap()).unwrap()
}

/// Sealing again under the current epoch changes nothing; a later epoch
/// replaces it; an earlier epoch is refused, to seal or to cancel, and
/// leaves the seal as it was; a zero or unstorable epoch is refused.
#[test]
fn seal_epochs_only_move_forward() {
    let db = SharedConnection::new(crate::db::test_conn());
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
    let db = SharedConnection::new(crate::db::test_conn());
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
/// the epoch is cancelled, whose cancel keeps what was recorded.
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
        seal.record_drain_suspend(&db, 5, "sbx-a", DrainPrior::Started)
            .unwrap()
            .is_none()
    );
    seal.seal(&db, 5).unwrap();
    assert!(
        seal.record_drain_suspend(&db, 4, "sbx-a", DrainPrior::Started)
            .unwrap()
            .is_none()
    );
    assert_eq!(recorded(), Vec::<String>::new());
    assert_eq!(
        seal.record_drain_suspend(&db, 5, "sbx-a", DrainPrior::Started)
            .unwrap(),
        Some(DrainPrior::Started)
    );
    assert_eq!(recorded(), ["sbx-a"]);

    seal.unseal(&db, 5).unwrap();
    assert_eq!(recorded(), ["sbx-a"]);
    assert!(
        seal.record_drain_suspend(&db, 5, "sbx-b", DrainPrior::Paused)
            .unwrap()
            .is_none()
    );
    assert_eq!(recorded(), ["sbx-a"]);
}

/// An activation reads the seal as the service last read or wrote it, never
/// the database: a row written behind the seal's back changes nothing until
/// the service is built again, which is when a restart reads it.
#[test]
fn an_activation_reads_the_seal_in_memory_not_the_database() {
    let db = SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    write_activation_seal(
        &db.lock().unwrap(),
        ActivationSealRecord {
            epoch: 4,
            sealed: true,
        },
    )
    .unwrap();
    drop(
        seal.admit("cold", SANDBOX)
            .expect("the seal in memory is unsealed"),
    );

    let restarted = ActivationSeal::load(&db.lock().unwrap());
    let refused = restarted.admit("cold", SANDBOX).unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(refused.message().contains("epoch 4"), "{refused:?}");
    seal.assert_quiesced();
    restarted.assert_quiesced();
}

fn hide_seal_table(db: &SharedConnection) {
    db.lock()
        .unwrap()
        .execute_batch("ALTER TABLE activation_seal RENAME TO activation_seal_hidden")
        .unwrap();
}

/// Restore the table [`hide_seal_table`] hid, holding `row`.
fn restore_seal_table(db: &SharedConnection, row: Option<ActivationSealRecord>) {
    let conn = db.lock().unwrap();
    conn.execute_batch("ALTER TABLE activation_seal_hidden RENAME TO activation_seal")
        .unwrap();
    if let Some(row) = row {
        write_activation_seal(&conn, row).unwrap();
    }
}

#[track_caller]
fn assert_unreadable_refusal(refused: &Status) {
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert!(
        refused
            .message()
            .contains(ACTIVATION_SEAL_UNREADABLE_MARKER),
        "{refused:?}"
    );
}

/// A seal row that can't be read at startup leaves the seal unknown, which
/// refuses every activation as retryable rather than admit what a sealed row
/// would refuse. A seal call reads the row again: one that still can't be
/// read refuses the call and changes nothing, and once it reads, the seal it
/// holds refuses activations from then on.
#[test]
fn an_unreadable_seal_refuses_activations_until_a_seal_call_reads_it() {
    let db = SharedConnection::new(crate::db::test_conn());
    hide_seal_table(&db);
    let seal = ActivationSeal::load(&db.lock().unwrap());
    assert_unreadable_refusal(&seal.admit("restore", SANDBOX).unwrap_err());
    assert_unreadable_refusal(&seal.check("cold", SANDBOX).unwrap_err());
    assert!(seal.status().in_flight.is_empty());
    assert_eq!(seal.seal(&db, 6).unwrap_err().code(), tonic::Code::Internal);
    assert_unreadable_refusal(&seal.check("cold", SANDBOX).unwrap_err());

    restore_seal_table(
        &db,
        Some(ActivationSealRecord {
            epoch: 5,
            sealed: true,
        }),
    );
    assert_unreadable_refusal(&seal.check("cold", SANDBOX).unwrap_err());
    seal.seal(&db, 5).unwrap();
    assert_eq!(seal.status().sealed_epoch, Some(5));
    let refused = seal.admit("restore", SANDBOX).unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains(ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
    seal.assert_quiesced();
}

/// The background read of a seal left unknown at startup keeps retrying
/// while the row can't be read, refusing every activation meanwhile, and
/// publishes the row once it reads: a node never sealed, or one whose drain
/// was cancelled, admits activations again; a sealed one refuses them under
/// its epoch. The retry ends there.
#[tokio::test(start_paused = true)]
async fn the_background_read_publishes_the_seal_row_once_it_reads() {
    let cancelled = ActivationSealRecord {
        epoch: 3,
        sealed: false,
    };
    let sealed = ActivationSealRecord {
        epoch: 4,
        sealed: true,
    };
    for row in [None, Some(cancelled), Some(sealed)] {
        let db = SharedConnection::new(crate::db::test_conn());
        hide_seal_table(&db);
        let seal = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
        let shutdown = CancellationToken::new();
        let retry = tokio::spawn(Arc::clone(&seal).recover(
            db.clone(),
            shutdown.clone(),
            Duration::from_millis(100),
            Duration::from_secs(1),
        ));
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!retry.is_finished(), "{row:?}: retries while unreadable");
        assert_unreadable_refusal(&seal.admit("cold", SANDBOX).unwrap_err());

        restore_seal_table(&db, row);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(retry.is_finished(), "{row:?}: the retry ends once it reads");
        retry.await.unwrap();
        match row.filter(|row| row.sealed) {
            None => drop(
                seal.admit("cold", SANDBOX)
                    .unwrap_or_else(|refused| panic!("{row:?}: {refused:?}")),
            ),
            Some(_) => {
                let refused = seal.admit("cold", SANDBOX).unwrap_err();
                assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{row:?}");
                assert!(refused.message().contains("epoch 4"), "{refused:?}");
            }
        }
        seal.assert_quiesced();
    }
}

/// A seal that reads at startup starts no background read, and the retry of
/// an unknown seal is one task that ends at shutdown, leaving the seal
/// unknown and activations refused.
#[tokio::test(start_paused = true)]
async fn the_background_read_runs_only_while_the_seal_is_unknown() {
    let db = SharedConnection::new(crate::db::test_conn());
    let tracker = tokio_util::task::TaskTracker::new();
    let known = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
    known.spawn_recovery(db.clone(), CancellationToken::new(), &tracker);
    assert!(tracker.is_empty(), "a known seal needs no retry");

    hide_seal_table(&db);
    let unknown = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
    let shutdown = CancellationToken::new();
    unknown.spawn_recovery(db.clone(), shutdown.clone(), &tracker);
    assert_eq!(tracker.len(), 1);
    shutdown.cancel();
    tracker.close();
    tokio::time::timeout(Duration::from_secs(1), tracker.wait())
        .await
        .expect("the retry ends at shutdown");
    assert_unreadable_refusal(&unknown.check("cold", SANDBOX).unwrap_err());
}

/// The level and fields of each event the code under test logs.
#[derive(Clone, Default)]
struct LoggedEvents(Arc<Mutex<Vec<(tracing::Level, CapturedEvent)>>>);

impl LoggedEvents {
    fn with(&self, level: tracing::Level) -> Vec<CapturedEvent> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(logged, _)| *logged == level)
            .map(|(_, event)| event.clone())
            .collect()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LoggedEvents {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut visitor = RecordingVisitor::default();
        event.record(&mut visitor);
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), visitor.into_fields()));
    }
}

/// A seal row that stays unreadable logs a warning at most once a minute,
/// after the startup error, naming the read's error and how long the seal
/// has been unknown; the retry's other failures log at debug. The recovery
/// line says how long the seal was unknown.
#[tokio::test(start_paused = true)]
async fn a_seal_that_stays_unreadable_warns_once_a_minute_until_it_reads() {
    let logged = LoggedEvents::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(logged.clone()));
    let db = SharedConnection::new(crate::db::test_conn());
    hide_seal_table(&db);
    let seal = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
    let retry = tokio::spawn(Arc::clone(&seal).recover(
        db.clone(),
        CancellationToken::new(),
        RECOVERY_BACKOFF_INITIAL,
        RECOVERY_BACKOFF_MAX,
    ));

    tokio::time::sleep(Duration::from_secs(59)).await;
    assert!(
        logged.with(tracing::Level::WARN).is_empty(),
        "no warning inside the first minute"
    );

    tokio::time::sleep(Duration::from_secs(5)).await;
    let warned = logged.with(tracing::Level::WARN);
    assert_eq!(warned.len(), 1, "{warned:?}");
    assert!(
        field_value(&warned[0], "error").is_some_and(|error| error.contains("activation_seal")),
        "{warned:?}"
    );
    let first = unreadable_secs(&warned[0]);
    assert!((60..=62).contains(&first), "{warned:?}");

    tokio::time::sleep(Duration::from_secs(50)).await;
    assert_eq!(logged.with(tracing::Level::WARN).len(), 1, "once a minute");
    tokio::time::sleep(Duration::from_secs(15)).await;
    let warned = logged.with(tracing::Level::WARN);
    assert_eq!(warned.len(), 2, "{warned:?}");
    let second = unreadable_secs(&warned[1]);
    assert!((120..=124).contains(&second), "{warned:?}");

    restore_seal_table(&db, None);
    tokio::time::sleep(Duration::from_secs(3)).await;
    retry.await.unwrap();
    assert_eq!(logged.with(tracing::Level::WARN).len(), 2);
    let recovered: Vec<_> = logged
        .with(tracing::Level::INFO)
        .into_iter()
        .filter(|event| {
            field_value(event, "message").is_some_and(|message| message.contains("read the"))
        })
        .collect();
    assert_eq!(recovered.len(), 1, "{recovered:?}");
    assert!(
        unreadable_secs(&recovered[0]) >= 125,
        "the recovery line says how long the seal was unknown: {recovered:?}"
    );
    drop(seal.admit("cold", SANDBOX).unwrap());
}

fn unreadable_secs(event: &CapturedEvent) -> u64 {
    field_value(event, "unreadable_secs")
        .and_then(|secs| secs.parse().ok())
        .unwrap_or_else(|| panic!("no unreadable_secs in {event:?}"))
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

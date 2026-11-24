use super::*;

const SANDBOX: &str = "sandbox-1";

fn record(db: &crate::db::SharedConnection) -> Option<ActivationSealRecord> {
    crate::db::read::activation_seal(&db.lock().unwrap()).unwrap()
}

/// Sealing again under the current generation changes nothing; an earlier
/// generation is refused, to seal or to cancel, and leaves the seal as it
/// was; a zero or unstorable generation is refused.
#[test]
fn seal_generations_only_move_forward() {
    let db = SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());

    seal.seal(&db, 7).unwrap();
    seal.seal(&db, 7).unwrap();
    assert_eq!(seal.status().sealed_generation, Some(7));

    let written = Some(ActivationSealRecord {
        generation: 7,
        sealed: true,
    });
    assert_eq!(record(&db), written);
    for refused in [
        seal.seal(&db, 6).unwrap_err(),
        seal.unseal(&db, 6).unwrap_err(),
    ] {
        assert_eq!(
            refused.code(),
            tonic::Code::FailedPrecondition,
            "{refused:?}"
        );
        assert_refusal_names(&refused, 7, 6);
        assert_eq!(seal.status().sealed_generation, Some(7));
        assert_eq!(record(&db), written, "a refused call writes nothing");
    }

    for generation in [0, u64::MAX] {
        assert_eq!(
            seal.seal(&db, generation).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            seal.unseal(&db, generation).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(seal.status().sealed_generation, Some(7));
}

/// Both generations a refusal names: the node's and the call's.
#[track_caller]
fn assert_refusal_names(refused: &Status, current: u64, asked: u64) {
    for generation in [current, asked] {
        assert!(
            refused
                .message()
                .contains(&format!("generation {generation}")),
            "{generation}: {refused:?}"
        );
    }
}

/// One drain seals the node at a time: sealed under a generation, the node
/// refuses a seal under any other, a later one included, with both
/// generations named, and keeps its seal, in memory, in the row and across
/// a restart. Once that drain is cancelled, a later generation seals.
#[test]
fn a_seal_under_another_generation_is_refused() {
    let db = SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    seal.seal(&db, 7).unwrap();
    let written = Some(ActivationSealRecord {
        generation: 7,
        sealed: true,
    });

    for other in [6, 8, 9] {
        let refused = seal.seal(&db, other).unwrap_err();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{other}");
        assert_refusal_names(&refused, 7, other);
        assert_eq!(seal.status().sealed_generation, Some(7), "{other}");
        assert_eq!(
            record(&db),
            written,
            "{other}: a refused seal writes nothing"
        );
    }
    let restarted = ActivationSeal::load(&db.lock().unwrap());
    assert_refusal_names(&restarted.seal(&db, 8).unwrap_err(), 7, 8);
    assert_eq!(restarted.status().sealed_generation, Some(7));

    assert_eq!(restarted.unseal(&db, 7).unwrap(), 7);
    restarted.seal(&db, 8).unwrap();
    assert_eq!(restarted.status().sealed_generation, Some(8));
}

/// A cancel of the current generation unseals and is remembered as its
/// tombstone: no seal at or below it is taken again, while a later
/// generation seals. A cancel that arrives before its seal (an undrain
/// racing the drain's first call) still refuses that seal, a cancel below
/// the tombstone keeps it, and a later cancel unseals an earlier seal. Each
/// cancel answers the tombstone. A restart keeps the cancel.
#[test]
fn a_cancelled_generation_never_seals_again() {
    let db = SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    let cancelled = |generation| {
        Some(ActivationSealRecord {
            generation,
            sealed: false,
        })
    };

    seal.seal(&db, 5).unwrap();
    assert_eq!(seal.unseal(&db, 5).unwrap(), 5);
    assert_eq!(seal.status().sealed_generation, None);
    assert_eq!(record(&db), cancelled(5));
    assert_eq!(seal.unseal(&db, 5).unwrap(), 5);
    for stale in [4, 5] {
        let refused = seal.seal(&db, stale).unwrap_err();
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{stale}");
        assert_refusal_names(&refused, 5, stale);
    }
    assert_eq!(seal.status().sealed_generation, None);

    assert_eq!(seal.unseal(&db, 9).unwrap(), 9);
    assert_eq!(
        seal.seal(&db, 9).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(seal.unseal(&db, 3).unwrap(), 9, "the tombstone stays");
    assert_eq!(record(&db), cancelled(9));

    seal.seal(&db, 10).unwrap();
    assert_eq!(seal.unseal(&db, 12).unwrap(), 12);
    assert_eq!(seal.status().sealed_generation, None);
    assert_eq!(record(&db), cancelled(12));

    let reloaded = ActivationSeal::load(&db.lock().unwrap());
    assert_eq!(
        reloaded.seal(&db, 11).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    reloaded.seal(&db, 13).unwrap();
    assert_eq!(reloaded.status().sealed_generation, Some(13));
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
            generation: 4,
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
    assert!(refused.message().contains("generation 4"), "{refused:?}");
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
            generation: 5,
            sealed: true,
        }),
    );
    assert_unreadable_refusal(&seal.check("cold", SANDBOX).unwrap_err());
    seal.seal(&db, 5).unwrap();
    assert_eq!(seal.status().sealed_generation, Some(5));
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
/// its generation. The retry ends there.
#[tokio::test(start_paused = true)]
async fn the_background_read_publishes_the_seal_row_once_it_reads() {
    let cancelled = ActivationSealRecord {
        generation: 3,
        sealed: false,
    };
    let sealed = ActivationSealRecord {
        generation: 4,
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
                assert!(refused.message().contains("generation 4"), "{refused:?}");
            }
        }
        seal.assert_quiesced();
    }
}

/// A seal call that reads the row between the background read and its
/// publication wins: the background read's answer, older than the seal, is
/// not published over it, and the retry ends with the seal known.
#[test]
fn a_seal_call_between_the_background_read_and_its_publication_wins() {
    let db = SharedConnection::new(crate::db::test_conn());
    hide_seal_table(&db);
    let seal = ActivationSeal::load(&db.lock().unwrap());
    restore_seal_table(&db, None);
    // The background read finds a node never sealed...
    let read =
        crate::db::read::activation_seal(&db.lock().unwrap()).map_err(|error| error.to_string());
    assert_eq!(read, Ok(None));
    // ...and a seal call reads the row and seals before that is published.
    seal.seal(&db, 9).unwrap();
    assert!(seal.publish_recovered(read), "the seal is known");
    assert_eq!(seal.status().sealed_generation, Some(9));
    let refused = seal.admit("cold", SANDBOX).unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    seal.assert_quiesced();
}

/// While the background read waits on the database, an activation is
/// refused as retryable at once rather than blocked behind it: the read
/// holds no state lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_activation_is_refused_not_blocked_while_the_background_read_waits() {
    let db = SharedConnection::new(crate::db::test_conn());
    hide_seal_table(&db);
    let seal = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
    let shutdown = CancellationToken::new();
    let held = db.clone();
    let guard = held.lock().unwrap();
    let retry = tokio::spawn(Arc::clone(&seal).recover(
        db.clone(),
        shutdown.clone(),
        Duration::from_millis(10),
        Duration::from_millis(10),
    ));
    // Long enough for the retry to be waiting on the held database.
    std::thread::sleep(Duration::from_millis(200));
    let (sent, received) = std::sync::mpsc::channel();
    let checking = Arc::clone(&seal);
    std::thread::spawn(move || {
        let _ = sent.send(checking.check("cold", SANDBOX));
    });
    let refused = received
        .recv_timeout(Duration::from_secs(2))
        .expect("the check is not blocked behind the background read")
        .unwrap_err();
    assert_unreadable_refusal(&refused);
    drop(guard);
    shutdown.cancel();
    retry.await.unwrap();
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

/// A background read that finds the service database's mutex poisoned is
/// published as a failed read: the seal stays unknown with that error, and
/// activations are refused as unreadable. It isn't dropped.
#[tokio::test(start_paused = true)]
async fn a_poisoned_database_is_reported_by_the_background_read() {
    let db = SharedConnection::new(crate::db::test_conn());
    hide_seal_table(&db);
    let seal = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
    let poisoner = db.clone();
    std::thread::spawn(move || {
        let _held = poisoner.lock().unwrap();
        panic!("poisons the service database's mutex");
    })
    .join()
    .unwrap_err();
    let shutdown = CancellationToken::new();
    let retry = tokio::spawn(Arc::clone(&seal).recover(
        db.clone(),
        shutdown.clone(),
        Duration::from_millis(100),
        Duration::from_secs(1),
    ));
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_eq!(
        seal.lock_state().record,
        Err(POISONED_DATABASE.to_owned()),
        "the poisoned read is published"
    );
    assert_unreadable_refusal(&seal.check("cold", SANDBOX).unwrap_err());
    shutdown.cancel();
    retry.await.unwrap();
}

/// A background read that panicked is published as a failed read too: the
/// seal stays unknown with the panic as its error, and activations are
/// refused as unreadable.
#[tokio::test]
async fn a_panicked_background_read_is_reported() {
    let db = SharedConnection::new(crate::db::test_conn());
    hide_seal_table(&db);
    let seal = ActivationSeal::load(&db.lock().unwrap());
    let joined = tokio::task::spawn_blocking(|| -> Result<Option<ActivationSealRecord>, String> {
        panic!("the read panics")
    })
    .await;

    assert!(
        !seal.publish_recovered(joined_read(joined)),
        "still unknown"
    );
    let error = seal.lock_state().record.clone().unwrap_err();
    assert!(error.contains("panicked"), "{error}");
    assert_unreadable_refusal(&seal.check("cold", SANDBOX).unwrap_err());
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

/// A node-local artifact producer for a sandbox without a VM is refused on a
/// node known sealed, with the plain sealed refusal, and admitted on a node
/// never sealed or whose seal is unknown; one for a sandbox with a VM is
/// admitted sealed too. Each admitted producer is in flight, apart from the
/// activations, until its token drops.
#[test]
fn a_sealed_node_refuses_only_the_producers_of_sandboxes_without_a_vm() {
    let db = SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    let unsealed = seal.admit_producer("snapshot", SANDBOX, false).unwrap();
    seal.seal(&db, 3).unwrap();
    let refused = seal
        .admit_producer("snapshot", "sandbox-2", false)
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().starts_with(ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
    let with_vm = seal.admit_producer("snapshot", "sandbox-3", true).unwrap();
    let status = seal.status();
    assert_eq!(status.producers_in_flight, [SANDBOX, "sandbox-3"]);
    assert!(status.in_flight.is_empty());
    drop((unsealed, with_vm));
    seal.assert_quiesced();

    hide_seal_table(&db);
    let unknown = ActivationSeal::load(&db.lock().unwrap());
    assert!(unknown.read_error().is_some());
    let admitted = unknown.admit_producer("snapshot", SANDBOX, false).unwrap();
    assert_eq!(unknown.status().producers_in_flight, [SANDBOX]);
    drop(admitted);
    unknown.assert_quiesced();
}

/// The check of work that would leave a sandbox without a VM stopped, the
/// idle auto-stop of a suspended sandbox: a node known sealed refuses it with
/// the sealed refusal, a node never sealed, cancelled or whose seal row can't
/// be read admits it, and nothing is counted in flight.
#[test]
fn a_sealed_node_refuses_work_that_would_leave_a_sandbox_stopped() {
    let db = SharedConnection::new(crate::db::test_conn());
    let seal = ActivationSeal::load(&db.lock().unwrap());
    seal.check_without_vm("idle auto-stop", SANDBOX).unwrap();
    seal.seal(&db, 3).unwrap();
    let refused = seal
        .check_without_vm("idle auto-stop", SANDBOX)
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().starts_with(ACTIVATIONS_SEALED_MARKER)
            && refused.message().contains("idle auto-stop"),
        "{refused:?}"
    );
    seal.assert_quiesced();
    seal.unseal(&db, 3).unwrap();
    seal.check_without_vm("idle auto-stop", SANDBOX).unwrap();

    hide_seal_table(&db);
    let unknown = ActivationSeal::load(&db.lock().unwrap());
    assert!(unknown.read_error().is_some());
    unknown.check_without_vm("idle auto-stop", SANDBOX).unwrap();
    unknown.assert_quiesced();
}

/// A sandbox has a VM while no cleanup owns its row and its lifecycle is
/// `started` or `paused`.
#[test]
fn only_a_started_or_paused_sandbox_has_a_vm() {
    for state in SandboxStatus::sweep() {
        assert_eq!(
            has_vm(state),
            matches!(
                state,
                SandboxStatus::Unowned(SandboxState::Started | SandboxState::Paused)
            ),
            "{state}"
        );
    }
}

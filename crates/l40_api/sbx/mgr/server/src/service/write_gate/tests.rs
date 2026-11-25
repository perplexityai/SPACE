use super::*;
use crate::activation_seal::{ACTIVATION_SEAL_UNREADABLE_MARKER, ACTIVATIONS_SEALED_MARKER};
use crate::db::test_fixtures::state_write_fixtures::insert_row;

/// The seal states a node can be in when a writer asks.
#[derive(Clone, Copy, Debug)]
enum Seal {
    NeverSealed,
    Sealed,
    /// Sealed, then cancelled: the tombstone is unsealed.
    Cancelled,
    /// The startup read of the seal row failed.
    Unknown,
}

/// What a writer or an activation asks the gate for, by the entry point
/// that serves it.
#[derive(Clone, Copy, Debug)]
enum Asked {
    /// `lock_for_write`.
    Locked(LockedWriter),
    /// `lock_for_publication` of a disk artifact.
    Publication(LockedWriter),
    /// `admit`, which takes no lock.
    Unlocked(UnlockedWriter),
    /// `lock`.
    DrainCompatible(DrainCompatible),
    /// `lock_for_activation`.
    ActivationLock(ActivationAdmissionKind),
    /// `check_activation`.
    Activation(ActivationAdmissionKind),
}

/// A sandbox's row as the gate may read it.
#[derive(Clone, Copy, Debug)]
enum Row {
    /// In this status, or missing.
    Reads(Option<SandboxStatus>),
    /// The sandboxes table can't be read.
    Unreadable,
}

/// What the gate answers.
#[derive(Debug, Eq, PartialEq)]
enum Answer {
    /// Admitted; counted in flight as an artifact producer or not.
    Admitted { counted: bool },
    /// Refused with this code and a message starting with this marker, or
    /// with none.
    Refused(tonic::Code, Option<&'static str>),
}

fn every_ask() -> impl Iterator<Item = Asked> {
    let locked = || {
        Producer::ALL
            .into_iter()
            .map(LockedWriter::Producer)
            .chain([LockedWriter::RuntimeUpgrade])
    };
    locked()
        .map(Asked::Locked)
        .chain(locked().map(Asked::Publication))
        .chain([UnlockedWriter::IdleStop, UnlockedWriter::UpgradeStage].map(Asked::Unlocked))
        .chain(DrainCompatible::ALL.map(Asked::DrainCompatible))
        .chain(ActivationAdmissionKind::ALL.map(Asked::ActivationLock))
        .chain(ActivationAdmissionKind::ALL.map(Asked::Activation))
}

/// The contract each class keeps, as the gate's and the seal's docs state it,
/// on a node known sealed: a producer for a sandbox without a VM is refused;
/// a runtime upgrade's fence is refused for a sandbox that has no VM and
/// isn't stopped; the idle auto-stop and an upgrade's stage are refused
/// whatever the row, which they don't read; a rule that reads a row it
/// can't read refuses as retryable, without the sealed marker. An unknown
/// seal admits every writer. An activation's lock and a drain-compatible
/// writer are always admitted, and an activation's check refuses while
/// sealed or unknown. Producers, the auto-stop and the stage are counted in
/// flight.
fn contract(asked: Asked, seal: Seal, row: Row) -> Answer {
    let sealed = matches!(seal, Seal::Sealed);
    let refused = Answer::Refused(
        tonic::Code::FailedPrecondition,
        Some(ACTIVATIONS_SEALED_MARKER),
    );
    let retry = Answer::Refused(tonic::Code::Unavailable, None);
    let unowned = match row {
        Row::Reads(status) => Ok(status.and_then(SandboxStatus::unowned)),
        Row::Unreadable => Err(()),
    };
    match asked {
        Asked::Locked(writer) | Asked::Publication(writer) if sealed => {
            let (admitted, counted) = match writer {
                LockedWriter::Producer(_) => {
                    (&[SandboxState::Started, SandboxState::Paused][..], true)
                }
                LockedWriter::RuntimeUpgrade => (
                    &[
                        SandboxState::Started,
                        SandboxState::Paused,
                        SandboxState::Stopped,
                    ][..],
                    false,
                ),
            };
            match unowned {
                Ok(Some(state)) if admitted.contains(&state) => Answer::Admitted { counted },
                Ok(_) => refused,
                Err(()) => retry,
            }
        }
        Asked::Locked(writer) | Asked::Publication(writer) => Answer::Admitted {
            counted: matches!(writer, LockedWriter::Producer(_)),
        },
        Asked::Unlocked(_) if sealed => refused,
        Asked::Unlocked(_) => Answer::Admitted { counted: true },
        Asked::DrainCompatible(_) | Asked::ActivationLock(_) => Answer::Admitted { counted: false },
        Asked::Activation(_) => match seal {
            Seal::NeverSealed | Seal::Cancelled => Answer::Admitted { counted: false },
            Seal::Sealed => refused,
            Seal::Unknown => Answer::Refused(
                tonic::Code::Unavailable,
                Some(ACTIVATION_SEAL_UNREADABLE_MARKER),
            ),
        },
    }
}

/// A node in `seal`, with its rows one per lifecycle and cleanup status and
/// a missing one, by id; or, when `unreadable`, with one sandbox whose row
/// can't be read.
fn node(seal: Seal, unreadable: bool) -> (NodeWriteGate, Vec<(String, Row)>) {
    let db = SharedConnection::new(crate::db::test_conn());
    let mut rows: Vec<_> = {
        let conn = db.lock().unwrap();
        SandboxStatus::sweep()
            .into_iter()
            .map(|status| (insert_row(&conn, status), Row::Reads(Some(status))))
            .collect()
    };
    rows.push((
        pplx_space_sandbox_model::SandboxId::new().to_string(),
        Row::Reads(None),
    ));
    if matches!(seal, Seal::Unknown) {
        db.lock()
            .unwrap()
            .execute_batch("ALTER TABLE activation_seal RENAME TO activation_seal_hidden")
            .unwrap();
    }
    let activation_seal = Arc::new(ActivationSeal::load(&db.lock().unwrap()));
    match seal {
        Seal::NeverSealed | Seal::Unknown => {}
        Seal::Sealed => activation_seal.seal(&db, 5).unwrap(),
        Seal::Cancelled => {
            activation_seal.seal(&db, 5).unwrap();
            activation_seal.unseal(&db, 5).unwrap();
        }
    }
    if unreadable {
        db.lock()
            .unwrap()
            .execute_batch("ALTER TABLE sandboxes RENAME TO sandboxes_hidden")
            .unwrap();
        rows.truncate(1);
        rows[0].1 = Row::Unreadable;
    }
    let gate = NodeWriteGate::new(activation_seal, db, AutoLifecycleCoordinator::default());
    (gate, rows)
}

/// Every entry point, with every class it takes, on a node never sealed,
/// sealed, cancelled or whose seal is unknown, for a sandbox in every status,
/// one with no row and one whose row can't be read: the gate admits or
/// refuses each as its class's contract says. A writer decided under the
/// lock is decided while holding it, and a refusal releases it; a writer
/// admitted without the lock holds none. A refusal carries the wire's code
/// and marker. An admitted producer counts in flight until its guard drops,
/// and nothing else counts.
#[tokio::test]
async fn the_gate_admits_each_class_as_its_contract_says() {
    for seal in [
        Seal::NeverSealed,
        Seal::Sealed,
        Seal::Cancelled,
        Seal::Unknown,
    ] {
        for unreadable in [false, true] {
            let (gate, rows) = node(seal, unreadable);
            for asked in every_ask() {
                for (id, row) in &rows {
                    let case = format!("{asked:?} on {seal:?}, sandbox {row:?}");
                    let answer = ask(&gate, asked, id, &case).await;
                    assert_eq!(answer, contract(asked, seal, *row), "{case}");
                    gate.seal().assert_quiesced();
                }
            }
        }
    }
}

/// `asked` of `gate` for sandbox `id`, as the gate answers it.
async fn ask(gate: &NodeWriteGate, asked: Asked, id: &str, case: &str) -> Answer {
    let locked = |admitted: Result<ArtifactWriteGuard, SealRefusal>| match admitted {
        Ok(guard) => holding(gate, id, guard, case),
        Err(refusal) => {
            assert!(
                gate.try_lock_for_read(id, Reader::Test).is_some(),
                "{case}: still locked"
            );
            refused(refusal)
        }
    };
    match asked {
        Asked::Locked(writer) => locked(gate.lock_for_write(id, writer).await),
        Asked::Publication(writer) => locked(
            gate.lock_for_publication(id, ArtifactKind::Disk, writer)
                .await
                .map(|(wake, guard)| {
                    assert!(
                        wake.is_none(),
                        "{case}: a disk publication takes no wake lock"
                    );
                    guard
                }),
        ),
        Asked::Unlocked(writer) => match gate.admit(id, writer) {
            Ok(admission) => {
                assert!(
                    gate.try_lock_for_read(id, Reader::Test).is_some(),
                    "{case}: locked"
                );
                let counted = counted(gate, id);
                drop(admission);
                Answer::Admitted { counted }
            }
            Err(refusal) => refused(refusal),
        },
        Asked::DrainCompatible(writer) => holding(gate, id, gate.lock(id, writer).await, case),
        Asked::ActivationLock(kind) => {
            holding(gate, id, gate.lock_for_activation(id, kind).await, case)
        }
        Asked::Activation(kind) => match gate.check_activation(kind, id) {
            Ok(()) => Answer::Admitted { counted: false },
            Err(refusal) => refused(refusal),
        },
    }
}

/// An admitted writer's answer while `guard` holds sandbox `id`'s artifact
/// lock, which then drops.
fn holding(gate: &NodeWriteGate, id: &str, guard: ArtifactWriteGuard, case: &str) -> Answer {
    assert!(
        gate.try_lock_for_read(id, Reader::Test).is_none(),
        "{case}: unlocked"
    );
    let counted = counted(gate, id);
    drop(guard);
    Answer::Admitted { counted }
}

/// Whether sandbox `id` is counted as an artifact producer in flight.
fn counted(gate: &NodeWriteGate, id: &str) -> bool {
    gate.seal().status().producers_in_flight == [id.to_owned()]
}

/// The answer a refusal gives on the wire: its code, and the marker its
/// message starts with, if any.
fn refused(refusal: SealRefusal) -> Answer {
    let status = Status::from(refusal);
    let marker = [ACTIVATIONS_SEALED_MARKER, ACTIVATION_SEAL_UNREADABLE_MARKER]
        .into_iter()
        .find(|marker| status.message().contains(marker));
    if let Some(marker) = marker {
        assert!(status.message().starts_with(marker), "{status:?}");
    }
    Answer::Refused(status.code(), marker)
}

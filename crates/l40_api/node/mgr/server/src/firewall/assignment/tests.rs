use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Barrier;

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision};
use tempfile::TempDir;
use tonic::Status;
use uuid::Uuid;

use super::{
    AcceptError, Acceptance, AtomicWriteError, DesiredAssignment, FirewallAssignments,
    IncomingAssignment, OpenError, Ordering, Unavailable, write_record,
};
use crate::service::artifact_fs;

const NODE: NodeId = NodeId::from_uuid(Uuid::from_u128(1));
const DNS: &str = r#"{"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}"#;
const WEB: &str = r#"{"id":"web","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]}"#;

/// Makes `store` unavailable, as an unsynced write does.
pub(crate) fn make_unavailable(store: &FirewallAssignments) {
    store.unavailable.store(true, Ordering::Release);
}

fn document(rules: &[&str]) -> String {
    format!(r#"{{"schema_version":1,"rules":[{}]}}"#, rules.join(","))
}

fn incoming(generation: u64, revision: u128, document: &str) -> IncomingAssignment<'_> {
    IncomingAssignment {
        node_id: NODE,
        generation,
        revision_id: Uuid::from_u128(revision),
        document,
    }
}

fn accepted(generation: u64, revision: u128, document: &str) -> DesiredAssignment {
    DesiredAssignment {
        node_id: NODE,
        generation,
        revision_id: Uuid::from_u128(revision),
        revision: decode_revision(document).unwrap(),
    }
}

fn record_path() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    // The record is opened without following links, so resolve any in the
    // temporary directory's own path.
    let path = dir
        .path()
        .canonicalize()
        .unwrap()
        .join("firewall-assignment.json");
    (dir, path)
}

fn open(path: &Path) -> FirewallAssignments {
    FirewallAssignments::open(path).unwrap()
}

fn current(store: &FirewallAssignments) -> Option<DesiredAssignment> {
    store.current().unwrap().as_deref().cloned()
}

/// Memory, and a fresh open of the file, both hold `expected`.
fn assert_state(store: &FirewallAssignments, path: &Path, expected: &DesiredAssignment) {
    assert_eq!(current(store).as_ref(), Some(expected), "in memory");
    assert_eq!(current(&open(path)).as_ref(), Some(expected), "on disk");
}

/// Catches a missing file read as anything but no assignment, and an
/// acceptance that doesn't survive reopening with its ordered rules, empty
/// rules included.
#[test]
fn accepted_assignments_reopen() {
    let (_dir, path) = record_path();
    let store = open(&path);
    assert_eq!(current(&store), None);

    let rules = document(&[WEB, DNS]);
    assert_eq!(
        store.accept(NODE, incoming(12, 1, &rules)).unwrap(),
        Acceptance::Committed
    );
    assert_eq!(current(&open(&path)), Some(accepted(12, 1, &rules)));

    let empty = document(&[]);
    store.accept(NODE, incoming(13, 2, &empty)).unwrap();
    assert_eq!(current(&open(&path)), Some(accepted(13, 2, &empty)));
}

/// Catches a replay that rewrites the record or compares raw JSON, and a
/// conflict check that ignores the revision ID or compares rules unordered.
#[test]
fn same_generation_replays_exactly_or_conflicts() {
    let (_dir, path) = record_path();
    let store = open(&path);
    let rules = document(&[WEB, DNS]);
    store.accept(NODE, incoming(12, 1, &rules)).unwrap();
    // Every write renames a new inode into place.
    let inode = || std::fs::metadata(&path).unwrap().ino();
    let unwritten = inode();

    let reformatted = rules.replace(',', ", ");
    assert_eq!(
        store.accept(NODE, incoming(12, 1, &reformatted)).unwrap(),
        Acceptance::Replayed
    );
    assert_eq!(inode(), unwritten, "a replay must not write");
    let conflicts = [
        store.accept(NODE, incoming(12, 2, &rules)),
        store.accept(NODE, incoming(12, 1, &document(&[DNS, WEB]))),
    ];
    assert!(
        matches!(
            conflicts,
            [
                Err(AcceptError::Conflict { generation: 12 }),
                Err(AcceptError::Conflict { generation: 12 })
            ]
        ),
        "{conflicts:?}"
    );
    assert_state(&store, &path, &accepted(12, 1, &rules));
}

/// Catches acceptance by novelty rather than generation: an older generation
/// is stale even with the same rules, and a later one may repeat them.
#[test]
fn generation_order_decides_acceptance() {
    let (_dir, path) = record_path();
    let store = open(&path);
    let rules = document(&[DNS]);
    store.accept(NODE, incoming(12, 1, &rules)).unwrap();

    let stale = store.accept(NODE, incoming(11, 1, &rules));
    assert!(
        matches!(
            stale,
            Err(AcceptError::Stale {
                accepted: 12,
                incoming: 11
            })
        ),
        "{stale:?}"
    );
    assert_state(&store, &path, &accepted(12, 1, &rules));
    assert_eq!(
        store.accept(NODE, incoming(15, 1, &rules)).unwrap(),
        Acceptance::Committed
    );
    assert_state(&store, &path, &accepted(15, 1, &rules));
}

/// Catches an assignment that authorizes its own node, before or after the
/// store is bound, a store that rebinds to another node, and invalid,
/// unsupported or oversized assignments that reach disk or memory.
#[test]
fn refused_assignments_change_nothing() {
    let (_dir, path) = record_path();
    let store = open(&path);
    let rules = document(&[DNS]);
    let other = NodeId::from_uuid(Uuid::from_u128(2));
    let for_other = |generation| IncomingAssignment {
        node_id: other,
        ..incoming(generation, 1, &rules)
    };

    let unbound = store.accept(NODE, for_other(12));
    assert!(
        matches!(unbound, Err(AcceptError::WrongNode { .. })),
        "{unbound:?}"
    );
    assert_eq!(current(&store), None);
    assert!(!path.exists());

    store.accept(NODE, incoming(12, 1, &rules)).unwrap();
    let invalid = document(&[r#"{"id":"r","all_of":[]}"#]);
    // Fit the one-rule record already on disk, but not a two-rule replacement.
    let max_record_bytes = std::fs::metadata(&path).unwrap().len();
    let two_rules = document(&[DNS, WEB]);
    let refusals = [
        store.accept(NODE, for_other(13)),
        store.accept(other, for_other(13)),
        store.accept(NODE, incoming(13, 2, &invalid)),
        store.accept(NODE, incoming(13, 2, r#"{"schema_version":2,"rules":[]}"#)),
        store.accept_with_limit(NODE, incoming(13, 2, &two_rules), max_record_bytes),
    ];
    assert!(
        matches!(
            refusals,
            [
                Err(AcceptError::WrongNode { .. }),
                Err(AcceptError::BoundToOtherNode { .. }),
                Err(AcceptError::InvalidRevision(DecodeError::EmptyAllOf { .. })),
                Err(AcceptError::InvalidRevision(
                    DecodeError::UnsupportedSchemaVersion(2)
                )),
                Err(AcceptError::TooLarge),
            ]
        ),
        "{refusals:?}"
    );
    assert_state(&store, &path, &accepted(12, 1, &rules));
}

/// Catches publishing or succeeding before the record is replaced, a failure
/// that blocks the old record's replay, and a lost reply that the identical
/// delivery can't recover.
#[test]
fn failed_write_publishes_nothing_and_lost_reply_replays() {
    let (_dir, path) = record_path();
    let mut store = open(&path);
    let (twelve, thirteen) = (document(&[DNS]), document(&[WEB]));
    store.accept(NODE, incoming(12, 1, &twelve)).unwrap();

    store.write = |path, _| {
        artifact_fs::write_durable_atomic(path, |_| Err(Status::unavailable("injected"))).map(drop)
    };
    let failed = store.accept(NODE, incoming(13, 2, &thirteen));
    assert!(matches!(failed, Err(AcceptError::Write(_))), "{failed:?}");
    assert_state(&store, &path, &accepted(12, 1, &twelve));
    assert_eq!(
        store.accept(NODE, incoming(12, 1, &twelve)).unwrap(),
        Acceptance::Replayed
    );

    store.write = write_record;
    let _lost_reply = store.accept(NODE, incoming(13, 2, &thirteen));
    assert_eq!(
        store.accept(NODE, incoming(13, 2, &thirteen)).unwrap(),
        Acceptance::Replayed
    );
    assert_state(&store, &path, &accepted(13, 2, &thirteen));
}

/// Catches a store that stays authoritative after a replacement it can't
/// prove durable, or that acknowledges a retry by reading that replacement back.
#[test]
fn unsynced_replace_is_unavailable_until_reopened() {
    let (_dir, path) = record_path();
    let mut store = open(&path);
    let (twelve, thirteen) = (document(&[DNS]), document(&[WEB]));
    store.accept(NODE, incoming(12, 1, &twelve)).unwrap();

    store.write = |path, bytes| {
        write_record(path, bytes)?;
        Err(AtomicWriteError::NotSynced(Status::unavailable("injected")))
    };
    let unsynced = store.accept(NODE, incoming(13, 2, &thirteen));
    assert!(
        matches!(unsynced, Err(AcceptError::Unsynced(_))),
        "{unsynced:?}"
    );
    store.write = write_record;
    let retry = store.accept(NODE, incoming(13, 2, &thirteen));
    assert!(
        matches!(retry, Err(AcceptError::Unavailable(Unavailable))),
        "{retry:?}"
    );
    store.current().unwrap_err();

    let reopened = open(&path);
    assert_eq!(current(&reopened), Some(accepted(13, 2, &thirteen)));
    assert_eq!(
        reopened.accept(NODE, incoming(13, 2, &thirteen)).unwrap(),
        Acceptance::Replayed
    );
}

/// Catches an unreadable record opened as no assignment, or rewritten.
#[test]
fn unreadable_records_fail_closed_and_stay() {
    let (_dir, path) = record_path();
    let newer_schema = format!(
        r#"{{"format_version":1,"node_id":"{}","generation":12,"revision_id":"{}","revision":"{{\"schema_version\":2,\"rules\":[]}}"}}"#,
        NODE.as_uuid(),
        Uuid::from_u128(1),
    );
    let errors = [
        r#"{"format_version":2}"#,
        r#"{"format_version":1,"#,
        &newer_schema,
    ]
    .map(|contents| {
        std::fs::write(&path, contents).unwrap();
        let error = FirewallAssignments::open(&path).err();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), contents);
        error
    });
    assert!(
        matches!(
            errors,
            [
                Some(OpenError::Format(2)),
                Some(OpenError::Decode(_)),
                Some(OpenError::Revision(DecodeError::UnsupportedSchemaVersion(
                    2
                ))),
            ]
        ),
        "{errors:?}"
    );
}

fn race<const N: usize>(
    store: &FirewallAssignments,
    attempts: [IncomingAssignment<'_>; N],
) -> [Result<Acceptance, AcceptError>; N] {
    let barrier = &Barrier::new(N);
    std::thread::scope(|scope| {
        attempts
            .map(|attempt| {
                scope.spawn(move || {
                    barrier.wait();
                    store.accept(NODE, attempt)
                })
            })
            .map(|thread| thread.join().unwrap())
    })
}

/// Catches acceptance that isn't serialized through publication: racing
/// generations end at the highest on disk and in memory, and racing
/// assignments at one generation leave exactly one winner.
#[test]
fn racing_accepts_serialize() {
    let (_dir, path) = record_path();
    let store = open(&path);
    let (dns, web) = (document(&[DNS]), document(&[WEB]));
    for higher in (2..=64).step_by(2) {
        let results = race(
            &store,
            [incoming(higher - 1, 1, &dns), incoming(higher, 2, &web)],
        );
        assert!(
            matches!(
                results,
                [
                    Ok(Acceptance::Committed) | Err(AcceptError::Stale { .. }),
                    Ok(Acceptance::Committed)
                ]
            ),
            "{results:?}"
        );
        assert_state(&store, &path, &accepted(higher, 2, &web));
    }

    let results = race(&store, [incoming(100, 3, &dns), incoming(100, 4, &web)]);
    let winner = match results {
        [Ok(Acceptance::Committed), Err(AcceptError::Conflict { .. })] => accepted(100, 3, &dns),
        [Err(AcceptError::Conflict { .. }), Ok(Acceptance::Committed)] => accepted(100, 4, &web),
        other => panic!("expected one winner: {other:?}"),
    };
    assert_state(&store, &path, &winner);
}

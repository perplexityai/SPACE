use std::path::{Path, PathBuf};
use std::sync::Barrier;

use pplx_space_accounts::NodeId;
use pplx_space_firewall_rules_protocol::{DecodeError, decode_revision};
use rusqlite::Connection;
use rusqlite::types::Value;
use sqlx::migrate::Migrator;
use tempfile::TempDir;
use uuid::Uuid;

use super::{AcceptError, Acceptance, DesiredAssignment, FirewallAssignments, IncomingAssignment};
use crate::run::NODE_MANAGER_MIGRATOR;

/// The migration that adds `firewall_rules_assignment`.
const ASSIGNMENT_MIGRATION: i64 = 20261119205954;
const NODE: NodeId = NodeId::from_uuid(Uuid::from_u128(1));
const DNS: &str = r#"{"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}"#;
const WEB: &str = r#"{"id":"web","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]}"#;

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

fn migrate(path: &Path, migrator: &Migrator) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(pplx_space_sqlite::migrate("node-manager", path, migrator))
        .unwrap();
}

fn migrated_db() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-manager.db");
    migrate(&path, &NODE_MANAGER_MIGRATOR);
    (dir, path)
}

fn open(path: &Path) -> FirewallAssignments {
    FirewallAssignments::open(path).unwrap()
}

/// Memory, and a fresh open of the file, both hold `expected`.
fn assert_state(store: &FirewallAssignments, path: &Path, expected: &DesiredAssignment) {
    assert_eq!(store.current().as_deref(), Some(expected), "in memory");
    assert_eq!(open(path).current().as_deref(), Some(expected), "on disk");
}

/// Catches a migration that disturbs slot rows, a fresh table read as an
/// accepted assignment, a writer left at NORMAL, and an acceptance that
/// doesn't survive reopening with its ordered rules, empty rules included.
#[test]
fn migrated_database_keeps_slots_and_reopens_accepted_assignments() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node-manager.db");
    let before_assignments = Migrator {
        migrations: NODE_MANAGER_MIGRATOR
            .iter()
            .filter(|migration| migration.version < ASSIGNMENT_MIGRATION)
            .cloned()
            .collect::<Vec<_>>()
            .into(),
        ..Migrator::DEFAULT
    };
    migrate(&path, &before_assignments);
    let slots = || -> Vec<Vec<Value>> {
        let conn = Connection::open(&path).unwrap();
        let mut stmt = conn
            .prepare("SELECT * FROM network_slots ORDER BY slot_id")
            .unwrap();
        let columns = stmt.column_count();
        stmt.query_map([], |row| (0..columns).map(|i| row.get(i)).collect())
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    Connection::open(&path)
        .unwrap()
        .execute(
            "INSERT INTO network_slots (slot_id, slot_label, generation, schema_version, state,
                 health, sandbox_id, created_at, updated_at)
             VALUES (3, 'slot-3', 7, 1, 'leased', 'healthy', 'sbx-a', 10, 20)",
            [],
        )
        .unwrap();
    let populated = slots();
    migrate(&path, &NODE_MANAGER_MIGRATOR);
    assert_eq!(slots(), populated);

    let store = open(&path);
    assert_eq!(store.current(), None);
    {
        let writer = store.writer.lock().unwrap();
        let pragma = |name| writer.pragma_query_value(None, name, |row| row.get::<_, Value>(0));
        assert_eq!(pragma("journal_mode").unwrap(), Value::Text("wal".into()));
        assert_eq!(pragma("synchronous").unwrap(), Value::Integer(2));
    }
    let rules = document(&[WEB, DNS]);
    assert_eq!(
        store.accept(NODE, incoming(12, 1, &rules)).unwrap(),
        Acceptance::Committed
    );
    drop(store);
    let store = open(&path);
    assert_eq!(store.current().as_deref(), Some(&accepted(12, 1, &rules)));

    let empty = document(&[]);
    store.accept(NODE, incoming(13, 2, &empty)).unwrap();
    drop(store);
    assert_eq!(
        open(&path).current().as_deref(),
        Some(&accepted(13, 2, &empty))
    );
}

/// Catches a replay that rewrites the row or compares raw JSON, and a conflict
/// check that ignores the revision ID or compares rules unordered.
#[test]
fn same_generation_replays_exactly_or_conflicts() {
    let (_dir, path) = migrated_db();
    let store = open(&path);
    let rules = document(&[WEB, DNS]);
    store.accept(NODE, incoming(12, 1, &rules)).unwrap();
    let observer = Connection::open(&path).unwrap();
    // Changes whenever another connection commits a write.
    let data_version = || -> i64 {
        observer
            .pragma_query_value(None, "data_version", |row| row.get(0))
            .unwrap()
    };
    let unwritten = data_version();

    let reformatted = rules.replace(',', ", ");
    assert_eq!(
        store.accept(NODE, incoming(12, 1, &reformatted)).unwrap(),
        Acceptance::Replayed
    );
    assert_eq!(data_version(), unwritten, "a replay must not write");
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
    let (_dir, path) = migrated_db();
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

/// Catches an assignment that authorizes its own node, a store that rebinds to
/// another node, and invalid, unsupported or unstorable assignments that
/// reach disk or memory.
#[test]
fn refused_assignments_change_nothing() {
    let (_dir, path) = migrated_db();
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
    assert_eq!(store.current(), None);
    assert_eq!(open(&path).current(), None);

    store.accept(NODE, incoming(12, 1, &rules)).unwrap();
    let invalid = document(&[r#"{"id":"r","all_of":[]}"#]);
    let refusals = [
        store.accept(NODE, for_other(13)),
        store.accept(other, for_other(13)),
        store.accept(NODE, incoming(13, 2, &invalid)),
        store.accept(NODE, incoming(13, 2, r#"{"schema_version":2,"rules":[]}"#)),
        store.accept(NODE, incoming(1 << 63, 2, &rules)),
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
                Err(AcceptError::GenerationOutOfRange(_)),
            ]
        ),
        "{refusals:?}"
    );
    assert_state(&store, &path, &accepted(12, 1, &rules));
}

/// Catches publishing or succeeding before the commit returns, and a lost
/// reply that the identical delivery can't recover.
#[test]
fn failed_commit_publishes_nothing_and_lost_reply_replays() {
    let (_dir, path) = migrated_db();
    let store = open(&path);
    let (twelve, thirteen) = (document(&[DNS]), document(&[WEB]));
    store.accept(NODE, incoming(12, 1, &twelve)).unwrap();

    // A commit hook that returns true turns the COMMIT into a rollback.
    let set_hook = |hook: Option<fn() -> bool>| store.writer.lock().unwrap().commit_hook(hook);
    set_hook(Some(|| true)).unwrap();
    let failed = store.accept(NODE, incoming(13, 2, &thirteen));
    assert!(matches!(failed, Err(AcceptError::Storage(_))), "{failed:?}");
    assert_state(&store, &path, &accepted(12, 1, &twelve));

    set_hook(None).unwrap();
    let _lost_reply = store.accept(NODE, incoming(13, 2, &thirteen));
    assert_eq!(
        store.accept(NODE, incoming(13, 2, &thirteen)).unwrap(),
        Acceptance::Replayed
    );
    assert_state(&store, &path, &accepted(13, 2, &thirteen));
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
    let (_dir, path) = migrated_db();
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

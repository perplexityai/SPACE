use rusqlite::{OptionalExtension, params};

use pplx_space_template_model::TemplateCachePartition;

use super::*;
use crate::{Connection, test_db};

const NAME: &str = "base";
const HASH: &str = "sha256:aaaa";
const PARTITION: TemplateCachePartition = TemplateCachePartition::Legacy;

fn insert_build(conn: &Connection, state: &str, created_at: u64, last_used_at: u64) {
    conn.0
        .execute(
            "INSERT INTO template_builds
             (name, content_hash, cache_partition, build_date, manifest_json, runtime, arch,
              state, created_at, last_used_at)
         VALUES (?1, ?2, ?3, '0', '{}', 'firecracker', 'x86_64', ?4, ?5, ?6)",
            params![
                NAME,
                HASH,
                PARTITION.key(),
                state,
                created_at.to_string(),
                last_used_at.to_string()
            ],
        )
        .unwrap();
}

fn insert_component(conn: &Connection) {
    conn.0
        .execute(
            "INSERT INTO template_build_components (name, content_hash, cache_partition, kind)
         VALUES (?1, ?2, ?3, 'kernel')",
            params![NAME, HASH, PARTITION.key()],
        )
        .unwrap();
}

fn build_state(conn: &Connection) -> Option<String> {
    conn.0
        .query_row(
            "SELECT state FROM template_builds
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3",
            params![NAME, HASH, PARTITION.key()],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
}

fn component_count(conn: &Connection) -> i64 {
    conn.0
        .query_row(
            "SELECT COUNT(*) FROM template_build_components
         WHERE name = ?1 AND content_hash = ?2 AND cache_partition = ?3",
            params![NAME, HASH, PARTITION.key()],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn mark_ready_build_deleting_swaps_only_a_ready_untouched_build() {
    let conn = test_db::conn();
    assert!(!mark_ready_build_deleting(&conn, NAME, HASH, PARTITION, None).unwrap());
    assert_eq!(build_state(&conn), None);

    insert_build(&conn, "ready", 100, 200);
    assert!(!mark_ready_build_deleting(&conn, NAME, HASH, PARTITION, Some(199)).unwrap());
    assert_eq!(build_state(&conn).as_deref(), Some("ready"));

    assert!(mark_ready_build_deleting(&conn, NAME, HASH, PARTITION, Some(200)).unwrap());
    assert_eq!(build_state(&conn).as_deref(), Some("deleting"));

    assert!(!mark_ready_build_deleting(&conn, NAME, HASH, PARTITION, None).unwrap());
    assert_eq!(build_state(&conn).as_deref(), Some("deleting"));

    let conn = test_db::conn();
    insert_build(&conn, "ready", 300, 100);
    assert!(!mark_ready_build_deleting(&conn, NAME, HASH, PARTITION, Some(200)).unwrap());
    assert_eq!(build_state(&conn).as_deref(), Some("ready"));
}

#[test]
fn resets_to_ready_leave_a_build_not_deleting() {
    for reset in [
        reaper_reset_deleting_build_to_ready,
        startup_reset_deleting_build_to_ready,
    ] {
        let conn = test_db::conn();
        assert!(!reset(&conn, NAME, HASH, PARTITION).unwrap());
        assert_eq!(build_state(&conn), None);

        insert_build(&conn, "ready", 100, 100);
        assert!(!reset(&conn, NAME, HASH, PARTITION).unwrap());
        assert_eq!(build_state(&conn).as_deref(), Some("ready"));

        conn.0
            .execute("UPDATE template_builds SET state = 'deleting'", [])
            .unwrap();
        assert!(reset(&conn, NAME, HASH, PARTITION).unwrap());
        assert_eq!(build_state(&conn).as_deref(), Some("ready"));
    }
}

#[test]
fn reaper_delete_leaves_a_build_not_deleting() {
    let conn = test_db::conn();
    assert!(!reaper_delete_deleting_build(&conn, NAME, HASH, PARTITION).unwrap());
    assert_eq!(build_state(&conn), None);
    insert_build(&conn, "ready", 100, 100);
    assert!(!reaper_delete_deleting_build(&conn, NAME, HASH, PARTITION).unwrap());
    assert_eq!(build_state(&conn).as_deref(), Some("ready"));

    conn.0
        .execute("UPDATE template_builds SET state = 'deleting'", [])
        .unwrap();
    assert!(reaper_delete_deleting_build(&conn, NAME, HASH, PARTITION).unwrap());
    assert_eq!(build_state(&conn), None);
}

#[test]
fn delete_components_of_absent_build_keeps_a_present_builds_components() {
    let conn = test_db::conn();
    insert_build(&conn, "deleting", 100, 100);
    insert_component(&conn);
    assert_eq!(
        delete_components_of_absent_build(&conn, NAME, HASH, PARTITION).unwrap(),
        0
    );
    assert_eq!(component_count(&conn), 1);

    // Delete the build row without cascading, leaving its components behind.
    conn.0.pragma_update(None, "foreign_keys", false).unwrap();
    conn.0.execute("DELETE FROM template_builds", []).unwrap();
    assert_eq!(component_count(&conn), 1);
    assert_eq!(
        delete_components_of_absent_build(&conn, NAME, HASH, PARTITION).unwrap(),
        1
    );
    assert_eq!(component_count(&conn), 0);
}

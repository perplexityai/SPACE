use super::*;
use crate::test_db;

fn key(name: &str, other: &str) -> (String, String) {
    (name.to_string(), other.to_string())
}

fn tag_pins(conn: &Connection) -> Vec<(String, String, String)> {
    let mut stmt = conn
        .0
        .prepare(
            "SELECT name, tag, cache_partition FROM template_pinned_tags
             ORDER BY name, tag, cache_partition",
        )
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn release_pins(conn: &Connection) -> Vec<(String, String)> {
    let mut stmt = conn
        .0
        .prepare(
            "SELECT name, content_hash FROM template_pinned_releases ORDER BY name, content_hash",
        )
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}

fn seed_release_pins(conn: &Connection) {
    for (name, hash) in [
        ("a", "h1"),
        ("a", "h2"),
        ("a", "h3"),
        ("b", "h1"),
        ("b", "h2"),
        ("c", "h1"),
    ] {
        upsert_release_pin(conn, name, hash, "release", "0", "0").unwrap();
    }
}

#[test]
fn delete_tag_pins_outside_keeps_only_the_listed_tags_in_every_partition() {
    let conn = test_db::conn();
    for (name, tag, partition) in [
        ("a", "latest", ""),
        ("a", "latest", "env"),
        ("a", "stable", ""),
        ("b", "latest", ""),
        ("b", "beta", "env"),
        ("c", "latest", ""),
    ] {
        upsert_tag_pin(&conn, name, tag, partition, "hash", "0").unwrap();
    }

    delete_tag_pins_outside(&conn, &[key("a", "latest"), key("b", "beta")]).unwrap();

    assert_eq!(
        tag_pins(&conn),
        [
            ("a".into(), "latest".into(), "".into()),
            ("a".into(), "latest".into(), "env".into()),
            ("b".into(), "beta".into(), "env".into()),
        ]
    );

    delete_tag_pins_outside(&conn, &[key("b", "beta")]).unwrap();

    assert_eq!(tag_pins(&conn), [("b".into(), "beta".into(), "env".into())]);
}

#[test]
fn delete_release_pins_of_templates_leaves_other_templates_pins() {
    let conn = test_db::conn();
    seed_release_pins(&conn);

    delete_release_pins_of_templates(&conn, &["a".to_string(), "c".to_string()]).unwrap();

    assert_eq!(release_pins(&conn), [key("b", "h1"), key("b", "h2")]);
}

#[test]
fn delete_release_pins_of_templates_outside_keeps_the_listed_keys_and_other_templates() {
    let conn = test_db::conn();
    seed_release_pins(&conn);

    delete_release_pins_of_templates_outside(
        &conn,
        &["a".to_string(), "b".to_string()],
        &[key("a", "h1"), key("a", "h3"), key("b", "h2")],
    )
    .unwrap();

    assert_eq!(
        release_pins(&conn),
        [
            key("a", "h1"),
            key("a", "h3"),
            key("b", "h2"),
            key("c", "h1")
        ]
    );

    delete_release_pins_of_templates_outside(&conn, &["a".to_string()], &[key("a", "h3")]).unwrap();

    assert_eq!(
        release_pins(&conn),
        [key("a", "h3"), key("b", "h2"), key("c", "h1")]
    );
}

use rusqlite::params;

use pplx_space_policy_authority::PolicyAuthority;

use super::*;
use crate::{Connection, test_db};

const TEMPLATE: &str = "base";
const POLICY_JSON: &str = r#"{"version":1}"#;

fn insert_notification(conn: &Connection, generation: i64, authority: &str) {
    conn.0
        .execute(
            "INSERT INTO template_network_policy_notifications
             (template, policy_json, generation, authority, requested_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, '0', '0')",
            params![TEMPLATE, POLICY_JSON, generation, authority],
        )
        .unwrap();
}

fn queued(conn: &Connection) -> i64 {
    conn.0
        .query_row(
            "SELECT COUNT(*) FROM template_network_policy_notifications WHERE template = ?1",
            params![TEMPLATE],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn acknowledge_keeps_a_notification_that_is_not_the_one_sent() {
    let conn = test_db::conn();
    assert_eq!(
        acknowledge(&conn, TEMPLATE, 3, PolicyAuthority::Cluster, POLICY_JSON).unwrap(),
        0
    );

    insert_notification(&conn, 3, "cluster");
    for (generation, authority, policy_json) in [
        (2, PolicyAuthority::Cluster, POLICY_JSON),
        (3, PolicyAuthority::Local, POLICY_JSON),
        (3, PolicyAuthority::Cluster, r#"{"version":2}"#),
    ] {
        assert_eq!(
            acknowledge(&conn, TEMPLATE, generation, authority, policy_json).unwrap(),
            0
        );
        assert_eq!(queued(&conn), 1);
    }

    assert_eq!(
        acknowledge(&conn, TEMPLATE, 3, PolicyAuthority::Cluster, POLICY_JSON).unwrap(),
        1
    );
    assert_eq!(queued(&conn), 0);
}

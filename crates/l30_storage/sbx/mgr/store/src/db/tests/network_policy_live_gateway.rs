//! The live-gateway answer behind `notified_gateways` and the network-policy
//! repair decision: a sandbox counts only while it has a registered gateway
//! and is not a pool member.

use super::*;

struct GatewayRow {
    name: &'static str,
    netgtw_pid: Option<i64>,
    pool_template: Option<&'static str>,
    live: bool,
}

const GATEWAY_ROWS: [GatewayRow; 4] = [
    GatewayRow {
        name: "registered gateway",
        netgtw_pid: Some(4242),
        pool_template: None,
        live: true,
    },
    GatewayRow {
        name: "no gateway",
        netgtw_pid: None,
        pool_template: None,
        live: false,
    },
    GatewayRow {
        name: "pool member with a gateway",
        netgtw_pid: Some(4242),
        pool_template: Some("seed"),
        live: false,
    },
    GatewayRow {
        name: "pool member without a gateway",
        netgtw_pid: None,
        pool_template: Some("seed"),
        live: false,
    },
];

fn insert_gateway_row(conn: &Connection, row: &GatewayRow) {
    insert_test_sandbox(
        conn,
        TestSandboxInsert {
            network_policy_override: Some(NetworkPolicyV1::deny_all()),
            network_policy_generation: 3,
            pool_template: row.pool_template.map(str::to_string),
            ..TestSandboxInsert::new("sandbox", "gateway1", SandboxState::Started)
        },
    );
    conn.execute(
        "UPDATE sandboxes SET netgtw_pid = ?1 WHERE id = 'sandbox'",
        params![row.netgtw_pid],
    )
    .unwrap();
}

#[test]
fn live_gateway_read_requires_a_gateway_and_no_pool_template() {
    for row in &GATEWAY_ROWS {
        let conn = test_conn();
        insert_gateway_row(&conn, row);
        // Asked twice as a control against the old upsert, which answered a
        // repeated call through its conflict arm rather than its insert.
        for call in ["first", "repeated"] {
            assert_eq!(
                has_live_network_policy_gateway(&conn, "sandbox").unwrap(),
                row.live,
                "{}: {call} call",
                row.name
            );
        }
    }

    // Another sandbox's live gateway does not answer for a missing one.
    let conn = test_conn();
    insert_gateway_row(&conn, &GATEWAY_ROWS[0]);
    assert!(!has_live_network_policy_gateway(&conn, "missing").unwrap());
}

#[test]
fn unchanged_policy_repairs_only_a_live_non_pool_gateway() {
    for row in &GATEWAY_ROWS {
        let conn = test_conn();
        insert_gateway_row(&conn, row);
        let expected = NetworkPolicyOverrideUpdateOutcome {
            decision: if row.live {
                NetworkPolicyOverrideUpdate::Repair
            } else {
                NetworkPolicyOverrideUpdate::Skip
            },
            queued_live_gateway: row.live,
            queued_dirty_ref: false,
        };
        assert_eq!(
            update_network_policy_override(
                &conn,
                "sandbox",
                Some(&NetworkPolicyV1::deny_all()),
                3,
                PolicyAuthority::Cluster,
                true,
                true,
            )
            .unwrap(),
            expected,
            "{}",
            row.name
        );
    }
}

#[test]
fn applied_policy_reaches_only_a_live_non_pool_gateway() {
    for row in &GATEWAY_ROWS {
        let conn = test_conn();
        insert_gateway_row(&conn, row);
        assert_eq!(
            update_network_policy_override(
                &conn,
                "sandbox",
                Some(&NetworkPolicyV1::allow_all()),
                4,
                PolicyAuthority::Cluster,
                true,
                false,
            )
            .unwrap(),
            NetworkPolicyOverrideUpdateOutcome {
                decision: NetworkPolicyOverrideUpdate::Apply,
                queued_live_gateway: row.live,
                queued_dirty_ref: false,
            },
            "{}",
            row.name
        );
    }
}

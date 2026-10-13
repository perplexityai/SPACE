//! `clear_local_network_policy`: clearing a sandbox's local network-policy
//! override hands policy authority back to the cluster.

use super::*;

#[test]
fn clearing_a_local_network_policy_hands_authority_back_to_the_cluster() {
    struct Case {
        name: &'static str,
        authority: PolicyAuthority,
        live_gateway: bool,
        enqueue_live_gateway: bool,
        mark_dirty: bool,
        expected: NetworkPolicyOverrideUpdateOutcome,
    }
    let outcome =
        |decision, queued_live_gateway, queued_dirty_ref| NetworkPolicyOverrideUpdateOutcome {
            decision,
            queued_live_gateway,
            queued_dirty_ref,
        };
    let cases = [
        Case {
            name: "local, live gateway, publish",
            authority: PolicyAuthority::Local,
            live_gateway: true,
            enqueue_live_gateway: true,
            mark_dirty: true,
            expected: outcome(NetworkPolicyOverrideUpdate::Apply, true, true),
        },
        Case {
            name: "local, no live gateway",
            authority: PolicyAuthority::Local,
            live_gateway: false,
            enqueue_live_gateway: true,
            mark_dirty: false,
            expected: outcome(NetworkPolicyOverrideUpdate::Apply, false, false),
        },
        Case {
            name: "local, live gateway not requested",
            authority: PolicyAuthority::Local,
            live_gateway: true,
            enqueue_live_gateway: false,
            mark_dirty: true,
            expected: outcome(NetworkPolicyOverrideUpdate::Apply, false, true),
        },
        Case {
            name: "cluster, live gateway",
            authority: PolicyAuthority::Cluster,
            live_gateway: true,
            enqueue_live_gateway: true,
            mark_dirty: true,
            expected: outcome(NetworkPolicyOverrideUpdate::Repair, true, false),
        },
        Case {
            name: "cluster, live gateway not requested",
            authority: PolicyAuthority::Cluster,
            live_gateway: true,
            enqueue_live_gateway: false,
            mark_dirty: true,
            expected: outcome(NetworkPolicyOverrideUpdate::Skip, false, false),
        },
        Case {
            name: "cluster, no live gateway",
            authority: PolicyAuthority::Cluster,
            live_gateway: false,
            enqueue_live_gateway: true,
            mark_dirty: true,
            expected: outcome(NetworkPolicyOverrideUpdate::Skip, false, false),
        },
    ];
    for case in cases {
        let name = case.name;
        let conn = test_conn();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert {
                network_policy_override: Some(NetworkPolicyV1::deny_all()),
                network_policy_generation: 5,
                network_policy_authority: case.authority,
                ..TestSandboxInsert::new("sandbox", "policy01", SandboxState::Started)
            },
        );
        if case.live_gateway {
            conn.execute(
                "UPDATE sandboxes SET netgtw_pid = 4242 WHERE id = 'sandbox'",
                [],
            )
            .unwrap();
        }

        assert_eq!(
            clear_local_network_policy(
                &conn,
                "sandbox",
                case.enqueue_live_gateway,
                case.mark_dirty,
            )
            .unwrap(),
            case.expected,
            "{name}"
        );

        let row = get_sandbox(&conn, "sandbox").unwrap().unwrap();
        assert_eq!(
            row.network_policy_authority,
            PolicyAuthority::Cluster,
            "{name}"
        );
        if case.authority == PolicyAuthority::Local {
            assert_eq!(row.network_policy_override, None, "{name}");
            assert_eq!(row.network_policy_generation, 0, "{name}");
        } else {
            assert_eq!(
                row.network_policy_override,
                Some(NetworkPolicyV1::deny_all()),
                "{name}: cluster-owned policy must be left alone"
            );
            assert_eq!(row.network_policy_generation, 5, "{name}");
        }
        let queued: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM network_policy_reconcile_queue WHERE sandbox_id = 'sandbox'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(queued == 1, case.expected.queued_live_gateway, "{name}");
        let dirty = list_node_dirty_refs(&conn, 16)
            .unwrap()
            .into_iter()
            .any(|reference| {
                reference.element_kind == "network_policy_sandbox"
                    && reference.element_value == "sandbox"
            });
        assert_eq!(dirty, case.expected.queued_dirty_ref, "{name}");
    }

    let conn = test_conn();
    assert!(matches!(
        clear_local_network_policy(&conn, "missing", true, true),
        Err(rusqlite::Error::QueryReturnedNoRows)
    ));
}

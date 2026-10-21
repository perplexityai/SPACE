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
                NetworkPolicyOverrideUpdateOptions {
                    enqueue_live_gateway: true,
                    mark_dirty: true,
                    ..Default::default()
                },
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
                NetworkPolicyOverrideUpdateOptions {
                    enqueue_live_gateway: true,
                    ..Default::default()
                },
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

#[test]
fn cluster_seed_divergence_repair_persists_and_is_idempotent() {
    let mut desired = NetworkPolicyV1::deny_all();
    desired.tls_interception = pplx_space_net_common::TlsInterception::On;
    desired.tls_interception_exemptions = vec![pplx_space_net_common::EgressDestinationV1::Domain(
        "relay.example.test".parse().unwrap(),
    )];
    let mut seed = desired.clone();
    seed.tls_interception_exemptions.clear();

    for (seed, desired) in [
        (Some(seed.clone()), Some(desired.clone())),
        (Some(seed), None),
        (None, Some(desired)),
    ] {
        let conn = test_conn();
        insert_test_sandbox(
            &conn,
            TestSandboxInsert {
                network_policy_override: seed,
                network_policy_generation: 7,
                network_policy_authority: PolicyAuthority::Cluster,
                ..TestSandboxInsert::new("sandbox", "seed0001", SandboxState::Started)
            },
        );
        conn.execute(
            "UPDATE sandboxes SET netgtw_pid = 4242 WHERE id = 'sandbox'",
            [],
        )
        .unwrap();
        let apply = || {
            update_network_policy_override(
                &conn,
                "sandbox",
                desired.as_ref(),
                7,
                PolicyAuthority::Cluster,
                NetworkPolicyOverrideUpdateOptions {
                    enqueue_live_gateway: true,
                    mark_dirty: true,
                    allow_seed_divergence_repair: true,
                },
            )
            .unwrap()
        };
        let outcome = apply();
        let row = get_sandbox(&conn, "sandbox").unwrap().unwrap();
        assert_eq!(
            outcome.decision,
            NetworkPolicyOverrideUpdate::RepairDivergence,
            "row_matches_requested={}",
            row.network_policy_override == desired
        );
        assert_eq!(row.network_policy_override, desired);
        assert_eq!(row.network_policy_generation, 7);
        assert_eq!(row.network_policy_authority, PolicyAuthority::Cluster);
        assert!(outcome.queued_live_gateway);
        assert!(outcome.queued_dirty_ref);
        let dirty = list_node_dirty_refs(&conn, 16).unwrap();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].element_kind, "network_policy_sandbox");
        assert_eq!(dirty[0].element_value, "sandbox");
        delete_node_dirty_refs(&conn, &dirty).unwrap();

        assert_eq!(
            apply(),
            NetworkPolicyOverrideUpdateOutcome {
                decision: NetworkPolicyOverrideUpdate::Repair,
                queued_live_gateway: true,
                queued_dirty_ref: false,
            }
        );
        assert!(list_node_dirty_refs(&conn, 16).unwrap().is_empty());
        let row = get_sandbox(&conn, "sandbox").unwrap().unwrap();
        assert_eq!(row.network_policy_override, desired);
        assert_eq!(row.network_policy_generation, 7);
    }
}

#[test]
fn cluster_seed_divergence_repair_preserves_other_decisions() {
    use NetworkPolicyOverrideUpdatePlan::{Apply, Conflict, RepairIfLiveGateway, Skip};
    for allow_repair in [false, true] {
        for (existing_authority, requested_authority, generation, expected) in [
            (PolicyAuthority::Cluster, PolicyAuthority::Cluster, 3, Skip),
            (
                PolicyAuthority::Cluster,
                PolicyAuthority::Cluster,
                4,
                if allow_repair {
                    Apply {
                        generation: 4,
                        authority: PolicyAuthority::Cluster,
                        repair_divergence: true,
                    }
                } else {
                    Conflict
                },
            ),
            (
                PolicyAuthority::Cluster,
                PolicyAuthority::Cluster,
                5,
                Apply {
                    generation: 5,
                    authority: PolicyAuthority::Cluster,
                    repair_divergence: false,
                },
            ),
            (PolicyAuthority::Local, PolicyAuthority::Cluster, 4, Skip),
            (
                PolicyAuthority::Cluster,
                PolicyAuthority::Local,
                4,
                Apply {
                    generation: 0,
                    authority: PolicyAuthority::Local,
                    repair_divergence: false,
                },
            ),
            (
                PolicyAuthority::Local,
                PolicyAuthority::Local,
                4,
                Apply {
                    generation: 0,
                    authority: PolicyAuthority::Local,
                    repair_divergence: false,
                },
            ),
        ] {
            assert_eq!(
                decide_network_policy_override_update(
                    NetworkPolicyOverrideExisting {
                        json: Some("seed"),
                        generation: 4,
                        authority: existing_authority,
                    },
                    NetworkPolicyOverrideRequested {
                        json: Some("desired"),
                        generation,
                        authority: requested_authority,
                    },
                    allow_repair,
                ),
                expected
            );
        }
        for authority in [PolicyAuthority::Local, PolicyAuthority::Cluster] {
            assert_eq!(
                decide_network_policy_override_update(
                    NetworkPolicyOverrideExisting {
                        json: Some("same"),
                        generation: 4,
                        authority,
                    },
                    NetworkPolicyOverrideRequested {
                        json: Some("same"),
                        generation: 4,
                        authority,
                    },
                    allow_repair,
                ),
                RepairIfLiveGateway
            );
        }
    }
    for (generation, allow_repair) in [(0, true), (0, false), (4, false)] {
        assert_eq!(
            decide_network_policy_override_update(
                NetworkPolicyOverrideExisting {
                    json: Some("seed"),
                    generation,
                    authority: PolicyAuthority::Cluster,
                },
                NetworkPolicyOverrideRequested {
                    json: Some("desired"),
                    generation,
                    authority: PolicyAuthority::Cluster,
                },
                allow_repair,
            ),
            Conflict
        );
    }
}

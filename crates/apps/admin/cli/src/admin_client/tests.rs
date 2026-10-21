use super::*;
use pplx_space_common_core::types::{
    NodeId, NodeManagedBy, NodeRole, admin::AdminNodeDrainability,
};

#[test]
fn action_url_appends_colon_action() {
    let url = action_url(
        "http://127.0.0.1:8080",
        "11111111-1111-1111-1111-111111111111",
        "drain",
    );
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes/11111111-1111-1111-1111-111111111111:drain"
    );
}

#[test]
fn action_url_trims_trailing_slash_on_base() {
    let url = action_url(
        "http://127.0.0.1:8080/",
        "11111111-1111-1111-1111-111111111111",
        "terminate",
    );
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes/11111111-1111-1111-1111-111111111111:terminate"
    );
}

#[test]
fn action_url_supports_https_with_path_prefix() {
    let url = action_url(
        "https://space.example.com",
        "22222222-2222-2222-2222-222222222222",
        "undrain",
    );
    assert_eq!(
        url,
        "https://space.example.com/admin/api/v1/nodes/22222222-2222-2222-2222-222222222222:undrain"
    );
}

fn row(id: &str) -> AdminNodeResponse {
    AdminNodeResponse {
        id: NodeId::from_uuid(Uuid::parse_str(id).expect("test row id must be a valid uuid")),
        name: None,
        address: None,
        node_api_endpoint: None,
        role: NodeRole::Worker,
        managed_by: NodeManagedBy::Kubernetes,
        state: None,
        scheduling_status: None,
        scheduling_reason: None,
        drain_phase: None,
        drain_mode: None,
        drain_started_at: None,
        drain_last_error: None,
        terminate_decrement_capacity: false,
        k8s_node_name: None,
        metadata: serde_json::Value::Null,
        last_heartbeat: None,
        created_at: "2026-05-14T00:00:00Z".to_string(),
        updated_at: None,
        deleted_at: None,
        sandboxes_active: Some(0),
        sandboxes_local_snapshots: Some(0),
        drainability: None,
        residency_evidence: None,
        network_slots: AdminNodeNetworkSlots::default(),
        status: None,
    }
}

#[test]
fn resolve_prefix_returns_full_id_on_unique_match() {
    let nodes = vec![
        row("11111111-aaaa-bbbb-cccc-dddddddddddd"),
        row("22222222-eeee-ffff-1111-222222222222"),
    ];
    let resolved = resolve_prefix("11111111", &nodes).unwrap();
    assert_eq!(resolved, "11111111-aaaa-bbbb-cccc-dddddddddddd");
}

#[test]
fn resolve_prefix_errors_on_no_match() {
    let nodes = vec![row("11111111-aaaa-bbbb-cccc-dddddddddddd")];
    let err = resolve_prefix("99999999", &nodes).unwrap_err();
    match err {
        CliError::Argument(msg) => assert!(msg.contains("no node matches")),
        other => panic!("expected Argument, got {other:?}"),
    }
}

#[test]
fn resolve_prefix_matches_exact_k8s_node_name() {
    let mut a = row("11111111-aaaa-bbbb-cccc-dddddddddddd");
    a.k8s_node_name = Some("ip-192-0-2-180.ec2.internal".to_string());
    let b = row("22222222-eeee-ffff-1111-222222222222");
    let nodes = vec![a, b];
    let resolved = resolve_prefix("ip-192-0-2-180.ec2.internal", &nodes).unwrap();
    assert_eq!(resolved, "11111111-aaaa-bbbb-cccc-dddddddddddd");
}

#[test]
fn resolve_prefix_matches_exact_address() {
    let mut a = row("11111111-aaaa-bbbb-cccc-dddddddddddd");
    a.address = Some("192.0.2.125".parse().unwrap());
    let b = row("22222222-eeee-ffff-1111-222222222222");
    let nodes = vec![a, b];
    let resolved = resolve_prefix("192.0.2.125", &nodes).unwrap();
    assert_eq!(resolved, "11111111-aaaa-bbbb-cccc-dddddddddddd");
}

#[test]
fn resolve_prefix_k8s_node_name_requires_exact_match() {
    let mut a = row("11111111-aaaa-bbbb-cccc-dddddddddddd");
    a.k8s_node_name = Some("ip-192-0-2-180.ec2.internal".to_string());
    let nodes = vec![a];
    // A bare prefix of the k8s name is not enough; only id prefixes fall
    // back to prefix matching, and "ip-192" is neither an id prefix nor an
    // exact name.
    let err = resolve_prefix("ip-192", &nodes).unwrap_err();
    match err {
        CliError::Argument(msg) => assert!(msg.contains("no node matches")),
        other => panic!("expected Argument, got {other:?}"),
    }
}

#[test]
fn resolve_prefix_exact_name_match_wins_over_id_prefix() {
    // `a`'s k8s name equals the reference; `b`'s id starts with it. The
    // exact name match must short-circuit before the id-prefix fallback.
    let mut a = row("11111111-aaaa-bbbb-cccc-dddddddddddd");
    a.k8s_node_name = Some("33333333".to_string());
    let b = row("33333333-eeee-ffff-1111-222222222222");
    let nodes = vec![a, b];
    let resolved = resolve_prefix("33333333", &nodes).unwrap();
    assert_eq!(resolved, "11111111-aaaa-bbbb-cccc-dddddddddddd");
}

#[test]
fn resolve_prefix_matches_short_exact_name_below_prefix_min() {
    // A short exact k8s name (under the 4-char id-prefix minimum) must
    // still resolve — the length floor only guards the prefix fallback.
    let mut a = row("11111111-aaaa-bbbb-cccc-dddddddddddd");
    a.k8s_node_name = Some("n1".to_string());
    let nodes = vec![a];
    let resolved = resolve_prefix("n1", &nodes).unwrap();
    assert_eq!(resolved, "11111111-aaaa-bbbb-cccc-dddddddddddd");
}

#[test]
fn resolve_prefix_rejects_short_id_prefix_with_no_exact_match() {
    // "12" is under the 4-char id-prefix floor and matches no name/address,
    // so it's rejected rather than prefix-matched against the id column.
    let nodes = vec![row("12345678-aaaa-bbbb-cccc-dddddddddddd")];
    let err = resolve_prefix("12", &nodes).unwrap_err();
    match err {
        CliError::Argument(msg) => {
            assert!(msg.contains("no node matches"));
            assert!(msg.contains("at least 4 characters"));
        }
        other => panic!("expected Argument, got {other:?}"),
    }
}

#[test]
fn resolve_prefix_errors_on_ambiguous_exact_name_match() {
    let mut a = row("11111111-aaaa-0000-0000-000000000000");
    a.k8s_node_name = Some("192.0.2.125".to_string());
    let mut b = row("22222222-bbbb-0000-0000-000000000000");
    b.address = Some("192.0.2.125".parse().unwrap());
    let nodes = vec![a, b];
    let err = resolve_prefix("192.0.2.125", &nodes).unwrap_err();
    match err {
        CliError::Argument(msg) => {
            assert!(msg.contains("ambiguous"));
            assert!(msg.contains("11111111-aaaa"));
            assert!(msg.contains("22222222-bbbb"));
        }
        other => panic!("expected Argument, got {other:?}"),
    }
}

#[test]
fn resolve_prefix_errors_on_ambiguous_match_listing_every_match() {
    let nodes = vec![
        row("11111111-aaaa-0000-0000-000000000000"),
        row("11111111-bbbb-0000-0000-000000000000"),
        row("22222222-cccc-0000-0000-000000000000"),
    ];
    let err = resolve_prefix("11111111", &nodes).unwrap_err();
    match err {
        CliError::Argument(msg) => {
            assert!(msg.contains("ambiguous"));
            assert!(msg.contains("11111111-aaaa"));
            assert!(msg.contains("11111111-bbbb"));
            assert!(!msg.contains("22222222"));
        }
        other => panic!("expected Argument, got {other:?}"),
    }
}

#[test]
fn interpret_response_pretty_prints_success_json() {
    let body = r#"{"id":"abc","drain_phase":"draining"}"#;
    let result = interpret_response(StatusCode::OK, body).unwrap();
    assert!(result.contains("\"id\": \"abc\""));
    assert!(result.contains("\"drain_phase\": \"draining\""));
}

#[test]
fn interpret_response_passes_through_non_json_success_body() {
    let result = interpret_response(StatusCode::OK, "ok").unwrap();
    assert_eq!(result, "ok");
}

#[test]
fn interpret_response_propagates_error_body_verbatim_for_json() {
    let body = r#"{"error":"drain-phase transition not allowed"}"#;
    let err = interpret_response(StatusCode::CONFLICT, body).unwrap_err();
    match err {
        CliError::Api { status, body } => {
            assert_eq!(status, 409);
            assert!(body.contains("\"error\": \"drain-phase transition not allowed\""));
        }
        other => panic!("expected CliError::Api, got {other:?}"),
    }
}

#[test]
fn interpret_response_propagates_error_body_for_plain_text() {
    let err = interpret_response(StatusCode::INTERNAL_SERVER_ERROR, "boom").unwrap_err();
    match err {
        CliError::Api { status, body } => {
            assert_eq!(status, 500);
            assert_eq!(body, "boom");
        }
        other => panic!("expected CliError::Api, got {other:?}"),
    }
}

#[test]
fn list_url_without_filters_omits_query_string() {
    let url = list_url("http://127.0.0.1:8080", None, None, false, None, None);
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes?include_occupancy=false"
    );
}

#[test]
fn list_url_with_drain_phase_appends_query() {
    let url = list_url(
        "http://127.0.0.1:8080",
        Some(DrainPhaseFilter::Draining),
        None,
        false,
        None,
        None,
    );
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes?include_occupancy=false&drain_phase=draining"
    );
}

#[test]
fn list_url_combines_filters_in_a_stable_order() {
    let url = list_url(
        "http://127.0.0.1:8080/",
        Some(DrainPhaseFilter::Drained),
        Some(RoleFilter::Worker),
        true,
        None,
        None,
    );
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes?include_occupancy=false&drain_phase=drained&role=worker&include_deleted=true"
    );
}

#[test]
fn list_url_maps_drain_phase_none_to_query_value_none() {
    let url = list_url(
        "http://127.0.0.1:8080",
        Some(DrainPhaseFilter::None),
        None,
        false,
        None,
        None,
    );
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes?include_occupancy=false&drain_phase=none"
    );
}

#[test]
fn single_node_url_appends_uuid() {
    let url = single_node_url(
        "http://127.0.0.1:8080",
        "11111111-1111-1111-1111-111111111111",
        false,
    );
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes/11111111-1111-1111-1111-111111111111?include_occupancy=false"
    );
}

#[test]
fn warm_pools_url_trims_trailing_slash() {
    let url = warm_pools_url("http://127.0.0.1:8080/");
    assert_eq!(url, "http://127.0.0.1:8080/admin/api/v1/warm-pools");
}

#[test]
fn render_nodes_table_handles_empty_input() {
    let out = render_nodes_table(&[], false);
    assert!(out.contains("no nodes match"));
}

#[test]
fn render_nodes_table_shows_drain_phase_and_started_at() {
    let mut a = row("11111111-1111-1111-1111-111111111111");
    a.name = Some("worker-a".to_string());
    a.drain_phase = Some("draining".to_string());
    a.drain_started_at = Some("2026-05-14T01:23:45Z".to_string());
    a.sandboxes_active = Some(3);
    a.sandboxes_local_snapshots = Some(1);
    a.last_heartbeat = Some("2026-05-14T01:25:00Z".to_string());
    a.scheduling_status = Some("ready".to_string());
    a.network_slots = AdminNodeNetworkSlots {
        ready: Some(299),
        total: Some(300),
        leased: Some(1),
        ..Default::default()
    };
    let b = row("22222222-2222-2222-2222-222222222222");
    let out = render_nodes_table(&[a, b], false);
    // Snapshot pins the whole rendered table — a superset of the twelve
    // presence/absence checks the ladder ran. Inputs are fixed literals
    // (UUIDs, timestamps), so the render is deterministic and needs no
    // redaction.
    insta::assert_snapshot!(out);
}

#[test]
fn render_nodes_table_verbose_adds_k8s_state_and_error_columns() {
    let mut a = row("11111111-1111-1111-1111-111111111111");
    a.k8s_node_name = Some("ip-192-0-2-125.ec2.internal".to_string());
    a.state = Some("registered".to_string());
    a.drain_last_error = Some("cordon patch failed: timeout".to_string());
    let out = render_nodes_table(&[a], true);
    assert!(out.contains("K8S NODE"));
    assert!(out.contains("STATE"));
    assert!(out.contains("DRAIN ERROR"));
    assert!(out.contains("ip-192-0-2-125.ec2.internal"));
    assert!(out.contains("registered"));
    assert!(out.contains("cordon patch failed: timeout"));
}

#[test]
fn render_node_detail_emits_aligned_key_value_pairs() {
    let mut node = row("11111111-1111-1111-1111-111111111111");
    node.name = Some("ip-192-0-2-125.ec2.internal".to_string());
    node.address = Some("192.0.2.125".parse().unwrap());
    node.role = NodeRole::Builder;
    node.state = Some("registered".to_string());
    node.scheduling_status = Some("ready".to_string());
    node.scheduling_reason = Some("enough ready slots".to_string());
    node.network_slots = AdminNodeNetworkSlots {
        ready: Some(298),
        min_ready: Some(10),
        degraded: Some(1),
        quarantined: Some(0),
        total: Some(300),
        leased: Some(2),
        max_slots: Some(300),
    };
    node.k8s_node_name = Some("ip-192-0-2-125.ec2.internal".to_string());
    node.drain_phase = Some("draining".to_string());
    node.drain_started_at = Some("2026-05-14T01:23:45Z".to_string());
    node.drain_last_error = Some("cordon patch failed".to_string());
    node.last_heartbeat = Some("2026-05-07T18:34:01.282553Z".to_string());
    node.sandboxes_active = Some(2);
    node.sandboxes_local_snapshots = Some(1);
    let out = render_node_detail(&node);
    // Snapshot pins the whole rendered detail view — a superset of the
    // eleven `contains` checks plus the newline-count shape assertion the
    // ladder ran (full ID and hostname visibility, one label per line, and
    // every field present). Inputs are fixed literals, so the render is
    // deterministic and needs no redaction.
    insta::assert_snapshot!(out);
}

#[test]
fn render_node_detail_uses_dash_for_nulls() {
    let node = row("11111111-1111-1111-1111-111111111111");
    let out = render_node_detail(&node);
    // No drain in progress, no name, no address — all rendered as "-".
    // Padding adjusts to the longest label automatically, so the test
    // only asserts the label, the separator gap, and the dash without
    // baking in a specific column width.
    for label in ["Name", "Drain Phase", "Network Slots"] {
        assert!(
            out.lines()
                .any(|line| line.starts_with(label) && line.trim_end().ends_with(" -")),
            "expected `{label}` line to end with `-`, got:\n{out}"
        );
    }
}

#[test]
fn render_warm_pool_config_emits_config_and_slot_counts() {
    let config = WarmPoolConfigResponse {
        id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        template_release_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
        template_name: "app-runtime".to_string(),
        content_hash: "sha256:abc".to_string(),
        vcpu: 2,
        memory_mib: 8192,
        target_ready_slots: 10,
        refill_per_tick: 5,
        max_warming_slots: 10,
        namespace: "sandboxes".to_string(),
        enabled: true,
        warming_slots: 1,
        ready_slots: 9,
        claimed_slots: 3,
        created_at: "2026-05-19T00:00:00Z".to_string(),
        updated_at: "2026-05-19T00:01:00Z".to_string(),
    };

    let out = render_warm_pool_config(&config);

    assert!(out.contains("app-runtime"));
    assert!(out.contains("sha256:abc"));
    assert!(out.contains("Target Ready Slots"));
    assert!(out.contains("Ready Slots"));
    assert!(out.contains("9"));
}

#[test]
fn render_warm_pool_table_handles_empty_input() {
    let out = render_warm_pool_table(&[]);
    assert!(out.contains("no warm-pool configs"));
}

#[test]
fn render_warm_pool_table_emits_summary_columns() {
    let config = warm_pool("11111111-1111-1111-1111-111111111111", "app-runtime");
    let out = render_warm_pool_table(&[config]);

    assert!(out.contains("app-runtime"));
    assert!(out.contains("TARGET"));
    assert!(out.contains("READY"));
    assert!(out.contains("11111111"));
}

#[test]
fn resolve_warm_pool_ref_matches_unique_id_prefix() {
    let configs = vec![
        warm_pool("11111111-1111-1111-1111-111111111111", "app-runtime"),
        warm_pool("22222222-2222-2222-2222-222222222222", "seed"),
    ];

    let resolved = resolve_warm_pool_ref("11111111", &configs).unwrap();

    assert_eq!(resolved.template_name, "app-runtime");
}

#[test]
fn resolve_warm_pool_ref_matches_unique_template_name() {
    let configs = vec![
        warm_pool("11111111-1111-1111-1111-111111111111", "app-runtime"),
        warm_pool("22222222-2222-2222-2222-222222222222", "seed"),
    ];

    let resolved = resolve_warm_pool_ref("app-runtime", &configs).unwrap();

    assert_eq!(
        resolved.id,
        Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
    );
}

#[test]
fn resolve_warm_pool_ref_matches_template_digest_prefix() {
    let configs = vec![
        warm_pool("11111111-1111-1111-1111-111111111111", "app-runtime"),
        warm_pool("22222222-2222-2222-2222-222222222222", "seed"),
    ];

    let resolved = resolve_warm_pool_ref("app-runtime@sha256:abcdef012345", &configs).unwrap();

    assert_eq!(
        resolved.id,
        Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
    );
}

#[test]
fn resolve_warm_pool_ref_rejects_non_latest_tag_selector() {
    let configs = vec![warm_pool(
        "11111111-1111-1111-1111-111111111111",
        "app-runtime",
    )];

    let err = resolve_warm_pool_ref("app-runtime:git-sha", &configs).unwrap_err();

    assert!(err.to_string().contains("no warm-pool config matches"));
}

#[test]
fn resolve_warm_pool_ref_errors_on_ambiguous_template_name() {
    let configs = vec![
        warm_pool("11111111-1111-1111-1111-111111111111", "app-runtime"),
        warm_pool("22222222-2222-2222-2222-222222222222", "app-runtime"),
    ];

    let err = resolve_warm_pool_ref("app-runtime", &configs).unwrap_err();

    match err {
        CliError::Argument(msg) => {
            assert!(msg.contains("ambiguous"));
            assert!(msg.contains("11111111-1111"));
            assert!(msg.contains("22222222-2222"));
        }
        other => panic!("expected Argument, got {other:?}"),
    }
}

fn warm_pool(id: &str, template_name: &str) -> WarmPoolConfigResponse {
    WarmPoolConfigResponse {
        id: Uuid::parse_str(id).unwrap(),
        template_release_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
        template_name: template_name.to_string(),
        content_hash: "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            .to_string(),
        vcpu: 2,
        memory_mib: 8192,
        target_ready_slots: 10,
        refill_per_tick: 5,
        max_warming_slots: 10,
        namespace: "sandboxes".to_string(),
        enabled: true,
        warming_slots: 1,
        ready_slots: 9,
        claimed_slots: 3,
        created_at: "2026-05-19T00:00:00Z".to_string(),
        updated_at: "2026-05-19T00:01:00Z".to_string(),
    }
}

#[test]
fn list_url_carries_the_drainability_sort_and_threshold() {
    let url = list_url(
        "http://127.0.0.1:8080",
        None,
        Some(RoleFilter::Worker),
        false,
        Some(AdminNodeSort::Drainability),
        Some(168),
    );
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes?include_occupancy=false&role=worker&sort=drainability&long_runner_age_hours=168"
    );
}

/// The threshold is forwarded even without `--sort drainability`, so the
/// server's 400 on a rejected value reaches the operator rather than the client
/// silently dropping the flag.
#[test]
fn list_url_forwards_the_threshold_without_the_sort() {
    let url = list_url("http://127.0.0.1:8080", None, None, false, None, Some(0));
    assert_eq!(
        url,
        "http://127.0.0.1:8080/admin/api/v1/nodes?include_occupancy=false&long_runner_age_hours=0"
    );
}

fn drainable_row(id: &str, live_residents: i64, live_long_runners: i64) -> AdminNodeResponse {
    let mut node = row(id);
    node.sandboxes_active = Some(live_residents + 37);
    // Deliberately different from the block's `local_snapshots` mirror below.
    // Equal values would make the two candidate sources for the LOCAL SNAPS
    // cell indistinguishable, so a cell rewired to the mirror would render
    // identically and no test would fail.
    node.sandboxes_local_snapshots = Some(2);
    node.drainability = Some(AdminNodeDrainability {
        live_residents,
        live_long_runners,
        local_snapshots: 9,
        long_runner_age_hours: 24,
        heartbeat_fresh: true,
        drain_blockers: 0,
        terminate_blockers: 0,
    });
    node.residency_evidence = Some("node_census_aware".to_string());
    node
}

#[test]
fn render_nodes_table_adds_drainability_columns_when_present() {
    let node = drainable_row("11111111-1111-1111-1111-111111111111", 2, 1);
    let out = render_nodes_table(&[node], false);

    assert!(out.contains("LIVE SBX"));
    assert!(out.contains("LONG RUNNERS"));
    // Only that the active count reaches the table; the gap between it and the
    // live count is the node's phantom-row backlog, but the per-cell values are
    // pinned by `render_nodes_table_renders_a_missing_block_as_absent` and the
    // ranked-shape snapshot, not here.
    assert!(out.contains("39"));
    assert!(out.contains("ACTIVE SBX"));
}

/// Without the block the columns are absent rather than zero-filled — a fleet
/// of `0`s would read as a fleet of ideal drain candidates.
#[test]
fn render_nodes_table_omits_drainability_columns_when_absent() {
    let out = render_nodes_table(&[row("11111111-1111-1111-1111-111111111111")], false);

    assert!(!out.contains("LIVE SBX"));
    assert!(!out.contains("LONG RUNNERS"));
}

/// Read one cell out of a rendered table, by header name and row id prefix.
/// Positional, because a substring check against the whole render is satisfied
/// by any dash anywhere in it — including the unrelated empty columns every
/// fixture here has.
fn cell(rendered: &str, row_prefix: &str, column: &str) -> String {
    let cells = |line: &str| -> Vec<String> {
        line.trim()
            .trim_matches('│')
            .split('┆')
            .map(|cell| cell.trim().to_string())
            .collect()
    };
    let header = rendered
        .lines()
        .find(|line| line.contains("ACTIVE SBX"))
        .unwrap_or_else(|| panic!("no header row in: {rendered}"));
    // Prefix match, because one header carries a runtime value: `LONG RUNNERS`
    // becomes `LONG RUNNERS >24h`. Bounded by requiring a space after the name,
    // so it cannot slide onto a different column, and asserted unique so a
    // future header pair sharing a prefix fails loudly instead of picking one.
    let names = cells(header);
    let matches: Vec<usize> = names
        .iter()
        .enumerate()
        .filter(|(_, name)| name.as_str() == column || name.starts_with(&format!("{column} ")))
        .map(|(index, _)| index)
        .collect();
    let index = match matches.as_slice() {
        [only] => *only,
        [] => panic!("column {column} not in header: {header}"),
        many => panic!("column {column} matched {} headers: {header}", many.len()),
    };
    let row = rendered
        .lines()
        .find(|line| line.contains(row_prefix))
        .unwrap_or_else(|| panic!("row {row_prefix} not rendered in: {rendered}"));
    cells(row)
        .get(index)
        .unwrap_or_else(|| panic!("row {row_prefix} has no cell {index}"))
        .clone()
}

/// A node missing the block in a response that carries it for others is a
/// server-side inconsistency; rendering it as `-` keeps it distinguishable from
/// a genuine zero. Asserted cell-by-cell against a node whose counts really are
/// zero, because "absent" and "zero" have to differ in the *same* column for
/// the distinction to reach an operator.
#[test]
fn render_nodes_table_renders_a_missing_block_as_absent() {
    let with = drainable_row("11111111-1111-1111-1111-111111111111", 0, 0);
    let without = row("22222222-2222-2222-2222-222222222222");
    let out = render_nodes_table(&[with, without], false);

    for column in [
        "VISIBLE",
        "LIVE SBX",
        "LONG RUNNERS",
        "GATE BLOCKERS",
        "TERM BLOCKERS",
    ] {
        assert_eq!(cell(&out, "22222222", column), "-", "column {column}");
        let computed = cell(&out, "11111111", column);
        assert_ne!(computed, "-", "column {column} was computed for this node");
    }
    assert_eq!(cell(&out, "11111111", "LIVE SBX"), "0");
    assert_eq!(cell(&out, "11111111", "VISIBLE"), "yes");
}

#[test]
fn render_node_detail_includes_drainability_when_present() {
    let node = drainable_row("11111111-1111-1111-1111-111111111111", 2, 1);
    let out = render_node_detail(&node);

    assert!(out.contains("Live Residents"));
    assert!(out.contains("Live Long Runners"));

    let without = row("22222222-2222-2222-2222-222222222222");
    let out = render_node_detail(&without);
    assert!(!out.contains("Live Residents"));
}

/// The counters are uninterpretable without the policy that produced them:
/// under `lifecycle_only` nothing is discounted for census absence, so
/// `Live Residents` includes phantom rows, and under `node_census_aware` it
/// does not. This line is the only place either view names which was in force,
/// so it has to render whenever the server sent it — and stay absent when it
/// didn't, rather than defaulting to a policy the cluster may not be on.
#[test]
fn render_node_detail_names_the_residency_policy() {
    let node = drainable_row("11111111-1111-1111-1111-111111111111", 2, 1);
    let out = render_node_detail(&node);
    assert!(
        out.contains("Residency Policy") && out.contains("node_census_aware"),
        "{out}"
    );

    let mut unreported = drainable_row("22222222-2222-2222-2222-222222222222", 2, 1);
    unreported.residency_evidence = None;
    let out = render_node_detail(&unreported);
    assert!(!out.contains("Residency Policy"), "{out}");
    // The counters still render — only the policy line is conditional.
    assert!(out.contains("Live Residents"), "{out}");
}

/// A server that predates the ranking support ignores the unknown `sort`
/// parameter and answers 200 with name-ordered rows carrying no drainability
/// block. Rendering that as though it were ranked is the version-skew trap:
/// the operator reads a list that looks like a ranking and drains the wrong
/// node, so the CLI has to refuse it instead.
#[test]
fn ensure_ranked_rejects_a_response_that_ignored_the_sort_parameter() {
    let unranked = vec![
        row("11111111-1111-1111-1111-111111111111"),
        row("22222222-2222-2222-2222-222222222222"),
    ];

    let err = ensure_ranked(&unranked).unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("drainability"),
        "the error should name what was missing: {message}"
    );
    assert!(
        message.contains("--sort"),
        "the error should name the flag the operator passed: {message}"
    );
}

#[test]
fn ensure_ranked_accepts_a_response_carrying_the_blocks() {
    let ranked = vec![
        drainable_row("11111111-1111-1111-1111-111111111111", 0, 0),
        drainable_row("22222222-2222-2222-2222-222222222222", 3, 1),
    ];

    ensure_ranked(&ranked).expect("a response carrying blocks is ranked");
}

/// A server that supports ranking but matched no nodes returns an empty list,
/// which is indistinguishable from skew — and is a legitimate answer to a
/// narrow `--role`/`--drain-phase` filter, so it must not error.
#[test]
fn ensure_ranked_accepts_an_empty_listing() {
    ensure_ranked(&[]).expect("an empty listing is a legitimate answer");
}

/// A mixed response is unreachable from either server: an old one cannot
/// populate even one block, and the current route decides emission once per
/// request, so a node absent from the occupancy map still gets an all-zeros
/// block. This pins the guard's defensive branch rather than an observed
/// behaviour — `ensure_ranked` should not depend on that being a per-request
/// decision forever, and the server's own ranking guard anticipates a future
/// per-node computation path, which is when this case would first appear. The
/// renderer shows the gaps as `-`.
#[test]
fn ensure_ranked_accepts_a_partially_populated_response() {
    let mixed = vec![
        drainable_row("11111111-1111-1111-1111-111111111111", 2, 0),
        row("22222222-2222-2222-2222-222222222222"),
    ];

    ensure_ranked(&mixed).expect("a partially populated response must not read as skew");
}

/// The ranking's first term has to be on screen. A node whose last poll ran
/// recently but *failed* carries a healthy-looking `LAST HEARTBEAT` while
/// `heartbeat_fresh` is false and sends it to the bottom — without a column for
/// it an operator cannot explain the order they are shown.
#[test]
fn render_nodes_table_shows_control_plane_visibility() {
    let mut visible = drainable_row("11111111-1111-1111-1111-111111111111", 0, 0);
    visible.last_heartbeat = Some("2026-08-12T23:59:00Z".to_string());
    let mut blind = drainable_row("22222222-2222-2222-2222-222222222222", 0, 0);
    blind.last_heartbeat = Some("2026-08-12T23:59:30Z".to_string());
    blind.drainability = Some(AdminNodeDrainability {
        heartbeat_fresh: false,
        ..blind.drainability.expect("fixture carries a block")
    });

    let out = render_nodes_table(&[visible, blind], false);

    // Per cell, not `contains(" yes ")` — the fixture holds one node of each
    // state, so any mapping emitting both strings satisfies a whole-render
    // substring check, the inverted one included.
    assert_eq!(cell(&out, "11111111", "VISIBLE"), "yes");
    assert_eq!(cell(&out, "22222222", "VISIBLE"), "no");
    // Both rows carry a recent-looking heartbeat, so LAST HEARTBEAT cannot be
    // what distinguishes them. That gap is the whole reason the column exists:
    // a poll that ran seconds ago but *failed* leaves the timestamp fresh and
    // `heartbeat_fresh` false.
    assert_ne!(cell(&out, "22222222", "LAST HEARTBEAT"), "-");
}

#[test]
fn render_node_detail_explains_why_a_node_is_not_visible() {
    let mut node = drainable_row("11111111-1111-1111-1111-111111111111", 0, 0);
    node.drainability = Some(AdminNodeDrainability {
        heartbeat_fresh: false,
        ..node.drainability.expect("fixture carries a block")
    });

    let out = render_node_detail(&node);

    assert!(out.contains("Control Plane Visible"));
    assert!(
        out.contains("reachable and unexpired heartbeat required"),
        "{out}"
    );
}

/// Pins the whole ranked table, which the per-cell assertions cannot: where the
/// five ranking columns sit relative to `ACTIVE SBX` and `LOCAL SNAPS`, and the
/// full row shape. `render_nodes_table_renders_a_missing_block_as_absent` reads
/// named cells and so catches a wrong value in a column it names; this catches a
/// column moving, appearing, or disappearing, which no per-cell read can see.
#[test]
fn render_nodes_table_ranked_shape() {
    let mut visible = drainable_row("11111111-1111-1111-1111-111111111111", 2, 1);
    visible.name = Some("worker-a".to_string());
    visible.last_heartbeat = Some("2026-05-14T01:25:00Z".to_string());
    let mut blind = drainable_row("22222222-2222-2222-2222-222222222222", 5, 0);
    blind.name = Some("worker-b".to_string());
    // A fresher timestamp than the visible node's, deliberately: this pins the
    // case `VISIBLE` exists for, where a recent poll *failed* and LAST
    // HEARTBEAT looks healthier than the node is. A fixture whose invisible row
    // also had no heartbeat would pin only the case where the two agree.
    blind.last_heartbeat = Some("2026-05-14T01:26:00Z".to_string());
    blind.drainability = Some(AdminNodeDrainability {
        heartbeat_fresh: false,
        drain_blockers: 1,
        terminate_blockers: 6,
        ..blind.drainability.expect("fixture carries a block")
    });
    // Not reachable from the current route at all: it decides emission once per
    // request, and a node absent from the occupancy map still gets an all-zeros
    // block. Kept because the distinction it pins is what `ensure_ranked` rests
    // on — an absent block renders `-`, never `0`, whatever produced it.
    let mut ungrouped = row("33333333-3333-3333-3333-333333333333");
    ungrouped.name = Some("worker-c".to_string());
    ungrouped.sandboxes_active = Some(4);

    insta::assert_snapshot!(render_nodes_table(&[visible, blind, ungrouped], false));
}

fn list_args(sort: Option<AdminNodeSort>, json: bool) -> ListNodesArgs {
    ListNodesArgs {
        drain_phase: None,
        role: None,
        include_deleted: false,
        sort,
        long_runner_age_hours: None,
        verbose: false,
        json,
    }
}

/// Body an api-server without ranking support returns for `--sort
/// drainability`: 200, rows in name order, no `drainability` anywhere.
fn unranked_body() -> String {
    serde_json::to_string(&[
        row("11111111-1111-1111-1111-111111111111"),
        row("22222222-2222-2222-2222-222222222222"),
    ])
    .expect("fixture serializes")
}

/// The guard has to fire on the table path. Covered here rather than through
/// `list_nodes`, which needs a live server — while the check lived inline
/// behind the fetch, deleting its call site failed no test.
#[test]
fn render_listing_rejects_unranked_rows_on_the_table_path() {
    let err = render_listing(
        &unranked_body(),
        &list_args(Some(AdminNodeSort::Drainability), false),
    )
    .expect_err("an unranked response must not render as a ranking");
    assert!(matches!(err, CliError::Unsupported(_)), "{err:?}");
}

/// And on the `--json` path, which is the one that would otherwise hand the
/// unranked body straight to the operator.
#[test]
fn render_listing_rejects_unranked_rows_on_the_json_path() {
    let err = render_listing(
        &unranked_body(),
        &list_args(Some(AdminNodeSort::Drainability), true),
    )
    .expect_err("--json must not pass an unranked response through");
    assert!(matches!(err, CliError::Unsupported(_)), "{err:?}");
}

/// Without `--sort drainability` the same body is fine — the guard must not
/// fire on a listing nobody asked to rank.
#[test]
fn render_listing_passes_unranked_rows_when_ranking_was_not_requested() {
    for (sort, json) in [
        (None, false),
        (None, true),
        (Some(AdminNodeSort::Name), false),
        (Some(AdminNodeSort::Name), true),
    ] {
        render_listing(&unranked_body(), &list_args(sort, json))
            .unwrap_or_else(|err| panic!("sort={sort:?} json={json} should render: {err}"));
    }
}

/// A ranked body renders on both paths.
#[test]
fn render_listing_accepts_ranked_rows() {
    let body = serde_json::to_string(&[
        drainable_row("11111111-1111-1111-1111-111111111111", 0, 0),
        drainable_row("22222222-2222-2222-2222-222222222222", 3, 1),
    ])
    .expect("fixture serializes");

    let table = render_listing(&body, &list_args(Some(AdminNodeSort::Drainability), false))
        .expect("ranked rows render");
    assert!(table.contains("LONG RUNNERS"), "{table}");

    let json = render_listing(&body, &list_args(Some(AdminNodeSort::Drainability), true))
        .expect("ranked rows render as json");
    assert!(json.contains("drainability"), "{json}");
}

/// Pins the detail view's drainability block whole: the six lines it adds, their
/// order, and which field each one reads. The label-presence assertions above
/// pin none of that — `Live Residents` rendering `live_long_runners`, or the two
/// blocker lines disappearing outright, leaves them all green. Distinct fixture
/// values per field so a crossed pair is visible in the diff rather than
/// arithmetically hidden.
#[test]
fn render_node_detail_drainability_block_shape() {
    let mut node = drainable_row("11111111-1111-1111-1111-111111111111", 0, 0);
    node.name = Some("ip-192-0-2-125.ec2.internal".to_string());
    node.sandboxes_active = Some(41);
    node.sandboxes_local_snapshots = Some(7);
    node.drainability = Some(AdminNodeDrainability {
        live_residents: 11,
        live_long_runners: 22,
        local_snapshots: 33,
        long_runner_age_hours: 66,
        heartbeat_fresh: false,
        drain_blockers: 44,
        terminate_blockers: 55,
    });

    insta::assert_snapshot!(render_node_detail(&node));
}

/// `--json --sort drainability` has to decode, so a body this CLI cannot type
/// fails where plain `--json` would have printed it. The error is the only
/// place the operator learns the raw body is still reachable, so it has to name
/// the flag to drop — deleting the enrichment leaves a bare parse error and a
/// dead end.
#[test]
fn render_listing_json_decode_error_names_the_escape_hatch() {
    // Valid JSON, wrong shape: `role` is a closed enum with no catch-all, which
    // is what a newer server adding a variant looks like from here.
    let body = r#"[{"id":"11111111-1111-1111-1111-111111111111","role":"quantum","managed_by":"k8s","metadata":null,"created_at":"2026-05-14T00:00:00Z","sandboxes_active":0,"sandboxes_local_snapshots":0}]"#;

    let err = render_listing(body, &list_args(Some(AdminNodeSort::Drainability), true))
        .expect_err("an undecodable body must not be reported as ranked");
    let message = err.to_string();
    assert!(message.contains("--sort"), "{message}");
    assert!(
        !message.contains("  "),
        "the message must not carry wrapped-literal whitespace: {message:?}"
    );

    // Plain `--json` still hands the same body back rather than failing.
    render_listing(body, &list_args(None, true)).expect("plain --json passes the body through");
}

/// The threshold in the header is the server's own echo, and it is what makes
/// the count readable — `LONG RUNNERS 3` alone does not say "3 past what age",
/// and the operator cannot infer it: the single-node route uses a fixed default
/// rather than whatever `--long-runner-age-hours` was last passed, and the list
/// route clamps an absurdly large value rather than echoing it back. (Zero is
/// rejected outright with a 400, not clamped.)
#[test]
fn render_nodes_table_names_the_long_runner_threshold() {
    let mut a = drainable_row("11111111-1111-1111-1111-111111111111", 1, 1);
    let mut b = drainable_row("22222222-2222-2222-2222-222222222222", 2, 2);
    for node in [&mut a, &mut b] {
        node.drainability = Some(AdminNodeDrainability {
            long_runner_age_hours: 168,
            ..node.drainability.expect("fixture carries a block")
        });
    }
    let out = render_nodes_table(&[a.clone(), b.clone()], false);
    assert!(out.contains("LONG RUNNERS >168h"), "{out}");
    // Still addressable by its base name, so the column keeps its identity.
    assert_eq!(cell(&out, "11111111", "LONG RUNNERS"), "1");

    // One request yields one threshold, so rows that disagree mean a server
    // that mixed sources. Showing either number would be right for only some
    // rows, so the header goes bare rather than picking one.
    let mut mixed = b;
    mixed.drainability = Some(AdminNodeDrainability {
        long_runner_age_hours: 24,
        ..mixed.drainability.expect("fixture carries a block")
    });
    let out = render_nodes_table(&[a.clone(), mixed], false);
    assert!(out.contains("LONG RUNNERS"), "{out}");
    assert!(!out.contains(">168h") && !out.contains(">24h"), "{out}");

    // Zero is not a threshold the server will ever have used — the list route
    // rejects it with a 400 — so it can only mean the field was absent and
    // `#[serde(default)]` filled it in. Claiming `>0h` would assert a threshold
    // that cannot exist; the header goes bare, as it does for disagreement.
    let mut defaulted = a;
    defaulted.drainability = Some(AdminNodeDrainability {
        long_runner_age_hours: 0,
        ..defaulted.drainability.expect("fixture carries a block")
    });
    let out = render_nodes_table(&[defaulted], false);
    assert!(out.contains("LONG RUNNERS"), "{out}");
    assert!(!out.contains(">0h"), "{out}");
}

/// The widest arrangement, and the only one no other test renders: both
/// conditional column groups on at once, 18 columns. Worth its own snapshot
/// because a header/cell count mismatch here does not panic — comfy-table pads
/// to the widest row, so every cell after the shorter group silently shifts one
/// column left and the table still prints.
#[test]
fn render_nodes_table_ranked_verbose_shape() {
    let mut node = drainable_row("11111111-1111-1111-1111-111111111111", 3, 1);
    node.name = Some("worker-a".to_string());
    node.k8s_node_name = Some("ip-192-0-2-125.ec2.internal".to_string());
    node.state = Some("registered".to_string());
    node.drain_last_error = Some("cordon patch failed: timeout".to_string());
    node.last_heartbeat = Some("2026-05-14T01:25:00Z".to_string());

    insta::assert_snapshot!(render_nodes_table(&[node], true));
}

#[test]
fn inventory_null_counts_decode_and_render_as_unknown() {
    let mut node = row("11111111-aaaa-bbbb-cccc-dddddddddddd");
    node.sandboxes_active = None;
    node.sandboxes_local_snapshots = None;
    let body = serde_json::to_string(&vec![node.clone()]).unwrap();
    let decoded: Vec<AdminNodeResponse> = serde_json::from_str(&body).unwrap();
    assert_eq!(decoded[0].sandboxes_active, None);
    let rendered = render_listing(&body, &list_args(None, false)).unwrap();
    assert_eq!(cell(&rendered, "11111111", "ACTIVE SBX"), "-");
    assert_eq!(cell(&rendered, "11111111", "LOCAL SNAPS"), "-");
    let detail = render_node_detail(&node);
    let counts: Vec<_> = detail
        .lines()
        .filter(|line| line.contains("Active Sandboxes") || line.contains("Local Snapshots"))
        .collect();
    assert_eq!(counts.len(), 2);
    assert!(counts.iter().all(|line| line.trim_end().ends_with('-')));
    assert!(
        single_node_url("http://localhost", "node-id", true).ends_with("include_occupancy=true")
    );
}

#[test]
fn node_detail_preserves_unknown_and_absent_execution_reporting() {
    for outcome in [
        None,
        Some("complete"),
        Some("unavailable"),
        Some("legacy"),
        Some("future_outcome"),
    ] {
        let mut status = serde_json::json!({
            "observed_at": "2026-10-20T12:00:00Z",
            "stale_after": "2026-10-20T12:00:30Z",
            "stale": false,
            "reachable": true
        });
        if let Some(outcome) = outcome {
            status["execution_inventory"] = serde_json::json!({
                "status": outcome,
                "received_at": "2026-10-19T12:00:00Z"
            });
        }
        let mut node = row("11111111-1111-1111-1111-111111111111");
        // Old API responses omit the block; future labels remain readable.
        node.status = Some(serde_json::from_value(status).unwrap());
        let rendered = render_node_detail(&node);
        let collection = rendered
            .lines()
            .find(|line| line.starts_with("Execution Collection"))
            .unwrap();
        assert!(collection.ends_with(outcome.unwrap_or("not reported")));
        let receipt = rendered
            .lines()
            .find(|line| line.starts_with("Execution Receipt"))
            .unwrap();
        assert!(receipt.ends_with(if outcome.is_some() {
            "2026-10-19T12:00:00Z"
        } else {
            "-"
        }));
    }
}

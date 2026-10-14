use pplx_space_common_core::types::OrgId;
use pplx_space_common_core::types::UserId;
use uuid::Uuid;

use super::*;
use crate::models::User;
use crate::user_role::UserRole;
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;
use tower::ServiceExt;

use pplx_space_common_core::types::{NodeAddress, NodeDrainMode, NodeId, NodeManagedBy, NodeRole};

fn test_admin_node_row() -> AdminNodeRow {
    AdminNodeRow {
        id: NodeId::new(),
        name: Some("worker-1".to_string()),
        address: Some("10.0.0.10".parse::<NodeAddress>().unwrap()),
        node_api_endpoint: Some("10.0.0.10:9802".to_string()),
        role: NodeRole::Worker,
        managed_by: NodeManagedBy::Space,
        state: Some("registered".to_string()),
        scheduling_status: Some("ready".to_string()),
        scheduling_reason: Some("ready".to_string()),
        status_stale_after: None,
        status_reachable: None,
        status_error: None,
        status_role: None,
        status_node_id_reported: None,
        network_slots_ready: Some(300),
        network_pool_min_ready: Some(32),
        network_slots_degraded: Some(0),
        network_slots_quarantined: Some(0),
        network_slots_total: Some(300),
        network_slots_leased: Some(0),
        network_pool_max_slots: Some(300),
        drain_phase: None,
        drain_mode: None,
        drain_started_at: None,
        drain_last_error: None,
        terminate_decrement_capacity: false,
        metadata: None,
        last_heartbeat: None,
        status_observed_at: None,
        status_health_healthy: None,
        status_services: None,
        status_node_ctl: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: None,
        deleted_at: None,
        substrate_node_id: None,
    }
}

#[test]
fn admin_node_response_uses_status_snapshot_as_heartbeat() {
    let snapshot_observed_at = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(60);
    let mut row = test_admin_node_row();
    row.status_observed_at = Some(snapshot_observed_at);
    row.status_stale_after = Some(snapshot_observed_at + time::Duration::seconds(30));
    row.status_reachable = Some(true);
    row.status_health_healthy = Some(true);

    let response = admin_node_response(row, Some(SandboxOccupancy::default()), None);

    assert_eq!(
        response.last_heartbeat.as_deref(),
        Some("1970-01-01T00:01:00Z")
    );
    let status = response.status.expect("status snapshot should be present");
    assert_eq!(status.observed_at, "1970-01-01T00:01:00Z");
    assert!(status.reachable);
    assert_eq!(
        status
            .scheduling
            .as_ref()
            .map(|scheduling| scheduling.status.as_str()),
        Some("ready")
    );
    assert!(status.health.expect("health should be present").healthy);
}

#[test]
fn drain_only_allowed_from_no_phase() {
    let t = validate_drain_transition(None, DrainAction::Drain).unwrap();
    assert!(matches!(
        t,
        PhaseTransition::Move {
            new: Some(PHASE_DRAINING),
            set_started_at: true,
            clear_started_at: false,
        }
    ));

    for phase in [PHASE_DRAINING, PHASE_DRAINED, PHASE_TERMINATING] {
        let err = validate_drain_transition(Some(phase), DrainAction::Drain).unwrap_err();
        assert!(matches!(err, TransitionError::Conflict { .. }));
    }
}

#[test]
fn terminate_only_allowed_from_drained() {
    let t = validate_drain_transition(Some(PHASE_DRAINED), DrainAction::Terminate).unwrap();
    assert!(matches!(
        t,
        PhaseTransition::Move {
            new: Some(PHASE_TERMINATING),
            ..
        }
    ));

    for phase in [None, Some(PHASE_DRAINING), Some(PHASE_TERMINATING)] {
        let err = validate_drain_transition(phase, DrainAction::Terminate).unwrap_err();
        assert!(matches!(err, TransitionError::Conflict { .. }));
    }
}

#[test]
fn undrain_from_draining_or_drained() {
    for phase in [PHASE_DRAINING, PHASE_DRAINED] {
        let t = validate_drain_transition(Some(phase), DrainAction::Undrain).unwrap();
        assert!(matches!(
            t,
            PhaseTransition::Move {
                new: None,
                set_started_at: false,
                clear_started_at: true,
            }
        ));
    }

    let err = validate_drain_transition(None, DrainAction::Undrain).unwrap_err();
    assert!(matches!(err, TransitionError::Conflict { .. }));

    let err = validate_drain_transition(Some(PHASE_TERMINATING), DrainAction::Undrain).unwrap_err();
    assert!(matches!(err, TransitionError::Conflict { .. }));
}

#[test]
fn parse_spec_accepts_bare_uuid_and_uuid_colon_action() {
    let id = NodeId::new();

    let (parsed_id, action) = parse_spec(&id.to_string()).unwrap();
    assert_eq!(parsed_id, id);
    assert!(action.is_none());

    let (parsed_id, action) = parse_spec(&format!("{id}:drain")).unwrap();
    assert_eq!(parsed_id, id);
    assert!(matches!(action, Some(DrainAction::Drain)));

    for label in [
        "terminate",
        "undrain",
        "cordon",
        "uncordon",
        "force-upload-local-snapshots",
    ] {
        let (_, action) = parse_spec(&format!("{id}:{label}")).unwrap();
        assert_eq!(action.unwrap().label(), label);
    }
}

/// The force-upload action must validate as a no-change drain
/// transition from any phase — the actual remediation work runs
/// in the dedicated `apply_force_upload_local_snapshots` handler
/// (routed before `apply_drain_action` is called). This test pins
/// down the validator's contract; if a future change makes
/// `:force-upload-local-snapshots` move the drain state machine,
/// `dispatch_node_action` needs the matching update.
#[test]
fn force_upload_local_snapshots_never_changes_drain_phase() {
    for phase in [
        None,
        Some(PHASE_DRAINING),
        Some(PHASE_DRAINED),
        Some(PHASE_TERMINATING),
    ] {
        let transition =
            validate_drain_transition(phase, DrainAction::ForceUploadLocalSnapshots).unwrap();
        assert!(matches!(transition, PhaseTransition::NoChange));
    }
}

/// `:force-upload-local-snapshots` may only run while the user-API drain gate is
/// refusing wakes, because the legacy reupload it triggers reads the live
/// `/space/cache/<id>/snapshot/` directory and a concurrent wake plus re-suspend
/// would let it register a torn snapshot.
///
/// The handler expresses that as `NodeDrainState::blocks_placement`, the same
/// predicate the gate itself uses — sharing it is the point, since the hazard is
/// precisely a reader that believes wakes are blocked when they are not. This
/// test pins the truth table that makes the sharing correct: a **gentle**
/// `draining` node is the one non-NULL phase the endpoint must refuse.
///
/// Coverage gap, stated rather than papered over: this asserts the predicate, not
/// the handler wiring — `apply_force_upload_local_snapshots` needs a full
/// `AppState` (pool + substrate + auth) that this module has no harness for. What
/// it does catch is the predicate drifting out from under the endpoint.
/// The refusal text an operator actually reads must not carry the source
/// indentation of the literal that produced it.
///
/// A multi-line Rust string keeps the leading whitespace of every continuation
/// line unless that line ends in a `\`. rustfmt then joins the result onto one
/// physical line with the spaces baked in, so the defect is invisible in the
/// diff and shows up only in the JSON `detail` field — `is only` followed by
/// eighteen spaces and then `valid`. Asserting on the emitted string rather than
/// on the source is what gives this an oracle.
#[test]
fn force_upload_refusal_text_has_no_embedded_indentation() {
    use pplx_space_common_core::types::{NodeDrainPhase, NodeDrainState};

    let states = [
        None,
        NodeDrainState::from_columns(Some(NodeDrainPhase::Draining), Some("gentle")),
    ];
    for state in states {
        let (error, detail) = super::force_upload_gate_open_message(state);
        for text in [error, detail] {
            assert!(
                !text.contains("  "),
                "operator-facing text carries embedded indentation: {text:?}"
            );
        }
    }
}

#[test]
fn force_upload_requires_a_closed_wake_gate() {
    use pplx_space_common_core::types::{NodeDrainMode, NodeDrainPhase, NodeDrainState};

    // No drain at all: the gate is open, so the action is refused.
    assert!(
        !NodeDrainState::from_columns(None, None).is_some_and(NodeDrainState::blocks_placement)
    );

    // Gentle + draining: non-NULL phase, but the gate is deliberately open.
    let gentle_draining =
        NodeDrainState::from_columns(Some(NodeDrainPhase::Draining), Some("gentle"))
            .expect("a phase was supplied");
    assert!(
        !gentle_draining.blocks_placement(),
        "a gentle drain must not satisfy the force-upload precondition"
    );
    // ...and the same is true when nobody set a mode, since that reads as gentle.
    let unset_mode = NodeDrainState::from_columns(Some(NodeDrainPhase::Draining), None)
        .expect("a phase was supplied");
    assert!(!unset_mode.blocks_placement());

    // Forced draining, plus drained/terminating in either mode: gate closed, so
    // the action is allowed.
    for (phase, mode) in [
        (NodeDrainPhase::Draining, NodeDrainMode::Forced),
        (NodeDrainPhase::Drained, NodeDrainMode::Forced),
        (NodeDrainPhase::Drained, NodeDrainMode::Gentle),
        (NodeDrainPhase::Terminating, NodeDrainMode::Forced),
        (NodeDrainPhase::Terminating, NodeDrainMode::Gentle),
    ] {
        let state = NodeDrainState::from_columns(Some(phase), Some(mode.as_str()))
            .expect("a phase was supplied");
        assert!(
            state.blocks_placement(),
            "{phase}/{mode} must satisfy the force-upload precondition"
        );
    }
}

#[test]
fn parse_spec_rejects_unknown_action_with_not_found() {
    let id = Uuid::now_v7();
    let err = parse_spec(&format!("{id}:bogus")).unwrap_err();
    assert_eq!(err.0, StatusCode::NOT_FOUND);
}

#[test]
fn parse_spec_rejects_bad_uuid_with_bad_request() {
    let err = parse_spec("not-a-uuid:drain").unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}

#[test]
fn cordon_is_always_a_no_op_at_the_db_layer() {
    for phase in [
        None,
        Some(PHASE_DRAINING),
        Some(PHASE_DRAINED),
        Some(PHASE_TERMINATING),
    ] {
        assert!(matches!(
            validate_drain_transition(phase, DrainAction::Cordon).unwrap(),
            PhaseTransition::NoChange
        ));
    }
}

#[test]
fn uncordon_refuses_while_a_drain_is_active() {
    // No drain in progress: standalone uncordon is fine.
    assert!(matches!(
        validate_drain_transition(None, DrainAction::Uncordon).unwrap(),
        PhaseTransition::NoChange
    ));

    // Drain in any non-null phase blocks uncordon — operators must use
    // `:undrain`, which clears the phase and uncordons atomically.
    for phase in [PHASE_DRAINING, PHASE_DRAINED, PHASE_TERMINATING] {
        let err = validate_drain_transition(Some(phase), DrainAction::Uncordon).unwrap_err();
        assert!(matches!(err, TransitionError::Conflict { .. }));
    }
}

/// Persisted column value derived from the operator-supplied body.
/// Mirrors the inline `!drain_request.replace_after_terminate` in
/// `apply_drain_action`; the helper is duplicated in the test so a
/// future refactor that splits the computation out has a fixed
/// reference point for the bool inversion.
fn persisted_decrement_capacity(req: &DrainRequest) -> bool {
    !req.replace_after_terminate
}

#[test]
fn drain_with_default_body_persists_decrement_capacity_false() {
    let req = parse_drain_request(&[]).unwrap();
    assert!(req.replace_after_terminate);
    assert!(!persisted_decrement_capacity(&req));
}

#[test]
fn drain_with_empty_json_object_persists_decrement_capacity_false() {
    let req = parse_drain_request(b"{}").unwrap();
    assert!(req.replace_after_terminate);
    assert!(!persisted_decrement_capacity(&req));
}

/// A bodiless `:drain` — what `space-admin-cli drain-node <node>` and a bare `curl -X
/// POST` send — is a **gentle** drain. This is the safe default on purpose:
/// gentle costs drain latency, forced costs every resident sandbox an
/// evacuation plus a 409 window, so the cheaper failure mode is the one you
/// get by not asking.
#[test]
fn drain_with_no_body_defaults_to_gentle_mode() {
    assert_eq!(
        parse_drain_request(&[]).unwrap().mode,
        NodeDrainMode::Gentle
    );
    assert_eq!(
        parse_drain_request(b"{}").unwrap().mode,
        NodeDrainMode::Gentle
    );
    // A body that sets only the other field must not silently switch modes.
    assert_eq!(
        parse_drain_request(br#"{"replace_after_terminate": false}"#)
            .unwrap()
            .mode,
        NodeDrainMode::Gentle
    );
}

#[test]
fn drain_mode_is_parsed_from_the_body_when_supplied() {
    for (body, expected) in [
        (br#"{"mode": "gentle"}"#.as_slice(), NodeDrainMode::Gentle),
        (br#"{"mode": "forced"}"#.as_slice(), NodeDrainMode::Forced),
    ] {
        assert_eq!(parse_drain_request(body).unwrap().mode, expected);
    }
}

/// A typo'd mode is a 400, not a silent gentle drain. The rollout script and
/// the kind smoke both pass `mode: forced` because they need the evacuation
/// pipeline to run; if a misspelling fell back to the default they would cordon
/// a node and wait forever while reporting success.
#[test]
fn drain_with_unknown_mode_value_rejects_as_bad_request() {
    for body in [
        br#"{"mode": "gentel"}"#.as_slice(),
        br#"{"mode": "Forced"}"#.as_slice(),
        br#"{"mode": ""}"#.as_slice(),
        br#"{"mode": null}"#.as_slice(),
    ] {
        let err = parse_drain_request(body).unwrap_err();
        assert_eq!(
            err.0,
            StatusCode::BAD_REQUEST,
            "body {} must be rejected",
            String::from_utf8_lossy(body)
        );
    }
}

#[test]
fn drain_with_replace_after_terminate_false_persists_decrement_capacity_true() {
    let req = parse_drain_request(br#"{"replace_after_terminate": false}"#).unwrap();
    assert!(!req.replace_after_terminate);
    assert!(persisted_decrement_capacity(&req));
}

#[test]
fn drain_with_explicit_replace_after_terminate_true_persists_decrement_capacity_false() {
    let req = parse_drain_request(br#"{"replace_after_terminate": true}"#).unwrap();
    assert!(req.replace_after_terminate);
    assert!(!persisted_decrement_capacity(&req));
}

#[test]
fn drain_with_malformed_body_rejects_as_bad_request() {
    let err = parse_drain_request(b"not-json").unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}

#[test]
fn drain_with_unknown_field_rejects_as_bad_request() {
    // A misspelled / camelCase key on this capacity knob must surface as
    // 400, not silently fall through to the `true` default — that would
    // give the operator the opposite of the intended ASG capacity
    // behavior. Enforced via `deny_unknown_fields` on `DrainRequest`.
    let err = parse_drain_request(br#"{"replaceAfterTerminate": false}"#).unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}

/// A node whose block carries `heartbeat_fresh: true`, so the blindness term
/// does not dominate the ordering under test.
///
/// The row's `status_*` fields are set to match, but they are not what makes
/// this work: `drainability_rank` reads only [`AdminNodeDrainability`], so the
/// block's own flag is the lever. They are populated so the fixture is not
/// self-contradictory — a response claiming an unreachable node alongside a
/// fresh drainability heartbeat would be a shape the store never emits.
fn ranked(live_long_runners: i64, live_residents: i64, drain_blockers: i64) -> AdminNodeResponse {
    let mut row = test_admin_node_row();
    let observed_at = OffsetDateTime::now_utc();
    row.status_observed_at = Some(observed_at);
    row.status_stale_after = Some(observed_at + time::Duration::hours(1));
    row.status_reachable = Some(true);
    let mut response = admin_node_response(
        row,
        Some(SandboxOccupancy {
            active: live_residents,
            local_snapshots: 0,
        }),
        Some((
            AdminNodeDrainability {
                live_residents,
                live_long_runners,
                long_runner_age_hours: 24,
                local_snapshots: 0,
                heartbeat_fresh: true,
                drain_blockers,
                // Not 0: the store guarantees
                // `live_residents <= terminate_blockers <= active`, so a fixture
                // pinning this at zero while `live_residents` varies is a shape
                // the store cannot emit — an impossible baseline for the next
                // test that varies this term.
                terminate_blockers: live_residents,
            },
            ResidencyEvidence::NodeCensusAware,
        )),
    );
    response.name = Some(format!(
        "lr{live_long_runners}-r{live_residents}-b{drain_blockers}"
    ));
    response
}

/// The count terms in order: a node with any live long-runner sorts behind
/// every node with none, no matter how many short-lived residents or gate
/// blockers the latter carries. A gentle drain on a long-runner node never
/// converges on its own, so that term is not tradeable against the others.
#[test]
fn drainability_rank_orders_long_runners_then_residents_then_gate_blockers() {
    let mut nodes = [
        ranked(1, 1, 0),
        ranked(0, 9, 9),
        ranked(0, 2, 5),
        ranked(0, 2, 1),
        ranked(0, 0, 3),
    ];
    nodes.sort_by_key(drainability_rank);

    let order: Vec<&str> = nodes
        .iter()
        .map(|node| node.name.as_deref().unwrap())
        .collect();
    assert_eq!(
        order,
        vec![
            "lr0-r0-b3",
            "lr0-r2-b1",
            "lr0-r2-b5",
            "lr0-r9-b9",
            "lr1-r1-b0",
        ]
    );
}

/// Ties on the whole key keep the order the SQL produced (role, then name), so
/// two identical nodes do not swap places between calls and an operator working
/// down the list sees a stable ranking.
///
/// The tied pair is deliberately out of alphabetical order with a
/// strictly-better node between them, so what the assertion pins is *input*
/// order being preserved rather than the fixture happening to be sorted
/// already. It does **not** detect a switch to `sort_unstable_by_key`: Rust's
/// unstable sort falls back to insertion sort on short slices and is
/// incidentally stable there, verified by running this test against that
/// substitution. Catching that would take a fixture larger than the sort's
/// insertion-sort threshold, which is an implementation detail not worth
/// pinning a test to.
#[test]
fn drainability_rank_sort_is_stable_across_ties() {
    let mut tied_first = ranked(0, 1, 0);
    tied_first.name = Some("zzz".to_string());
    let mut better = ranked(0, 0, 0);
    better.name = Some("mmm".to_string());
    let mut tied_second = ranked(0, 1, 0);
    tied_second.name = Some("aaa".to_string());
    let mut nodes = [tied_first, better, tied_second];

    nodes.sort_by_key(drainability_rank);

    assert_eq!(
        nodes
            .iter()
            .map(|node| node.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        // `mmm` wins on live residents; the tied pair keeps its input order,
        // which is the SQL's and not alphabetical.
        vec!["mmm", "zzz", "aaa"]
    );
}

#[test]
fn parse_sort_defaults_to_name_and_rejects_unknown_values() {
    assert_eq!(parse_sort(None).unwrap(), AdminNodeSort::Name);
    assert!(!parse_sort(None).unwrap().needs_drainability());
    assert_eq!(
        parse_sort(Some("drainability")).unwrap(),
        AdminNodeSort::Drainability
    );
    assert!(
        parse_sort(Some("drainability"))
            .unwrap()
            .needs_drainability()
    );

    let err = parse_sort(Some("Drainability")).unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    // The whole reason the message is rendered from `AdminNodeSort::ALL` is that
    // the 400 names what the caller may send instead. Asserting only the status
    // code would pass with the enumeration dropped.
    let body = serde_json::to_string(&err.1.0).expect("error body serializes");
    for sort in AdminNodeSort::ALL.iter().copied() {
        assert!(
            body.contains(sort.as_str()),
            "the 400 must name {sort}: {body}"
        );
    }
}

/// A zero age would classify every live resident as a long-runner and rank the
/// whole fleet as undrainable — a plausible-looking answer that is wrong, so it
/// is rejected rather than accepted as "no threshold".
#[test]
fn parse_long_runner_age_defaults_to_24h_and_rejects_zero() {
    assert_eq!(
        parse_long_runner_age(None).unwrap(),
        Duration::from_secs(24 * 60 * 60)
    );
    assert_eq!(
        parse_long_runner_age(Some(168)).unwrap(),
        Duration::from_secs(7 * 24 * 60 * 60)
    );

    let err = parse_long_runner_age(Some(0)).unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}

/// The largest hour count the wire can carry must not overflow the seconds
/// conversion; the store clamps it further before it reaches `make_interval`.
#[test]
fn parse_long_runner_age_accepts_the_wire_maximum() {
    let age = parse_long_runner_age(Some(u32::MAX)).unwrap();
    assert_eq!(age.as_secs(), u64::from(u32::MAX) * 3600);
}

/// The two drainability knobs arrive through the same `Query` extractor the
/// route uses, so they are tested through it rather than through a hand-built
/// struct: `sort` stays an unvalidated string that the handler parses, and the
/// age arrives as a number.
#[test]
fn list_filters_extract_the_drainability_query_parameters() {
    let uri: axum::http::Uri =
        "/admin/api/v1/nodes?role=worker&sort=drainability&long_runner_age_hours=168"
            .parse()
            .unwrap();
    let Query(filters) = Query::<ListAdminNodesFilters>::try_from_uri(&uri).unwrap();

    assert_eq!(filters.role.as_deref(), Some("worker"));
    assert_eq!(
        parse_sort(filters.sort.as_deref()).unwrap(),
        AdminNodeSort::Drainability
    );
    assert_eq!(
        parse_long_runner_age(filters.long_runner_age_hours).unwrap(),
        Duration::from_secs(7 * 24 * 60 * 60)
    );

    let bare: axum::http::Uri = "/admin/api/v1/nodes".parse().unwrap();
    let Query(filters) = Query::<ListAdminNodesFilters>::try_from_uri(&bare).unwrap();
    assert_eq!(
        parse_sort(filters.sort.as_deref()).unwrap(),
        AdminNodeSort::Name
    );
    assert_eq!(
        parse_long_runner_age(filters.long_runner_age_hours).unwrap(),
        DEFAULT_LONG_RUNNER_AGE
    );
}

/// A node the control plane cannot see is not a candidate: nothing evacuates
/// off it, because the evacuation RPCs go to it. It must therefore sort behind
/// a healthy node even when its own counts look better — which they will, since
/// a blind node's rows all keep counting for want of absence evidence.
#[test]
fn drainability_rank_sorts_blind_nodes_last_however_empty_they_look() {
    let mut blind = ranked(0, 0, 0);
    blind.name = Some("blind".to_string());
    blind.drainability = Some(AdminNodeDrainability {
        heartbeat_fresh: false,
        ..blind.drainability.expect("fixture carries a block")
    });
    let mut busy = ranked(0, 9, 9);
    busy.name = Some("busy-but-visible".to_string());
    let mut nodes = [blind, busy];

    nodes.sort_by_key(drainability_rank);

    assert_eq!(
        nodes
            .iter()
            .map(|node| node.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["busy-but-visible", "blind"]
    );
}

/// A node with no computed block at all sorts as blind rather than as a
/// perfect candidate, because `AdminNodeDrainability::default()` yields
/// `heartbeat_fresh: false`. The three concrete blindness states — no status
/// row, unreachable, expired heartbeat — are the SQL fragment's definition and
/// are tested against Postgres in `sandbox_residency` and the store suite;
/// what this pins down is that the ranking reads the flag in the safe
/// direction.
#[test]
fn drainability_rank_treats_a_missing_block_as_blind() {
    let mut without = ranked(0, 0, 0);
    without.name = Some("no-block".to_string());
    without.drainability = None;
    let mut busy = ranked(0, 5, 0);
    busy.name = Some("visible".to_string());
    let mut nodes = [without, busy];

    nodes.sort_by_key(drainability_rank);

    assert_eq!(
        nodes
            .iter()
            .map(|node| node.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["visible", "no-block"]
    );
}

/// Two nodes that tie on every live count are separated by what the terminator
/// would block on — `stopped` / `created` / `quarantined` rows the census
/// reports and the drain pipeline ignores. Without this term the node that
/// cannot finish outranks the one that can.
///
/// The count is census-aware on purpose: the node holding *more* raw occupancy
/// here is the one that terminates cleanly, because all of it is phantom rows
/// the terminator discounts. Ranking on the raw count would invert these two.
#[test]
fn drainability_rank_breaks_live_ties_on_termination_blockers() {
    let mut parked = ranked(0, 0, 0);
    parked.name = Some("parks-in-drained".to_string());
    parked.sandboxes_active = Some(1);
    parked.drainability = Some(AdminNodeDrainability {
        terminate_blockers: 1,
        ..parked.drainability.expect("fixture carries a block")
    });
    let mut clean = ranked(0, 0, 0);
    clean.name = Some("terminates-cleanly".to_string());
    // Fifty occupying rows, every one of them a phantom the terminator drops.
    clean.sandboxes_active = Some(50);
    clean.drainability = Some(AdminNodeDrainability {
        terminate_blockers: 0,
        ..clean.drainability.expect("fixture carries a block")
    });
    let mut nodes = [parked, clean];

    nodes.sort_by_key(drainability_rank);

    assert_eq!(
        nodes
            .iter()
            .map(|node| node.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["terminates-cleanly", "parks-in-drained"]
    );
}

/// A node whose drain gate is held open by target-pinned rows sorts behind an
/// otherwise-identical node with a clear gate, even though both report zero
/// live residents. Ranking on residency alone offered the blocked node first.
#[test]
fn drainability_rank_demotes_nodes_whose_drain_gate_is_held_open() {
    let mut blocked = ranked(0, 0, 3);
    blocked.name = Some("gate-held".to_string());
    let mut clear = ranked(0, 0, 0);
    clear.name = Some("gate-clear".to_string());
    let mut nodes = [blocked, clear];

    nodes.sort_by_key(drainability_rank);

    assert_eq!(
        nodes
            .iter()
            .map(|node| node.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["gate-clear", "gate-held"]
    );
}

/// `local_snapshots` is reported but must not order anything. A local-only
/// pinned snapshot is a strand risk an operator weighs by hand — it is not
/// something a gentle drain waits out, so letting it outrank a live
/// long-runner would order candidates by the wrong cost.
///
/// This pins the public contract on [`AdminNodeSort::Drainability`], which
/// consumers read to know what the endpoint guarantees. Nothing else asserts
/// this omission: every other ranking test varies terms that *do* count, so
/// `local_snapshots` joining the tuple would pass all of them. It is not a
/// general guard — a *different* new term joining the tuple is caught by
/// neither this test nor those.
#[test]
fn local_snapshots_are_reported_but_never_rank() {
    let with_snapshots = |local_snapshots: i64| {
        let mut node = ranked(0, 1, 0);
        let mut block = node.drainability.expect("ranked() populates the block");
        block.local_snapshots = local_snapshots;
        node.drainability = Some(block);
        node
    };

    let none = with_snapshots(0);
    let many = with_snapshots(500);

    assert_eq!(
        drainability_rank(&none),
        drainability_rank(&many),
        "local_snapshots must not enter the rank tuple"
    );

    // And it is still surfaced — "not a ranking term" must not decay into
    // "dropped", since the strand risk is the reason the column exists.
    assert_eq!(
        many.drainability
            .expect("ranked() populates the block")
            .local_snapshots,
        500
    );

    // A node with zero snapshots but one live long-runner still sorts behind a
    // node with 500 snapshots and none: the term that does not rank cannot
    // outweigh the term that does.
    let mut long_runner = ranked(1, 1, 0);
    let mut block = long_runner
        .drainability
        .expect("ranked() populates the block");
    block.local_snapshots = 0;
    long_runner.drainability = Some(block);
    assert!(
        drainability_rank(&many) < drainability_rank(&long_runner),
        "500 pinned snapshots must still rank ahead of a single live long-runner"
    );
}

/// An absent block and an all-zero block are different facts: all-zero is the
/// best possible gentle-drain candidate, absent means nobody asked. Both keys
/// must therefore be *missing* from the serialized response on the paths that
/// compute no counters — the unsorted listing and the drain-action echoes.
///
/// This is what `#[serde(skip_serializing_if = "Option::is_none")]` buys, and
/// it can only be checked on the response: asserting it on a bare
/// `Option::<AdminNodeDrainability>::None` would pass for any `T` and would
/// survive deleting the attribute. Without it, a future change to a non-`Option`
/// field plus `#[serde(default)]` would ship an all-zero block on every
/// unsorted listing and read as a fleet of perfect drain candidates.
#[test]
fn drainability_is_absent_from_the_response_until_it_is_computed() {
    let without = admin_node_response(
        test_admin_node_row(),
        Some(SandboxOccupancy::default()),
        None,
    );
    let json = serde_json::to_value(&without).expect("response serializes");
    let object = json.as_object().expect("response is a JSON object");
    assert!(
        !object.contains_key("drainability"),
        "an uncomputed block must be omitted, not zeroed: {json}"
    );
    assert!(
        !object.contains_key("residency_evidence"),
        "the policy travels with the block or not at all: {json}"
    );

    // And present once computed, so "omitted when absent" cannot decay into
    // "never serialized at all".
    let with = admin_node_response(
        test_admin_node_row(),
        Some(SandboxOccupancy::default()),
        Some((
            AdminNodeDrainability::default(),
            ResidencyEvidence::LifecycleOnly,
        )),
    );
    let json = serde_json::to_value(&with).expect("response serializes");
    assert_eq!(json["drainability"]["live_residents"], 0);
    assert_eq!(json["drainability"]["heartbeat_fresh"], false);
    assert_eq!(json["residency_evidence"], "lifecycle_only");
}

/// The threshold on the wire has to be the one the counts were computed
/// against, not the one the caller asked for and not a constant.
///
/// Without this, deleting `long_runner_age_hours`'s plumbing in
/// `drainability_block` and hardcoding the 24 h default keeps every other test
/// green: the store-level guard reads `NodeLiveResidents::long_runner_age`
/// directly and never inspects the wire field.
#[test]
fn the_response_echoes_the_threshold_the_counts_were_computed_against() {
    // A store row whose effective age differs from every default, so a
    // hardcoded constant cannot accidentally match.
    let occupancy = NodeOccupancy {
        occupancy: SandboxOccupancy::default(),
        heartbeat_fresh: true,
        live: Some(node_store::NodeLiveResidents {
            live_residents: 0,
            live_long_runners: 0,
            drain_blockers: 0,
            terminate_blockers: 0,
            residency: ResidencyEvidence::NodeCensusAware,
            long_runner_age: Duration::from_secs(168 * 60 * 60),
        }),
    };
    let (block, residency) = drainability_block(
        occupancy,
        ResidencyEvidence::LifecycleOnly,
        DEFAULT_LONG_RUNNER_AGE,
    );
    assert_eq!(block.long_runner_age_hours, 168);
    assert_eq!(
        residency,
        ResidencyEvidence::NodeCensusAware,
        "the row's own policy wins over the requested one, same as the age"
    );

    // A row carrying no counters falls back to the requested age rather than to
    // `Duration::ZERO`, which would serialize an age the request layer 400s on.
    let without = NodeOccupancy {
        occupancy: SandboxOccupancy::default(),
        heartbeat_fresh: false,
        live: None,
    };
    let (block, _) = drainability_block(
        without,
        ResidencyEvidence::LifecycleOnly,
        Duration::from_secs(48 * 60 * 60),
    );
    assert_eq!(block.long_runner_age_hours, 48);
    assert!(
        parse_long_runner_age(Some(block.long_runner_age_hours)).is_ok(),
        "an echoed threshold must be one the endpoint would accept back"
    );
}

/// The fail-closed guard in `drainability_block`: a row that carries no counters
/// must not keep its real heartbeat, because zeroed counts beside a *true*
/// heartbeat is the best possible rank key and would sort a node whose counts
/// were never computed to the top.
///
/// `Some(age) ⇒ Some(live)` makes the combination unreachable through the store
/// today, which is exactly why this is asserted directly on the function —
/// without it, deleting `&& row.live.is_some()` leaves every other test green.
#[test]
fn a_row_without_counters_is_ranked_blind_even_when_the_node_is_reachable() {
    let reachable_but_uncomputed = NodeOccupancy {
        occupancy: SandboxOccupancy::default(),
        heartbeat_fresh: true,
        live: None,
    };
    let (block, _) = drainability_block(
        reachable_but_uncomputed,
        ResidencyEvidence::LifecycleOnly,
        DEFAULT_LONG_RUNNER_AGE,
    );
    assert!(
        !block.heartbeat_fresh,
        "a block with no computed counters must read blind, not reachable"
    );

    let mut node = ranked(0, 0, 0);
    node.drainability = Some(block);
    let mut busy = ranked(0, 9, 9);
    busy.name = Some("busy-but-computed".to_string());
    node.name = Some("uncomputed".to_string());
    let mut nodes = [node, busy];
    nodes.sort_by_key(drainability_rank);
    assert_eq!(
        nodes
            .iter()
            .map(|n| n.name.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["busy-but-computed", "uncomputed"],
        "all-zero-but-uncomputed must not outrank a node with real counts"
    );
}

/// The `:terminate` occupancy guard, both halves.
///
/// Both active residents and local snapshots must block termination. The
/// rollout script calls this route for drained nodes, so checking snapshots
/// alone could destroy a node that still hosts a sandbox.
#[test]
fn terminate_is_blocked_by_either_half_of_occupancy() {
    let cases = [
        (0, 0, false, "an empty node must still be terminable"),
        (0, 3, true, "local-disk snapshots would be stranded"),
        (
            2,
            0,
            true,
            "resident sandboxes would die with the instance — the half this \
             endpoint used to compute and ignore",
        ),
        (2, 3, true, "both blockers present"),
    ];

    for (active, local_snapshots, blocked, why) in cases {
        assert_eq!(
            terminate_blocked_by_occupancy(&SandboxOccupancy {
                active,
                local_snapshots,
            }),
            blocked,
            "active={active} local_snapshots={local_snapshots}: {why}"
        );
    }
}

#[tokio::test]
async fn fleet_http_inventory_preserves_legacy_counts_and_explicit_opt_out() -> TestResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let nodes = [Uuid::now_v7(), Uuid::now_v7()];
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role)
         SELECT id, '127.0.0.1'::INET, 'worker' FROM UNNEST($1::UUID[]) id"
    ))
    .bind(nodes.as_slice())
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, managed_by,
                               active_node_id, target_node_id, network_policy_generation)
         SELECT space_uuid_v7(), '00000000-0000-0000-0000-000000000010',
                '00000000-0000-0000-0000-000000000001', lifecycle, 'placed', 'k8s',
                CASE WHEN lifecycle = 'started' THEN $1::UUID END, $1, 1
           FROM UNNEST(ARRAY['started', 'started', 'suspended']) lifecycle"
    ))
    .bind(nodes[0])
    .execute(&db.pool)
    .await?;
    let actor =
        inventory_test_actor(crate::routes::authz::cluster_managing_org_id(&db.pool).await?);
    let app = axum::Router::new()
        .route("/admin/api/v1/nodes", axum::routing::get(list_admin_nodes))
        .layer(Extension(AuthUser(actor)))
        .with_state(AppState::from_pool(db.pool.clone()).await?);
    for (suffix, counts, ranked) in [
        ("", true, false),
        ("?include_occupancy=true", true, false),
        ("?include_occupancy=false", false, false),
        ("?include_occupancy=false&sort=drainability", true, true),
    ] {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/admin/api/v1/nodes{suffix}"))
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK, "{suffix}");
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
        let rows = body.as_array().expect("node list");
        assert_eq!(rows.len(), nodes.len());
        for (index, node) in nodes.iter().enumerate() {
            let row = rows
                .iter()
                .find(|row| row["id"] == node.to_string())
                .expect("requested node");
            if counts {
                assert_eq!(
                    row["sandboxes_active"],
                    if index == 0 { 2 } else { 0 },
                    "{suffix}"
                );
                assert_eq!(
                    row["sandboxes_local_snapshots"],
                    if index == 0 { 1 } else { 0 },
                    "{suffix}"
                );
            } else {
                assert!(row["sandboxes_active"].is_null());
                assert!(row["sandboxes_local_snapshots"].is_null());
            }
            assert_eq!(row.get("drainability").is_some(), ranked, "{suffix}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn inventory_reads_skip_locked_sandbox_accounting_but_diagnostics_do_not() -> TestResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            db.pool
                .connect_options()
                .as_ref()
                .clone()
                .options([("lock_timeout", "100ms")]),
        )
        .await?;
    let state = AppState::from_pool(pool).await?;
    let actor =
        inventory_test_actor(crate::routes::authz::cluster_managing_org_id(&db.pool).await?);
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role) VALUES ($1, '127.0.0.1', 'worker'),
         (space_uuid_v7(), '127.0.0.2', 'worker')"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;
    let mut lock = db.pool.begin().await?;
    sqlx::query(sql!("LOCK TABLE sandboxes IN ACCESS EXCLUSIVE MODE"))
        .execute(&mut *lock)
        .await?;

    let response = tokio::time::timeout(
        Duration::from_secs(2),
        list_admin_nodes(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Query(ListAdminNodesFilters {
                include_occupancy: Some(false),
                ..Default::default()
            }),
        ),
    )
    .await?
    .into_response();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
    assert_eq!(body.as_array().unwrap().len(), 2);
    assert!(body[0]["sandboxes_active"].is_null());
    assert!(body[0]["sandboxes_local_snapshots"].is_null());
    assert!(body[0].get("drainability").is_none());
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        get_admin_node(
            Query(GetAdminNodeFilters {
                include_occupancy: Some(false),
            }),
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(node_id.to_string()),
        ),
    )
    .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
    assert!(body["sandboxes_active"].is_null());

    // Explicit diagnostics fail closed on the database error.
    let response = list_admin_nodes(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Query(ListAdminNodesFilters::default()),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response = get_admin_node(
        Query(GetAdminNodeFilters::default()),
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(node_id.to_string()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response = list_admin_nodes(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Query(ListAdminNodesFilters {
            include_occupancy: Some(false),
            sort: Some("drainability".into()),
            ..Default::default()
        }),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let mut guard_tx = db.pool.begin().await?;
    sqlx::query(sql!("SET LOCAL lock_timeout = '100ms'"))
        .execute(&mut *guard_tx)
        .await?;
    let error = node_store::sandbox_occupancy_for_node_in_tx(
        &mut guard_tx,
        node_id,
        ResidencyEvidence::LifecycleOnly,
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("55P03")
    );
    guard_tx.rollback().await?;
    lock.rollback().await?;

    // Legacy callers still receive numeric zero once it has actually been computed.
    let response = get_admin_node(
        Query(GetAdminNodeFilters::default()),
        State(state),
        Extension(AuthUser(actor)),
        Path(node_id.to_string()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
    assert_eq!(body["sandboxes_active"], 0);
    assert_eq!(body["sandboxes_local_snapshots"], 0);
    assert!(body.get("drainability").is_some());
    Ok(())
}

fn inventory_test_actor(org_id: OrgId) -> User {
    User {
        id: UserId::new(),
        email: "inventory-test@example.invalid".into(),
        password_hash: String::new(),
        display_name: "Inventory test".into(),
        org_id,
        role: UserRole::OrganizationAdmin,
        owner_source: pplx_space_common_core::types::SandboxOwnerSource::Local,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    }
}

#[tokio::test]
async fn node_sandbox_pages_are_scoped_deduplicated_and_complete() -> TestResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor =
        inventory_test_actor(crate::routes::authz::cluster_managing_org_id(&db.pool).await?);
    let node_id = NodeId::new();
    let other_node = Uuid::now_v7();
    sqlx::query(sql!("INSERT INTO nodes (id, address, role) VALUES ($1, '127.0.0.1', 'worker'), ($2, '127.0.0.2', 'worker')"))
        .bind(node_id).bind(other_node).execute(&db.pool).await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, placement, managed_by,
            active_node_id, target_node_id, network_policy_generation)
         SELECT space_uuid_v7(), '00000000-0000-0000-0000-000000000010',
            '00000000-0000-0000-0000-000000000001', 'started', 'placed', 'k8s',
            CASE WHEN n <= 150 THEN $1 ELSE $2 END,
            CASE WHEN n <= 205 THEN $1 ELSE $2 END, 1
         FROM generate_series(1, 210) n"
    ))
    .bind(node_id)
    .bind(other_node)
    .execute(&db.pool)
    .await?;
    let mut after = None;
    let mut ids = Vec::new();
    for expected_len in [100, 100, 5] {
        let response = list_node_sandboxes(
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(node_id),
            Query(NodeSandboxesQuery { after }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
        let rows = body["sandboxes"].as_array().unwrap();
        assert_eq!(rows.len(), expected_len);
        for row in rows {
            assert!(
                row["active_node_id"] == node_id.to_string()
                    || row["target_node_id"] == node_id.to_string()
            );
            ids.push(row["id"].as_str().unwrap().to_string());
        }
        after = serde_json::from_value(body["next_after"].clone())?;
        assert_eq!(after.is_some(), expected_len == 100);
    }
    assert_eq!(ids.len(), 205);
    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    let response = list_node_sandboxes(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(NodeId::new()),
        Query(NodeSandboxesQuery { after: None }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let mut member = actor;
    member.role = UserRole::Member;
    let response = list_node_sandboxes(
        State(state),
        Extension(AuthUser(member)),
        Path(node_id),
        Query(NodeSandboxesQuery { after: None }),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    Ok(())
}

/// `AppState::from_pool` picks its substrate from the process environment, so
/// the schedulability tests pin it to Direct and swap a Kubernetes substrate
/// in where they need one. Returns false in the parent process.
fn enter_direct_test_process() -> bool {
    pplx_space_test_env::enter_test_process(&[(
        "SPACE_K8S_CONTROL_PLANE",
        Some(std::ffi::OsStr::new("false")),
    )])
}

/// `nodes` columns the drain and cordon actions write.
type DrainColumns = (
    Option<String>,
    Option<String>,
    Option<OffsetDateTime>,
    Option<String>,
    bool,
);

async fn insert_drain_test_node(
    pool: &sqlx::PgPool,
    managed_by: NodeManagedBy,
    drain_phase: Option<&str>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, drain_phase, drain_mode,
                            drain_started_at, drain_last_error)
         VALUES ($1, '127.0.0.1', 'worker', $2, $3,
                 CASE WHEN $3::TEXT IS NULL THEN NULL ELSE 'forced' END,
                 CASE WHEN $3::TEXT IS NULL THEN NULL ELSE NOW() END,
                 CASE WHEN $3::TEXT IS NULL THEN NULL
                      ELSE 'substrate cordon patch failed' END)"
    ))
    .bind(node_id)
    .bind(managed_by.as_str())
    .bind(drain_phase)
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn drain_columns(pool: &sqlx::PgPool, node_id: NodeId) -> TestResult<DrainColumns> {
    Ok(sqlx::query_as(sql!(
        "SELECT drain_phase, drain_mode, drain_started_at, drain_last_error,
                terminate_decrement_capacity
           FROM nodes
          WHERE id = $1"
    ))
    .bind(node_id)
    .fetch_one(pool)
    .await?)
}

async fn post_node_action(
    state: &AppState,
    actor: &User,
    node_id: NodeId,
    action: DrainAction,
    body: &'static str,
) -> TestResult<(StatusCode, serde_json::Value)> {
    let response = dispatch_node_action(
        State(state.clone()),
        Extension(AuthUser(actor.clone())),
        Path(format!("{node_id}:{}", action.label())),
        Bytes::from_static(body.as_bytes()),
    )
    .await;
    let status = response.status();
    let body = serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
    Ok((status, body))
}

/// Direct has no node scheduling primitive and runs no node-drain reconciler,
/// so a committed drain phase could never converge or advance. Each action is
/// refused before the node is read or locked: an unknown id gets the same 501
/// rather than a 404, and a node whose row another transaction holds
/// `FOR UPDATE` is refused without waiting for it. That node starts in a phase
/// the action would otherwise accept, so the refusal is the substrate's, and
/// its row must read the same afterwards.
#[tokio::test(flavor = "current_thread")]
async fn direct_refuses_node_drain_and_cordon_before_writing() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor =
        inventory_test_actor(crate::routes::authz::cluster_managing_org_id(&db.pool).await?);
    let refused = serde_json::json!({
        "error": "node drain and cordon are not supported on the Direct substrate"
    });

    for (action, phase, body) in [
        (DrainAction::Drain, None, r#"{"mode":"forced"}"#),
        (DrainAction::Terminate, Some(PHASE_DRAINED), ""),
        (DrainAction::Cordon, None, ""),
        (DrainAction::Uncordon, None, ""),
        // Forced `draining` closes the wake gate this action requires.
        (
            DrainAction::ForceUploadLocalSnapshots,
            Some(PHASE_DRAINING),
            "",
        ),
    ] {
        let label = action.label();
        let unknown = post_node_action(&state, &actor, NodeId::new(), action, body).await?;
        assert_eq!(
            unknown,
            (StatusCode::NOT_IMPLEMENTED, refused.clone()),
            "{label}"
        );

        let node_id = insert_drain_test_node(&db.pool, NodeManagedBy::Space, phase).await?;
        let before = drain_columns(&db.pool, node_id).await?;
        let mut lock = db.pool.begin().await?;
        sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE"))
            .bind(node_id)
            .execute(&mut *lock)
            .await?;

        let locked = tokio::time::timeout(
            Duration::from_secs(5),
            post_node_action(&state, &actor, node_id, action, body),
        )
        .await
        .map_err(|_| format!("{label} waited on the node's row lock"))??;

        lock.rollback().await?;
        assert_eq!(
            locked,
            (StatusCode::NOT_IMPLEMENTED, refused.clone()),
            "{label}"
        );
        assert_eq!(drain_columns(&db.pool, node_id).await?, before, "{label}");
    }
    Ok(())
}

/// A Direct node can still hold the `draining` phase an older `:drain`
/// committed before it failed the cordon patch. `:undrain` clears it, with
/// the mode the shared drain gates read and the error the failed patch left,
/// and needs no substrate call to succeed.
#[tokio::test(flavor = "current_thread")]
async fn direct_undrain_clears_a_phase_an_earlier_drain_left() -> TestResult {
    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor =
        inventory_test_actor(crate::routes::authz::cluster_managing_org_id(&db.pool).await?);
    let node_id =
        insert_drain_test_node(&db.pool, NodeManagedBy::Space, Some(PHASE_DRAINING)).await?;

    let (status, body) =
        post_node_action(&state, &actor, node_id, DrainAction::Undrain, "").await?;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["drain_phase"].is_null(), "{body}");
    assert_eq!(
        drain_columns(&db.pool, node_id).await?,
        (None, None, None, None, false)
    );
    Ok(())
}

/// Positive control for the Direct refusal: on Kubernetes every action still
/// runs, each drain-phase action commits its transition, and each one with a
/// scheduling effect patches its node's `spec.unschedulable`.
#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn kubernetes_node_drain_and_cordon_converge_the_node() -> TestResult {
    use axum::http::{Method, Request, Response};
    use http_body_util::BodyExt;
    use kube::client::{Body, ClientBuilder};
    use std::sync::{Arc, Mutex};

    if !enter_direct_test_process() {
        return Ok(());
    }
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let patches: Arc<Mutex<Vec<(String, serde_json::Value)>>> = Arc::default();
    let recorded = Arc::clone(&patches);
    let client = ClientBuilder::new(
        tower::service_fn(move |req: Request<Body>| {
            let recorded = Arc::clone(&recorded);
            async move {
                assert_eq!(req.method(), Method::PATCH, "{}", req.uri());
                let name = req
                    .uri()
                    .path()
                    .strip_prefix("/api/v1/nodes/")
                    .expect("a core.v1.Node patch")
                    .to_string();
                let patch: serde_json::Value =
                    serde_json::from_slice(&req.into_body().collect().await?.to_bytes())?;
                let node = serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {"name": name},
                    "spec": patch["spec"],
                });
                recorded.lock().unwrap().push((name, patch));
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Response::new(Body::from(
                    serde_json::to_vec(&node)?,
                )))
            }
        }),
        "space",
    )
    .build();
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    state.sandbox_controller = crate::controllers::sandbox::SandboxController::new(
        crate::controllers::substrate::Substrate::Kubernetes(Box::new(
            crate::controllers::substrate::kubernetes::tests::substrate_with_store_and_client(
                db.pool.clone(),
                client,
            ),
        )),
    );
    let actor =
        inventory_test_actor(crate::routes::authz::cluster_managing_org_id(&db.pool).await?);

    for (action, phase, unschedulable, phase_after) in [
        (DrainAction::Drain, None, Some(true), Some(PHASE_DRAINING)),
        (DrainAction::Cordon, None, Some(true), None),
        (DrainAction::Uncordon, None, Some(false), None),
        (
            DrainAction::Undrain,
            Some(PHASE_DRAINING),
            Some(false),
            None,
        ),
        (
            DrainAction::Terminate,
            Some(PHASE_DRAINED),
            None,
            Some(PHASE_TERMINATING),
        ),
        // The node holds no local-only snapshots, so there is nothing to upload.
        (
            DrainAction::ForceUploadLocalSnapshots,
            Some(PHASE_DRAINING),
            None,
            Some(PHASE_DRAINING),
        ),
    ] {
        let label = action.label();
        let node_id = insert_drain_test_node(&db.pool, NodeManagedBy::Kubernetes, phase).await?;
        let node_name = format!("kind-worker-{label}");
        sqlx::query(sql!(
            "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
        ))
        .bind(node_id)
        .bind(&node_name)
        .execute(&db.pool)
        .await?;

        let (status, body) = post_node_action(&state, &actor, node_id, action, "").await?;

        assert_eq!(status, StatusCode::OK, "{label}: {body}");
        assert_eq!(
            drain_columns(&db.pool, node_id).await?.0.as_deref(),
            phase_after,
            "{label}"
        );
        let node_patches: Vec<_> = patches
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| *name == node_name)
            .map(|(_, patch)| patch.clone())
            .collect();
        let expected: Vec<_> = unschedulable
            .map(|value| serde_json::json!({"spec": {"unschedulable": value}}))
            .into_iter()
            .collect();
        assert_eq!(node_patches, expected, "{label}");
    }
    Ok(())
}

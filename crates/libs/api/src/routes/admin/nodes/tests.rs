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
        execution_inventory_status: None,
        execution_inventory_received_at: None,
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

/// `:terminate`, `:undrain`, `:cordon`, `:uncordon` and
/// `:force-upload-local-snapshots` never consult a drain mode, so the tests
/// whose subject is the phase table say so by going through this rather than
/// naming a mode pair each call. `:drain` is the exception and calls
/// [`validate_drain_transition`] directly.
fn phase_only(
    phase: Option<&str>,
    action: DrainAction,
) -> Result<DrainTransition, TransitionError> {
    validate_drain_transition(phase, NodeDrainMode::Gentle, action, None)
}

/// A `:drain` request as the validator sees it: the node's resolved current
/// mode and the mode the operator asked for.
fn drain(
    phase: Option<&str>,
    current_mode: NodeDrainMode,
    requested_mode: NodeDrainMode,
) -> Result<DrainTransition, TransitionError> {
    validate_drain_transition(
        phase,
        current_mode,
        DrainAction::Drain,
        Some(requested_mode),
    )
}

#[test]
fn drain_starts_a_drain_only_from_no_phase() {
    let t = drain(None, NodeDrainMode::Gentle, NodeDrainMode::Gentle).unwrap();
    assert!(matches!(
        t,
        DrainTransition::Move {
            new: Some(PHASE_DRAINING),
            set_started_at: true,
            clear_started_at: false,
        }
    ));

    // `drained` and `terminating` are past the point where re-draining means
    // anything — the node is empty and on its way to being destroyed. Only
    // `draining` gained an edge; see `escalating_a_gentle_drain_*` below.
    for phase in [PHASE_DRAINED, PHASE_TERMINATING] {
        for requested in [NodeDrainMode::Gentle, NodeDrainMode::Forced] {
            let err = drain(Some(phase), NodeDrainMode::Gentle, requested).unwrap_err();
            assert!(
                matches!(err, TransitionError::Conflict { .. }),
                "{phase} + {requested:?} must stay a conflict"
            );
        }
    }
}

/// The operator-visible property: asking for `forced` on a node that is already
/// draining gently makes the drain destructive **without restarting it**. A
/// `Move` with `set_started_at: true` would satisfy "the mode changed" and still
/// be wrong: it resets the drain's age, which both an operator and
/// `space-rollout-mng.py`'s stuck detection read.
/// `node_store::update_in_flight_drain` names each reader.
#[test]
fn escalating_a_gentle_drain_keeps_the_drains_age() {
    let t = drain(
        Some(PHASE_DRAINING),
        NodeDrainMode::Gentle,
        NodeDrainMode::Forced,
    )
    .unwrap();
    // The variants are disjoint, so this alone rules out the `Move` path whose
    // `:drain` arm stamps `drain_started_at = NOW()`. The *column* half of the
    // property — that the write really does leave the stamp alone — is not
    // testable here and is pinned by
    // `node_store::in_flight_drain_tests::escalation_rewrites_the_mode_and_keeps_the_drains_age`
    // against a backdated row.
    assert_eq!(
        t,
        DrainTransition::UpdateInFlightDrain(Some(NodeDrainMode::Forced))
    );
}

/// That a NULL or unparseable `nodes.drain_mode` is escalatable is a
/// composition of two properties pinned elsewhere —
/// `types::node::tests::{absent,unrecognized}_drain_mode_column_reads_as_gentle`
/// for the resolution, and `escalating_a_gentle_drain_keeps_the_drains_age` for
/// the transition. There was a test here restating that composition; it asserted
/// nothing independently breakable, and its docstring claimed to pin the
/// handler's use of `from_column_str`, which no test in this file reaches —
/// every test here calls a pure helper. `drain_handler_tests` reaches it, by
/// driving `:drain` against a real draining row.
///
/// A drain cannot be made *less* aggressive in place: the suspends and
/// cross-node restores a forced drain has already run have no inverse, so the
/// node would be left half-evacuated in a state neither mode describes.
/// Refused with both modes named, because "409 conflict" plus a phase list
/// would point the operator at the phase, which is not what was wrong.
#[test]
fn a_forced_drain_cannot_be_downgraded_to_gentle() {
    let err = drain(
        Some(PHASE_DRAINING),
        NodeDrainMode::Forced,
        NodeDrainMode::Gentle,
    )
    .unwrap_err();
    let TransitionError::ModeDowngrade { current_mode } = err else {
        panic!("expected ModeDowngrade, got {err:?}");
    };
    assert_eq!(current_mode, NodeDrainMode::Forced);
}

/// Re-asserting the mode a node already has is idempotent *on the mode*, and not
/// an error: an operator who re-runs `space-admin-cli drain-node --forced`
/// after a timeout should find the node in the state they asked for, not a 409
/// to special-case.
///
/// It takes the same arm as the escalation, `UpdateInFlightDrain`, but with
/// `None` for the mode, rather than a distinct no-op arm. That is deliberate
/// and is the difference from `:cordon`: the repeat still writes, because it
/// clears `drain_last_error`, while `None` leaves the mode column as it is
/// (including a token this build can't parse). `drain_started_at` is untouched
/// either way — the store function is what guarantees that.
#[test]
fn re_asserting_the_same_drain_mode_keeps_that_mode() {
    for mode in [NodeDrainMode::Gentle, NodeDrainMode::Forced] {
        assert_eq!(
            drain(Some(PHASE_DRAINING), mode, mode).unwrap(),
            DrainTransition::UpdateInFlightDrain(None),
            "{mode:?} -> {mode:?} must not rewrite the mode column"
        );
    }
}

#[test]
fn terminate_only_allowed_from_drained() {
    let t = phase_only(Some(PHASE_DRAINED), DrainAction::Terminate).unwrap();
    assert!(matches!(
        t,
        DrainTransition::Move {
            new: Some(PHASE_TERMINATING),
            ..
        }
    ));

    for phase in [None, Some(PHASE_DRAINING), Some(PHASE_TERMINATING)] {
        let err = phase_only(phase, DrainAction::Terminate).unwrap_err();
        assert!(matches!(err, TransitionError::Conflict { .. }));
    }
}

#[test]
fn undrain_from_draining_or_drained() {
    for phase in [PHASE_DRAINING, PHASE_DRAINED] {
        let t = phase_only(Some(phase), DrainAction::Undrain).unwrap();
        assert!(matches!(
            t,
            DrainTransition::Move {
                new: None,
                set_started_at: false,
                clear_started_at: true,
            }
        ));
    }

    let err = phase_only(None, DrainAction::Undrain).unwrap_err();
    assert!(matches!(err, TransitionError::Conflict { .. }));

    let err = phase_only(Some(PHASE_TERMINATING), DrainAction::Undrain).unwrap_err();
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
        let transition = phase_only(phase, DrainAction::ForceUploadLocalSnapshots).unwrap();
        assert!(matches!(transition, DrainTransition::NoChange));
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
            phase_only(phase, DrainAction::Cordon).unwrap(),
            DrainTransition::NoChange
        ));
    }
}

#[test]
fn uncordon_refuses_while_a_drain_is_active() {
    // No drain in progress: standalone uncordon is fine.
    assert!(matches!(
        phase_only(None, DrainAction::Uncordon).unwrap(),
        DrainTransition::NoChange
    ));

    // Drain in any non-null phase blocks uncordon — operators must use
    // `:undrain`, which clears the phase and uncordons atomically.
    for phase in [PHASE_DRAINING, PHASE_DRAINED, PHASE_TERMINATING] {
        let err = phase_only(Some(phase), DrainAction::Uncordon).unwrap_err();
        assert!(matches!(err, TransitionError::Conflict { .. }));
    }
}

/// An omitted `replace_after_terminate` is `None`, not `Some(true)`, and
/// resolves to the auto-replace default only on the edge that starts a drain.
///
/// The distinction is load-bearing on the escalation edge, where `None` means
/// "leave the in-flight drain's intent alone" — so a test that only checked the
/// resolved bool would pass while the escalation reverted a blue-green shrink.
#[test]
fn an_omitted_replace_after_terminate_is_unexpressed_and_defaults_to_replace() {
    for body in [&b""[..], b"{}"] {
        let req = parse_drain_request(body).unwrap();
        // This pins the parse only. That an omitted field *resolves* to
        // auto-replace is a separate, independently-breakable property — flipping
        // `unwrap_or(true)` leaves this assertion green — and lives with the type
        // in
        // `types::admin::tests::an_unexpressed_replace_after_terminate_resolves_to_auto_replace`.
        assert_eq!(
            req.replace_after_terminate, None,
            "{body:?}: an omitted field must stay distinguishable from an explicit true"
        );
    }
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
    assert_eq!(req.replace_after_terminate, Some(false));
    assert!(persisted_decrement_capacity(&req));
}

#[test]
fn drain_with_explicit_replace_after_terminate_true_persists_decrement_capacity_false() {
    let req = parse_drain_request(br#"{"replace_after_terminate": true}"#).unwrap();
    assert_eq!(
        req.replace_after_terminate,
        Some(true),
        "an explicit true must be distinguishable from an omitted field"
    );
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

/// The 502 body's `reconciler_will_retry` must be true only for the phases the
/// reconciler's work lists actually scan.
///
/// The operator-visible property: a 502 that says a retry is coming is an
/// instruction to wait. On a `drained` node nothing is coming — no reconciler
/// re-cordons it — so waiting leaves the node schedulable until the drained-node
/// terminator destroys it, and any sandbox placed in that window dies with the
/// instance. `:cordon` is exactly the call an operator makes to fence such a
/// node, so this is the reading most likely to be trusted.
///
/// Asserted over every phase rather than just `drained`, because the failure is
/// a predicate that answers "is a phase set?" instead of "which phase?" — and
/// that predicate is right for three of the four inputs, wrong only on
/// `drained`.
#[test]
fn only_draining_and_terminating_promise_an_automatic_cordon_retry() {
    assert!(reconciler_reasserts_cordon(
        Some(PHASE_DRAINING),
        NodeManagedBy::Kubernetes
    ));
    assert!(reconciler_reasserts_cordon(
        Some(PHASE_TERMINATING),
        NodeManagedBy::Kubernetes
    ));
    assert!(
        !reconciler_reasserts_cordon(Some(PHASE_DRAINED), NodeManagedBy::Kubernetes),
        "no reconciler re-cordons a drained node; promising a retry parks it \
         schedulable until the terminator destroys it"
    );
    assert!(
        !reconciler_reasserts_cordon(None, NodeManagedBy::Kubernetes),
        "an undrained node is out of the reconciler's scope entirely"
    );

    // `managed_by` is the second conjunct of both work lists, and the case where
    // getting it wrong is worst: a `space`-managed node has no `node_k8s` row, so
    // `converge_substrate_schedulability` fails it on *every* `:drain`. Answering
    // "a retry is coming" there is the one reading that is always wrong.
    for phase in [Some(PHASE_DRAINING), Some(PHASE_TERMINATING)] {
        assert!(
            !reconciler_reasserts_cordon(phase, NodeManagedBy::Space),
            "{phase:?}: a direct-managed node is in neither work list"
        );
    }
}

/// A fresh `:drain --forced` still takes the drain-starting edge, so it still
/// persists the request's `replace_after_terminate`.
///
/// `set_started_at: true` is the sole selector for writing
/// `terminate_decrement_capacity`, and the mode does not participate in choosing
/// that edge. Without this, a refactor that made the `NULL` arm mode-dependent
/// would pass the rest of the suite while silently dropping
/// `--no-replace-after-terminate` on every forced first drain of a blue-green
/// shrink — a source MNG that never shrinks, discovered hours later.
#[test]
fn a_fresh_forced_drain_still_starts_the_clock() {
    for requested in [NodeDrainMode::Gentle, NodeDrainMode::Forced] {
        assert_eq!(
            drain(None, NodeDrainMode::Gentle, requested).unwrap(),
            DrainTransition::Move {
                new: Some(PHASE_DRAINING),
                set_started_at: true,
                clear_started_at: false,
            },
            "{requested:?}: a fresh drain must start the clock whatever mode it asks for"
        );
    }
}

/// A `:drain` against a drain already in flight may not change that drain's
/// replacement intent — but only an *expressed* disagreement is a conflict.
///
/// The column is the inverse of the request field, which is the whole reason
/// this is a named function: `terminate_decrement_capacity = true` means the ASG
/// terminate decrements desired capacity, i.e. `replace_after_terminate = false`.
/// An off-by-one-inversion here refuses exactly the requests it should accept.
#[test]
fn only_an_expressed_disagreement_conflicts_with_the_drain_in_flight() {
    // Stored `decrement = true` is a monotonic shrink, i.e. replace = false.
    assert_eq!(
        in_flight_replace_conflict(Some(true), true),
        Some(ReplaceConflict {
            stored_replace: false,
            requested_replace: true,
        })
    );
    assert_eq!(in_flight_replace_conflict(Some(false), true), None);
    // Stored `decrement = false` is the auto-replace default, i.e. replace = true.
    assert_eq!(
        in_flight_replace_conflict(Some(false), false),
        Some(ReplaceConflict {
            stored_replace: true,
            requested_replace: false,
        })
    );
    assert_eq!(in_flight_replace_conflict(Some(true), false), None);
    // Unexpressed is never a conflict, whatever is stored — this is what keeps a
    // bodiless repeat and a `space-admin-cli` that omits the flag on the 200 path.
    for stored in [true, false] {
        assert_eq!(
            in_flight_replace_conflict(None, stored),
            None,
            "stored={stored}: an unexpressed intent must not be refused"
        );
    }
}

/// The two recovery hints the 502 body and the `drain_last_error` write read are
/// different questions, and each consumer has to read its own.
///
/// Asserted as a pair per transition because each flag alone is a one-line
/// `matches!` that looks obviously right; it is their *disagreement* that
/// carries the meaning, and a pair is the smallest thing that can pin it.
///
/// What this test cannot reach is which consumer reads which flag — it calls
/// `recovery_hints` directly, so a swap at the call site leaves it green while
/// reintroducing both regressions the split exists to fix (`:cordon` on a
/// draining node under-promising a retry the reconciler is already performing,
/// and `:undrain` over-promising one that nothing will ever perform). That is
/// what the named `RecoveryHints` fields make unrepresentable, and what
/// `drain_handler_tests` covers by driving the handler.
#[test]
fn the_two_recovery_hints_disagree_exactly_where_they_should() {
    let start = DrainTransition::Move {
        new: Some(PHASE_DRAINING),
        set_started_at: true,
        clear_started_at: false,
    };
    let undrain = DrainTransition::Move {
        new: None,
        set_started_at: false,
        clear_started_at: true,
    };
    let in_flight = DrainTransition::UpdateInFlightDrain(Some(NodeDrainMode::Forced));

    // (wrote the drain columns, reconciler will re-assert cordon)
    assert_eq!(
        recovery_hints(&start, Some(PHASE_DRAINING), NodeManagedBy::Kubernetes),
        RecoveryHints {
            drain_columns_written: true,
            reconciler_will_reassert: true,
        }
    );
    assert_eq!(
        recovery_hints(&in_flight, Some(PHASE_DRAINING), NodeManagedBy::Kubernetes),
        RecoveryHints {
            drain_columns_written: true,
            reconciler_will_reassert: true,
        }
    );
    // `:undrain` wrote, and is the case where the two must differ: the phase is
    // gone, so nothing re-asserts, but the error is still worth recording.
    assert_eq!(
        recovery_hints(&undrain, None, NodeManagedBy::Kubernetes),
        RecoveryHints {
            drain_columns_written: true,
            reconciler_will_reassert: false,
        }
    );
    // `:cordon` on a draining node wrote nothing, and is the other case where
    // they must differ: no trace to record, but the reconciler does re-cordon.
    assert_eq!(
        recovery_hints(
            &DrainTransition::NoChange,
            Some(PHASE_DRAINING),
            NodeManagedBy::Kubernetes
        ),
        RecoveryHints {
            drain_columns_written: false,
            reconciler_will_reassert: true,
        }
    );
    // `:cordon` on a terminating node is the same disagreement as draining —
    // covered because the terminating handler is a second, independently
    // deletable caller of `ensure_cordon`.
    assert_eq!(
        recovery_hints(
            &DrainTransition::NoChange,
            Some(PHASE_TERMINATING),
            NodeManagedBy::Kubernetes
        ),
        RecoveryHints {
            drain_columns_written: false,
            reconciler_will_reassert: true,
        }
    );
    // `:cordon` on a drained node: neither. The one phase nothing scans.
    assert_eq!(
        recovery_hints(
            &DrainTransition::NoChange,
            Some(PHASE_DRAINED),
            NodeManagedBy::Kubernetes
        ),
        RecoveryHints {
            drain_columns_written: false,
            reconciler_will_reassert: false,
        }
    );
}

/// The replace-conflict 409 is a sentence an operator reads in a terminal, so it
/// has to be one — no run of collapsed indentation from a hand-wrapped literal,
/// and it has to name both values and the way out.
///
/// Asserted because the first draft of this message shipped with two 22-space
/// runs. That was unreachable by any test while the literal was inline in the
/// handler, which is the argument for the extraction more than for this test.
#[test]
fn the_replace_conflict_message_reads_as_one_sentence() {
    for (stored, requested) in [(false, true), (true, false)] {
        let message = replace_conflict_message(stored, requested);
        assert!(!message.contains("  "), "no double spaces; got {message:?}");
        assert!(!message.contains('\n'), "single line; got {message:?}");
        assert!(
            message.contains(&format!("replace_after_terminate={stored}")),
            "must name the stored value; got {message:?}"
        );
        assert!(
            message.contains(&format!("asked for {requested}")),
            "must name the requested value; got {message:?}"
        );
        // The remedy, not just the refusal — this is the only place an operator
        // learns that changing the terms means restarting the drain.
        assert!(
            message.contains(":undrain"),
            "must name the way out; got {message:?}"
        );
    }
}

/// The column and the request field are inverses, and this is the only place
/// that inversion happens — so it is the only thing standing between
/// `--no-replace-after-terminate` and a blue-green shrink that never shrinks.
///
/// Asserted against the production function rather than a restatement of it.
/// The predecessor of this test restated `!or_default()` in a local helper and
/// therefore asserted its own copy: deleting the `!` from `apply_drain_action`
/// failed nothing in the crate.
#[test]
fn the_persisted_decrement_capacity_inverts_the_request_field() {
    let shrink = DrainRequest {
        replace_after_terminate: Some(false),
        ..DrainRequest::default()
    };
    assert!(
        persisted_decrement_capacity(&shrink),
        "replace_after_terminate=false must persist decrement=true, or the ASG \
         replaces every instance a monotonic shrink terminates"
    );

    let replace = DrainRequest {
        replace_after_terminate: Some(true),
        ..DrainRequest::default()
    };
    assert!(!persisted_decrement_capacity(&replace));

    // An omitted field resolves to auto-replace on this edge, so it persists the
    // same column value as an explicit `true` — the single-node-drain default.
    assert!(!persisted_decrement_capacity(&DrainRequest::default()));
}

/// Only the edge that *starts* a drain may persist a replacement intent.
///
/// This is the sole guard on the sole writer of
/// `nodes.terminate_decrement_capacity`. Widening it to any `Move` is silent:
/// `:terminate` would write the default `false` over a shrink's `true` at the
/// moment the terminator reads it, with no error and a normal-looking audit
/// entry. Asserted over every transition rather than just the two `:drain`
/// edges, because the failure is a guard that admits one transition too many.
#[test]
fn only_the_drain_starting_edge_persists_a_replacement_intent() {
    let start = DrainTransition::Move {
        new: Some(PHASE_DRAINING),
        set_started_at: true,
        clear_started_at: false,
    };
    assert!(transition_starts_a_drain(&start));

    let terminate = DrainTransition::Move {
        new: Some(PHASE_TERMINATING),
        set_started_at: false,
        clear_started_at: false,
    };
    assert!(
        !transition_starts_a_drain(&terminate),
        ":terminate must not rewrite the initiating drain's replacement intent"
    );

    let undrain = DrainTransition::Move {
        new: None,
        set_started_at: false,
        clear_started_at: true,
    };
    assert!(!transition_starts_a_drain(&undrain));

    for mode in [None, Some(NodeDrainMode::Forced)] {
        assert!(
            !transition_starts_a_drain(&DrainTransition::UpdateInFlightDrain(mode)),
            "{mode:?}: a drain in flight keeps the intent it started with"
        );
    }
    assert!(!transition_starts_a_drain(&DrainTransition::NoChange));
}

/// The phase a transition leaves behind, which both the audit's `to_phase` and
/// the 502's retry promise read.
///
/// `UpdateInFlightDrain` is the case worth asserting: it re-asserts `draining`,
/// and returning `None` there — mirroring `:undrain`, or copying `NoChange`'s
/// pass-through — would tell an operator whose cordon patch failed that no retry
/// is coming, on the one edge where the reconciler re-cordons every tick.
#[test]
fn the_phase_after_a_transition_is_the_phase_the_row_holds() {
    for mode in [None, Some(NodeDrainMode::Forced)] {
        assert_eq!(
            phase_after(
                &DrainTransition::UpdateInFlightDrain(mode),
                Some(PHASE_DRAINING)
            ),
            Some(PHASE_DRAINING),
            "{mode:?}: the in-flight edge leaves the node draining"
        );
    }

    assert_eq!(
        phase_after(
            &DrainTransition::Move {
                new: Some(PHASE_DRAINING),
                set_started_at: true,
                clear_started_at: false,
            },
            None
        ),
        Some(PHASE_DRAINING)
    );
    assert_eq!(
        phase_after(
            &DrainTransition::Move {
                new: None,
                set_started_at: false,
                clear_started_at: true,
            },
            Some(PHASE_DRAINING)
        ),
        None,
        ":undrain leaves no phase"
    );

    // Cordon-only paths echo whatever the row held.
    for phase in [PHASE_DRAINING, PHASE_DRAINED, PHASE_TERMINATING] {
        assert_eq!(
            phase_after(&DrainTransition::NoChange, Some(phase)),
            Some(phase)
        );
    }
    assert_eq!(phase_after(&DrainTransition::NoChange, None), None);
    assert_eq!(
        phase_after(&DrainTransition::NoChange, Some("some_newer_phase")),
        None,
        "a phase this build cannot place in a work list must not be echoed as \
         one it can"
    );
}

/// `from_drain_mode` may only appear on an entry whose action moves the drain,
/// and only when there is a drain to have a mode.
///
/// Both guards were mutation-silent while this was inline in the handler, which
/// had no test at the time; `drain_handler_tests` now covers that layer too. The
/// phase guard is the one with teeth: without it every `:drain` that *starts* a
/// drain audits `from_drain_mode: gentle`, reporting a prior mode the row never
/// held.
#[test]
fn the_audited_from_mode_needs_both_a_moving_action_and_a_drain() {
    // A drain in progress, on each action that moves the state machine.
    for action in [
        DrainAction::Drain,
        DrainAction::Terminate,
        DrainAction::Undrain,
    ] {
        assert_eq!(
            audited_mode_in_progress(action, Some(PHASE_DRAINING), NodeDrainMode::Forced),
            Some(NodeDrainMode::Forced),
            "{action:?} moves the drain, so the from-mode is real"
        );
        assert_eq!(
            audited_mode_in_progress(action, None, NodeDrainMode::Gentle),
            None,
            "{action:?} on a node with no drain has no from-mode to report"
        );
    }

    // Actions that cannot move the drain never carry one, whatever the phase.
    for action in [
        DrainAction::Cordon,
        DrainAction::Uncordon,
        DrainAction::ForceUploadLocalSnapshots,
    ] {
        assert_eq!(
            audited_mode_in_progress(action, Some(PHASE_DRAINING), NodeDrainMode::Forced),
            None,
            "{action:?} transitions nothing, so a from-mode would invent one"
        );
    }
}

/// Only `:drain` carries a mode to write, and `:terminate` is the case with
/// teeth.
///
/// `:terminate` is a `Move` with both stamp flags false, so a mode supplied here
/// reaches `COALESCE($6, drain_mode)` and overwrites a forced drain's `'forced'`
/// at the `drained → terminating` edge — leaving a node one step from ASG
/// termination presenting as gentle, with the user-API gates reopened against
/// whatever is still attached to it.
#[test]
fn only_drain_supplies_a_mode_to_write() {
    let forced = DrainRequest {
        mode: NodeDrainMode::Forced,
        ..DrainRequest::default()
    };
    assert_eq!(
        requested_mode_for(DrainAction::Drain, &forced),
        Some(NodeDrainMode::Forced)
    );
    assert_eq!(
        requested_mode_for(DrainAction::Drain, &DrainRequest::default()),
        Some(NodeDrainMode::Gentle),
        "a bodiless :drain still carries a mode — gentle, per the column policy"
    );

    // Every other action arrives with a `DrainRequest` it never sent, so its
    // `mode` field is a default nobody chose.
    for action in [
        DrainAction::Terminate,
        DrainAction::Undrain,
        DrainAction::Cordon,
        DrainAction::Uncordon,
        DrainAction::ForceUploadLocalSnapshots,
    ] {
        assert_eq!(
            requested_mode_for(action, &forced),
            None,
            "{action:?} must not write a mode, whatever the defaulted body says"
        );
    }
}

/// The replacement-intent conflict check runs on the in-flight edge only.
///
/// Running it on the drain-starting edge would refuse fresh drains:
/// `terminate_decrement_capacity` is `NOT NULL` and is never cleared, not even
/// by `:undrain`, so a node drained bodiless and then undrained still carries
/// the previous drain's value — and a re-drain with
/// `--no-replace-after-terminate` would compute a disagreement against a drain
/// that does not exist. A blue-green wave would refuse to start on exactly the
/// nodes a previous wave had touched.
#[test]
fn the_replacement_intent_check_runs_only_for_a_drain_in_flight() {
    for mode in [None, Some(NodeDrainMode::Forced)] {
        assert!(checks_replacement_intent_conflict(
            &DrainTransition::UpdateInFlightDrain(mode)
        ));
    }
    for transition in [
        DrainTransition::Move {
            new: Some(PHASE_DRAINING),
            set_started_at: true,
            clear_started_at: false,
        },
        DrainTransition::Move {
            new: Some(PHASE_TERMINATING),
            set_started_at: false,
            clear_started_at: false,
        },
        DrainTransition::Move {
            new: None,
            set_started_at: false,
            clear_started_at: true,
        },
        DrainTransition::NoChange,
    ] {
        assert!(
            !checks_replacement_intent_conflict(&transition),
            "{transition:?}: no drain in flight to disagree with"
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
pub(super) fn enter_direct_test_process() -> bool {
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
        crate::controllers::substrate::Substrate::from_kubernetes(
            crate::controllers::substrate::kubernetes::tests::substrate_with_store_and_client(
                db.pool.clone(),
                client,
            ),
        ),
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

/// `require_undrained` refuses `:drain` on every drain phase and on nothing
/// else: not an undrained node, not a request without the flag, and not any
/// other action.
#[test]
fn require_undrained_refuses_every_existing_drain_phase() {
    for phase in [PHASE_DRAINING, PHASE_DRAINED, PHASE_TERMINATING] {
        assert!(
            refuses_existing_drain(DrainAction::Drain, true, Some(phase)),
            "{phase}"
        );
        assert!(
            !refuses_existing_drain(DrainAction::Drain, false, Some(phase)),
            "{phase}: the flag is opt-in"
        );
        assert!(
            !refuses_existing_drain(DrainAction::Terminate, true, Some(phase)),
            "{phase}: only :drain carries the precondition"
        );
    }
    assert!(!refuses_existing_drain(DrainAction::Drain, true, None));
}

#[tokio::test]
async fn node_inspection_reports_execution_receipt_independently_of_heartbeat() -> TestResult<()> {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let state = AppState::from_pool(db.pool.clone()).await?;
    let actor =
        inventory_test_actor(crate::routes::authz::cluster_managing_org_id(&db.pool).await?);
    let node_id = NodeId::new();
    pplx_space_db_test_fixtures::insert_ready_worker_node(&db.pool, node_id).await?;
    for collection in [None, Some("complete"), Some("unavailable"), Some("legacy")] {
        sqlx::query(sql!(
            "INSERT INTO node_status_snapshots (
                node_id, observed_at, stale_after, reachable, services, updated_at,
                execution_inventory_status, execution_inventory_received_at
             ) VALUES ($1, NOW(), NOW() + INTERVAL '30 seconds', TRUE, '[]', NOW(),
                       $2, CASE WHEN $2::TEXT IS NOT NULL THEN to_timestamp(0) ELSE NULL END)
             ON CONFLICT (node_id) DO UPDATE SET
                observed_at = EXCLUDED.observed_at,
                execution_inventory_status = EXCLUDED.execution_inventory_status,
                execution_inventory_received_at = EXCLUDED.execution_inventory_received_at"
        ))
        .bind(node_id)
        .bind(collection)
        .execute(&db.pool)
        .await?;
        let response = get_admin_node(
            Query(GetAdminNodeFilters {
                include_occupancy: Some(false),
            }),
            State(state.clone()),
            Extension(AuthUser(actor.clone())),
            Path(node_id.to_string()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: AdminNodeResponse =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 100_000).await?)?;
        let status = body.status.unwrap();
        assert!(!status.stale);
        match (collection, status.execution_inventory) {
            (None, None) => {}
            (Some(expected), Some(observation)) => {
                assert_eq!(observation.status, expected);
                assert_eq!(
                    OffsetDateTime::parse(
                        &observation.received_at,
                        &time::format_description::well_known::Rfc3339
                    )?,
                    OffsetDateTime::UNIX_EPOCH,
                );
                assert_ne!(observation.received_at, status.observed_at);
            }
            unexpected => panic!("unexpected execution receipt: {unexpected:?}"),
        }
    }
    Ok(())
}

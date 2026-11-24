use super::*;

#[test]
fn node_drain_phase_round_trips_and_rejects_unknown_values() {
    for phase in [
        NodeDrainPhase::Draining,
        NodeDrainPhase::Drained,
        NodeDrainPhase::Terminating,
    ] {
        assert_eq!(
            NodeDrainPhase::parse(phase.as_str())
                .unwrap_or_else(|error| panic!("{phase}: {error}")),
            phase
        );
        assert_eq!(phase.to_string(), phase.as_str());
        assert_eq!(String::from(phase), phase.as_str());
    }
    // The `nodes.drain_phase` CHECK allows exactly these three, so anything
    // else is a bug in the writer, not a value to classify defensively.
    for junk in ["", "Draining", "deleted", "registered", "future-phase"] {
        assert!(
            NodeDrainPhase::parse(junk).is_err(),
            "{junk} must not parse"
        );
    }
}

#[test]
fn node_drain_mode_round_trips_and_rejects_unknown_values() {
    for mode in [NodeDrainMode::Forced, NodeDrainMode::Gentle] {
        assert_eq!(
            NodeDrainMode::parse(mode.as_str()).unwrap_or_else(|error| panic!("{mode}: {error}")),
            mode
        );
        assert_eq!(mode.to_string(), mode.as_str());
        assert_eq!(String::from(mode), mode.as_str());
    }
    for junk in ["", "Gentle", "gentel", "soft", "none"] {
        assert!(NodeDrainMode::parse(junk).is_err(), "{junk} must not parse");
    }
}

#[test]
fn absent_drain_mode_column_reads_as_gentle() {
    // A missing operator choice must not resolve to the destructive mode. NULL
    // covers a node with no drain in progress and a drain that started before
    // the column existed; the latter stops being evacuated and drains slowly
    // instead, which is the accepted trade for never evacuating a node's
    // sandboxes without someone having asked.
    assert_eq!(NodeDrainMode::from_column_str(None), NodeDrainMode::Gentle);
    assert_eq!(
        NodeDrainMode::from_column_str(Some("gentle")),
        NodeDrainMode::Gentle
    );
    assert_eq!(
        NodeDrainMode::from_column_str(Some("forced")),
        NodeDrainMode::Forced
    );
}

#[test]
fn unrecognized_drain_mode_column_reads_as_gentle() {
    // `nodes.drain_mode` carries no CHECK constraint — the closed set lives
    // here, in application code — so Postgres will happily store a typo like
    // 'gently', and a newer service can write a mode an older reader has never
    // heard of mid-rollout. The column read must degrade to "leave the residents
    // alone" rather than fail the row read (which 500s the drain gate for every
    // sandbox resolved to that node) or, worse, fall through to evacuating them.
    // This is the check that replaces the constraint, so it is load-bearing.
    for junk in ["", " ", "Gentle", "gentel", "soft", "forced ", "FORCED"] {
        assert_eq!(
            NodeDrainMode::from_column_str(Some(junk)),
            NodeDrainMode::Gentle,
            "column value {junk:?} must read as gentle"
        );
    }
}

#[test]
fn drain_mode_parsing_stays_strict_at_the_request_boundary() {
    // The leniency above is scoped to the column. An operator typo in a
    // `:drain` body must surface as a 400 instead of silently becoming a
    // gentle drain — a rollout script that thinks it asked for `forced` and
    // got gentle would cordon its nodes and wait forever while reporting
    // success.
    for junk in ["", "Gentle", "gentel", "soft", "none"] {
        assert!(NodeDrainMode::parse(junk).is_err(), "{junk} must not parse");
        junk.parse::<NodeDrainMode>().unwrap_err();
        NodeDrainMode::try_from(junk.to_string()).unwrap_err();
    }
}

#[test]
fn drain_mode_default_is_gentle() {
    // Both places a mode can be missing — a `:drain` body without one and a
    // NULL column — resolve to the same value, so a single `Default` now says
    // it once instead of two hand-written fallbacks that could drift apart.
    assert_eq!(NodeDrainMode::default(), NodeDrainMode::Gentle);
    assert_eq!(
        NodeDrainMode::from_column_str(None),
        NodeDrainMode::default()
    );
}

#[test]
fn only_forced_mode_evacuates_and_blocks_user_calls() {
    assert!(NodeDrainMode::Forced.evacuates_residents());
    assert!(NodeDrainMode::Forced.blocks_user_calls_while_draining());
    assert!(!NodeDrainMode::Gentle.evacuates_residents());
    assert!(!NodeDrainMode::Gentle.blocks_user_calls_while_draining());
}

/// Every column combination a reader can meet, read once: the drain it is,
/// and what it refuses.
#[test]
fn node_drain_reads_every_column_combination() {
    use NodeDrainPhase::{Drained, Draining, Terminating};
    let v1 = |phase, mode| Some(NodeDrain::V1 { phase, mode });
    let v2 = |phase| Some(NodeDrain::V2 { phase });
    // (engine, phase, mode) and the drain they read as.
    type Case<'a> = (
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a str>,
        Option<NodeDrain>,
    );
    let cases: &[Case<'_>] = &[
        (None, None, None, None),
        (None, None, Some("forced"), None),
        (
            None,
            Some("draining"),
            None,
            v1(Draining, NodeDrainMode::Gentle),
        ),
        (
            None,
            Some("draining"),
            Some("forced"),
            v1(Draining, NodeDrainMode::Forced),
        ),
        (
            None,
            Some("draining"),
            Some("bogus"),
            v1(Draining, NodeDrainMode::Gentle),
        ),
        (
            None,
            Some("drained"),
            None,
            v1(Drained, NodeDrainMode::Gentle),
        ),
        (
            None,
            Some("terminating"),
            Some("forced"),
            v1(Terminating, NodeDrainMode::Forced),
        ),
        (Some("v2"), None, None, v2(V2DrainPhase::Cancelling)),
        (
            Some("v2"),
            Some("draining"),
            None,
            v2(V2DrainPhase::Draining),
        ),
        // A v2 drain takes no mode: one in the column changes nothing.
        (
            Some("v2"),
            Some("draining"),
            Some("forced"),
            v2(V2DrainPhase::Draining),
        ),
        (Some("v2"), Some("drained"), None, v2(V2DrainPhase::Drained)),
        (
            Some("v2"),
            Some("terminating"),
            None,
            v2(V2DrainPhase::Terminating),
        ),
        (Some("v3"), None, None, Some(NodeDrain::Unknown)),
        (Some("v3"), Some("draining"), None, Some(NodeDrain::Unknown)),
        (Some("V2"), Some("draining"), None, Some(NodeDrain::Unknown)),
        (None, Some("paused"), None, Some(NodeDrain::Unknown)),
        (Some("v2"), Some("paused"), None, Some(NodeDrain::Unknown)),
    ];
    for (engine, phase, mode, expected) in cases {
        assert_eq!(
            NodeDrain::from_columns(*engine, *phase, *mode),
            *expected,
            "engine {engine:?}, phase {phase:?}, mode {mode:?}"
        );
    }
}

/// What each drain refuses: a v1 drain as it always did, every v2 phase
/// every activation while only `drained` and `terminating` block running
/// sandboxes' calls, and an unknown drain everything.
#[test]
fn node_drain_refuses_by_engine_and_phase() {
    use NodeDrainPhase::{Drained, Draining, Terminating};
    let cases = [
        (
            NodeDrain::V1 {
                phase: Draining,
                mode: NodeDrainMode::Gentle,
            },
            false,
            false,
        ),
        (
            NodeDrain::V1 {
                phase: Draining,
                mode: NodeDrainMode::Forced,
            },
            true,
            true,
        ),
        (
            NodeDrain::V1 {
                phase: Drained,
                mode: NodeDrainMode::Gentle,
            },
            true,
            true,
        ),
        (
            NodeDrain::V1 {
                phase: Terminating,
                mode: NodeDrainMode::Gentle,
            },
            true,
            true,
        ),
        (
            NodeDrain::V2 {
                phase: V2DrainPhase::Draining,
            },
            false,
            true,
        ),
        (
            NodeDrain::V2 {
                phase: V2DrainPhase::Cancelling,
            },
            false,
            true,
        ),
        (
            NodeDrain::V2 {
                phase: V2DrainPhase::Drained,
            },
            true,
            true,
        ),
        (
            NodeDrain::V2 {
                phase: V2DrainPhase::Terminating,
            },
            true,
            true,
        ),
        (NodeDrain::Unknown, true, true),
    ];
    for (drain, blocks, refuses) in cases {
        assert_eq!(
            drain.blocks_placement(),
            blocks,
            "{drain:?} blocks_placement"
        );
        assert_eq!(
            drain.refuses_activations(),
            refuses,
            "{drain:?} refuses_activations"
        );
    }
}

#[test]
fn node_drain_names_its_state_in_refusals() {
    assert_eq!(
        NodeDrain::V1 {
            phase: NodeDrainPhase::Drained,
            mode: NodeDrainMode::Gentle
        }
        .to_string(),
        "drained"
    );
    assert_eq!(
        NodeDrain::V2 {
            phase: V2DrainPhase::Draining
        }
        .to_string(),
        "draining"
    );
    assert_eq!(
        NodeDrain::V2 {
            phase: V2DrainPhase::Cancelling
        }
        .to_string(),
        "being undrained"
    );
    assert_eq!(
        NodeDrain::Unknown.to_string(),
        "in a drain state this build doesn't know"
    );
}

#[cfg(feature = "serde")]
mod drain_request {
    use super::*;

    /// A `:drain` body that expressed no replacement intent must omit the key
    /// entirely, not send an explicit `null`.
    ///
    /// `space-admin-cli` serializes `DrainRequest` whole and its binaries roll
    /// independently of the api-server, so a new CLI routinely talks to a
    /// previous-release server where this field is a bare `bool`. serde's `default`
    /// fires only on an absent key — an explicit `null` deserializes as
    /// `invalid type: null, expected a boolean`, which would 400 every bodiless
    /// drain against that server. Asserted on the serialized bytes because that is
    /// the contract the old server reads; the `Option` alone does not imply it.
    #[test]
    fn an_unexpressed_replace_after_terminate_is_absent_from_the_wire() {
        let body = serde_json::to_value(DrainRequest::default()).expect("serializes");
        let object = body.as_object().expect("object");
        assert!(
            !object.contains_key("replace_after_terminate"),
            "an unexpressed intent must omit the key, not send null; got {body}"
        );

        // Both expressed values still reach the wire — a `skip_serializing_if` that
        // matched too broadly would silently drop the flag a blue-green shrink
        // depends on. Serialization only; the deserialize direction is pinned by
        // `routes::admin::nodes::tests`' `parse_drain_request` cases.
        for expressed in [true, false] {
            let body = serde_json::to_value(DrainRequest {
                replace_after_terminate: Some(expressed),
                ..DrainRequest::default()
            })
            .expect("serializes");
            assert_eq!(
                body.get("replace_after_terminate"),
                Some(&serde_json::Value::Bool(expressed)),
                "an expressed {expressed} must reach the wire"
            );
        }
    }

    /// The default resolution used on the edge that starts a drain: an unexpressed
    /// intent means the ASG auto-replaces the terminated instance.
    ///
    /// Not entailed by `replace_after_terminate == None` — that pins the parse, this
    /// pins the resolution. Flipping `unwrap_or(true)` to `unwrap_or(false)` leaves
    /// the parse assertion green and drops `terminate_decrement_capacity` to the
    /// wrong value on every fresh drain.
    #[test]
    fn an_unexpressed_replace_after_terminate_resolves_to_auto_replace() {
        assert!(DrainRequest::default().replace_after_terminate_or_default());
        assert!(
            DrainRequest {
                replace_after_terminate: Some(true),
                ..DrainRequest::default()
            }
            .replace_after_terminate_or_default()
        );
        assert!(
            !DrainRequest {
                replace_after_terminate: Some(false),
                ..DrainRequest::default()
            }
            .replace_after_terminate_or_default()
        );
    }

    /// `require_undrained` is absent from the wire unless it's set, so a request
    /// that doesn't ask for the start-only precondition stays byte-identical to
    /// what a previous-release api-server accepts (`DrainRequest` is
    /// `deny_unknown_fields` there). A set flag reaches the wire and parses back.
    #[test]
    fn require_undrained_is_on_the_wire_only_when_set() {
        let body = serde_json::to_value(DrainRequest::default()).expect("serializes");
        assert!(
            !body
                .as_object()
                .expect("object")
                .contains_key("require_undrained"),
            "an unset flag must omit the key; got {body}"
        );

        let set = DrainRequest {
            require_undrained: true,
            ..DrainRequest::default()
        };
        let body = serde_json::to_value(&set).expect("serializes");
        assert_eq!(
            body.get("require_undrained"),
            Some(&serde_json::Value::Bool(true))
        );
        let parsed: DrainRequest = serde_json::from_value(body).expect("parses back");
        assert!(parsed.require_undrained);
    }
}

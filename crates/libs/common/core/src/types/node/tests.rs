use super::*;

#[test]
fn node_pool_name_accepts_label_values_and_rejects_junk() {
    for value in ["isolated-test-pool", "pool_1", "a", "A.B-c_9", "0"] {
        let parsed = NodePoolName::parse(value).expect(value);
        assert_eq!(parsed.as_str(), value);
        assert_eq!(parsed.to_string(), value);
    }
    // Empty is the one rule NodePoolName adds on top of the shared label
    // grammar (an empty label value is legal Kubernetes but pins nothing).
    assert_eq!(NodePoolName::parse(""), Err(NodePoolNameParseError::Empty));
    // Grammar failures (edges, charset, length) are delegated to the shared
    // LabelValue validator, not re-implemented here.
    for bad in ["-lead", "trail-", "has space", &"a".repeat(64)] {
        assert!(
            matches!(
                NodePoolName::parse(bad),
                Err(NodePoolNameParseError::InvalidLabelValue(_))
            ),
            "{bad:?} must be rejected by the label grammar"
        );
    }
}

#[test]
fn node_roles_parse_canonical_storage_values() {
    assert_eq!(NodeRole::parse("worker").unwrap(), NodeRole::Worker);
    assert_eq!(NodeRole::parse("builder").unwrap(), NodeRole::Builder);
    assert_eq!(NodeRole::parse("cluster").unwrap(), NodeRole::Cluster);
    NodeRole::parse("api").unwrap_err();
}

#[test]
fn node_managed_by_parse_canonical_storage_values() {
    assert_eq!(NodeManagedBy::parse("space").unwrap(), NodeManagedBy::Space);
    assert_eq!(
        NodeManagedBy::parse("k8s").unwrap(),
        NodeManagedBy::Kubernetes
    );
    NodeManagedBy::parse("manual").unwrap_err();
}

#[test]
fn node_address_parses_ip_addresses_only() {
    assert_eq!(
        "10.0.0.1".parse::<NodeAddress>().unwrap().to_string(),
        "10.0.0.1"
    );
    "worker.local".parse::<NodeAddress>().unwrap_err();
}

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

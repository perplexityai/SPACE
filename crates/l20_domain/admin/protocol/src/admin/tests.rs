use super::*;
use std::str::FromStr;

#[test]
fn admin_node_sort_round_trips_through_its_wire_string() {
    for sort in AdminNodeSort::ALL.iter().copied() {
        assert_eq!(AdminNodeSort::from_str(sort.as_str()), Ok(sort));
        assert_eq!(sort.to_string(), sort.as_str());
        // The derived serde form and `as_str` are two spellings of the same
        // wire value, so they have to agree — a client that sends the enum
        // inside a JSON body and one that sends it as a query parameter are
        // naming the same ordering.
        assert_eq!(
            serde_json::to_value(sort).unwrap(),
            serde_json::Value::String(sort.as_str().to_string())
        );
    }
}

#[test]
fn admin_node_sort_rejects_unknown_values_and_names_the_accepted_set() {
    let err = AdminNodeSort::from_str("Drainability").unwrap_err();
    assert_eq!(err.value, "Drainability");
    let message = err.to_string();
    for sort in AdminNodeSort::ALL.iter().copied() {
        assert!(
            message.contains(sort.as_str()),
            "error should name {sort}: {message}"
        );
    }
}

#[test]
fn only_the_drainability_ordering_needs_the_counters() {
    assert!(!AdminNodeSort::Name.needs_drainability());
    assert!(AdminNodeSort::Drainability.needs_drainability());
    assert_eq!(AdminNodeSort::default(), AdminNodeSort::Name);
}

/// An all-zero block serializes every field rather than eliding the zeros, so
/// a client that receives a block can read all seven of them. That an *absent*
/// block is omitted from the enclosing response — the distinction that keeps a
/// zero-resident node from being confused with an unranked one — is asserted
/// where the response is built, in the api crate's
/// `drainability_is_absent_from_the_response_until_it_is_computed`. It cannot
/// be asserted here: `Option::<T>::None` serializes to `null` for every `T`
/// regardless of this type's attributes, so a test written against the bare
/// `Option` would pass with `skip_serializing_if` deleted.
#[test]
fn a_computed_drainability_block_serializes_every_term() {
    let json = serde_json::to_value(AdminNodeDrainability::default()).unwrap();
    assert_eq!(json["live_residents"], 0);
    assert_eq!(json["live_long_runners"], 0);
    assert_eq!(json["local_snapshots"], 0);
    assert_eq!(json["terminate_blockers"], 0);
    assert_eq!(json["drain_blockers"], 0);
    assert_eq!(json["heartbeat_fresh"], false);
    assert_eq!(json["long_runner_age_hours"], 0);
    assert_eq!(
        json.as_object().expect("block is an object").len(),
        7,
        "every field is serialized, and a new one needs an assertion here: {json}"
    );
}

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

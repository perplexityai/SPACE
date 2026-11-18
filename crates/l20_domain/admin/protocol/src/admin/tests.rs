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

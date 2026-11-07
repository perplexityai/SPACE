use super::*;

#[test]
fn every_stored_value_round_trips_through_as_str() {
    for &lifecycle in StoredSandboxLifecycle::all() {
        assert_eq!(lifecycle.as_str().parse(), Ok(lifecycle));
    }
}

#[test]
fn every_stored_value_parses_as_stored_and_padded_in_upper_case() {
    for &lifecycle in StoredSandboxLifecycle::all() {
        let stored = lifecycle.as_str();
        let padded = format!(" {} ", stored.to_ascii_uppercase());
        assert_eq!(stored.parse(), Ok(lifecycle), "{stored:?}");
        assert_eq!(padded.parse(), Ok(lifecycle), "{padded:?}");
    }
}

#[test]
fn parse_normalizes_whitespace_and_case() {
    assert_eq!(
        "  Suspended \t".parse(),
        Ok(StoredSandboxLifecycle::Live(SandboxLifecycle::Suspended))
    );
    assert_eq!(" DELETED ".parse(), Ok(StoredSandboxLifecycle::Deleted));
}

#[test]
fn parse_rejects_garbage_values() {
    "".parse::<StoredSandboxLifecycle>().unwrap_err();
    "not-a-lifecycle"
        .parse::<StoredSandboxLifecycle>()
        .unwrap_err();
    for garbage in ["delete", "cleanup-required", "live"] {
        garbage.parse::<StoredSandboxLifecycle>().unwrap_err();
    }
}

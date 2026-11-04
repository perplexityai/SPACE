use super::*;

#[test]
fn every_stored_value_round_trips_through_as_str() {
    for &lifecycle in StoredSandboxLifecycle::all() {
        assert_eq!(lifecycle.as_str().parse(), Ok(lifecycle));
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
}

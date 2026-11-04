use super::*;

fn callsite(level: &str, count: u64, target: &str) -> GuestDiagnosticCallsite {
    GuestDiagnosticCallsite {
        level: level.to_string(),
        target: target.to_string(),
        file: "src/lib.rs".to_string(),
        line: 7,
        count,
    }
}

/// A guest's answer is bounded before any of it is acted on: a fixed number of
/// entries is read, at most a few logged, each count clamped and each string
/// truncated, and every entry past the bound is only counted.
#[test]
fn guest_drain_is_bounded_whatever_the_guest_returns() {
    let long_target = format!("\u{1b}[2J{}", "t".repeat(4 * MAX_FIELD_BYTES));
    let mut diagnostics = vec![callsite("error", u64::MAX, &long_target)];
    diagnostics.extend((1..READ_PER_DRAIN).map(|_| callsite("warn", u64::MAX, "guest")));
    diagnostics.extend((0..10_000).map(|_| callsite("error", 1, "")));
    let response = PingResponse {
        diagnostics,
        diagnostics_dropped: u64::MAX,
    };

    let drain = summarize(&response);

    assert_eq!(drain.logged.len(), LOGGED_PER_DRAIN);
    assert_eq!(drain.logged[0].level, VmDiagnosticLevel::Error);
    assert_eq!(drain.logged[0].count, MAX_COUNT_PER_ENTRY);
    assert!(drain.logged[0].target.len() <= MAX_FIELD_BYTES + 2);
    assert!(!drain.logged[0].target.chars().any(char::is_control));
    let unlogged = u64::try_from(READ_PER_DRAIN - LOGGED_PER_DRAIN).unwrap();
    assert_eq!(drain.rate_limited_warn, unlogged * MAX_COUNT_PER_ENTRY);
    assert_eq!(
        drain.rate_limited_error, 0,
        "entries past the read bound are not visited"
    );
    assert_eq!(drain.dropped, MAX_COUNT_PER_ENTRY + 10_000);
}

/// A restored guest's memory carries the record of the sandbox the snapshot
/// came from, and a restore that ends paused sends no snapshot-restore notice,
/// only a later plain resume. So the host drops the first drain after any
/// restore: restore, resume, drain forwards none of the parent's callsites,
/// and the next drain is forwarded as usual.
#[test]
fn first_drain_after_a_snapshot_restore_is_dropped() {
    let restored = RestoredGuests::default();
    let parent = PingResponse {
        diagnostics: vec![callsite("warn", 3, "parent")],
        diagnostics_dropped: 0,
    };
    let own = PingResponse {
        diagnostics: vec![callsite("warn", 1, "own")],
        diagnostics_dropped: 0,
    };

    restored.mark("sandbox");
    assert!(admitted_drain("sandbox", &parent, &restored).is_none());
    let forwarded = admitted_drain("sandbox", &own, &restored).expect("later drains forward");
    assert_eq!(forwarded.logged[0].target, "own");
    assert!(
        admitted_drain("other", &parent, &restored).is_some(),
        "a sandbox that was not restored forwards its first drain"
    );
}

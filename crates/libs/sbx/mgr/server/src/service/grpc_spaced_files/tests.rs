use super::*;

#[test]
fn cpu_stat_usage_usec_parses_the_usage_line() {
    let content = "usage_usec 41398047\nuser_usec 40000000\nsystem_usec 1398047\n";
    assert_eq!(cpu_stat_usage_usec(content), Some(41_398_047));
    assert_eq!(cpu_stat_usage_usec("user_usec 1\n"), None);
}

#[test]
fn inspect_warnings_are_rate_limited_per_sandbox() {
    let warnings = PerSandboxWarnings::new(Duration::from_secs(60));
    let start = Instant::now();
    assert!(warnings.admit("a", start));
    assert!(!warnings.admit("a", start + Duration::from_secs(59)));
    // Another sandbox is not held back by the first one's warning.
    assert!(warnings.admit("b", start + Duration::from_secs(1)));
    assert!(warnings.admit("a", start + Duration::from_secs(60)));
    // Admitting a warning drops every expired entry.
    assert!(warnings.admit("c", start + Duration::from_secs(120)));
    assert_eq!(warnings.last_warned.lock().unwrap().len(), 1);
}

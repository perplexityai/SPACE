use std::ffi::OsString;
use std::time::Duration;

use super::{watchdog_interval, watchdog_pid_matches_current};

#[test]
fn watchdog_interval_uses_half_of_systemd_budget() {
    assert_eq!(watchdog_interval("30000000"), Some(Duration::from_secs(15)));
    assert_eq!(watchdog_interval("1"), Some(Duration::from_micros(1)));
}

#[test]
fn watchdog_interval_rejects_invalid_values() {
    assert_eq!(watchdog_interval("0"), None);
    assert_eq!(watchdog_interval("not-a-number"), None);
}

#[test]
fn watchdog_pid_filter_accepts_current_or_missing_pid() {
    assert!(watchdog_pid_matches_current(None));
    let current = OsString::from(std::process::id().to_string());
    assert!(watchdog_pid_matches_current(Some(current.as_os_str())));
}

#[test]
fn watchdog_pid_filter_rejects_other_or_invalid_pid() {
    let other = OsString::from(std::process::id().wrapping_add(1).max(1).to_string());
    assert!(!watchdog_pid_matches_current(Some(other.as_os_str())));
    assert!(!watchdog_pid_matches_current(Some(
        OsString::from("nope").as_os_str()
    )));
}

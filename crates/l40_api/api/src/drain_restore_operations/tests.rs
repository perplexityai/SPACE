use super::DrainRestoreRetry;

mod executor;
mod supersession;

/// The backoff doubles from its base to its cap.
#[test]
fn failed_attempts_back_off_to_the_cap() {
    let retry = DrainRestoreRetry::with_max_attempts(16);
    let waits = (1..=7)
        .map(|attempt| retry.backoff(attempt).as_secs())
        .collect::<Vec<_>>();
    assert_eq!(waits, [60, 120, 240, 480, 960, 1800, 1800]);
}

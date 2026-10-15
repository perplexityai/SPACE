use super::*;

use VisibilityLookupDegradation::{ColumnMissing, QueryFailed, TemplateMissing, UnrecognizedValue};

#[test]
fn idle_replica_reports_explicit_zero_for_every_alertable_reason() {
    let started = Instant::now();
    let observations = Observations::starting_at(started);
    assert_eq!(
        observations.trailing_counts(started),
        [(ColumnMissing, 0), (QueryFailed, 0), (UnrecognizedValue, 0)]
    );
}

#[test]
fn template_missing_is_not_counted() {
    let started = Instant::now();
    let observations = Observations::starting_at(started);
    observations.record_at(TemplateMissing, started);
    assert!(
        observations
            .trailing_counts(started)
            .iter()
            .all(|(_, count)| *count == 0)
    );
}

#[test]
fn counts_accumulate_per_reason_across_buckets() {
    let started = Instant::now();
    let observations = Observations::starting_at(started);
    observations.record_at(QueryFailed, started);
    observations.record_at(QueryFailed, started + Duration::from_secs(1));
    observations.record_at(QueryFailed, started + Duration::from_mins(5));
    observations.record_at(UnrecognizedValue, started + Duration::from_mins(10));
    assert_eq!(
        observations.trailing_counts(started + Duration::from_mins(10)),
        [(ColumnMissing, 0), (QueryFailed, 3), (UnrecognizedValue, 1)]
    );
}

#[test]
fn events_leave_the_window_after_an_hour() {
    let started = Instant::now();
    let observations = Observations::starting_at(started);
    let event_late_in_minute = started + Duration::from_secs(59);
    observations.record_at(ColumnMissing, started);
    observations.record_at(ColumnMissing, event_late_in_minute);
    observations.record_at(ColumnMissing, started + Duration::from_mins(30));

    // An event late in its minute is never dropped before it is an hour old:
    // the first bucket survives until the whole bucket has left the window.
    assert_eq!(
        observations.trailing_counts(event_late_in_minute + WINDOW - Duration::from_secs(1))[0],
        (ColumnMissing, 3)
    );
    assert_eq!(
        observations.trailing_counts(started + WINDOW + BUCKET)[0],
        (ColumnMissing, 1)
    );
    assert_eq!(
        observations.trailing_counts(started + WINDOW + Duration::from_mins(31))[0],
        (ColumnMissing, 0)
    );
}

#[test]
fn a_late_recorder_counts_in_the_newest_bucket() {
    // A handler that read the clock before a minute boundary but took the lock
    // after a newer bucket exists must not push an older bucket behind it.
    let started = Instant::now();
    let observations = Observations::starting_at(started);
    observations.record_at(QueryFailed, started + Duration::from_secs(61));
    observations.record_at(QueryFailed, started + Duration::from_secs(59));
    let buckets = observations.buckets.lock().unwrap();
    let (_, query_failed) = buckets
        .iter()
        .find(|(reason, _)| *reason == QueryFailed)
        .unwrap();
    assert_eq!(query_failed.iter().copied().collect::<Vec<_>>(), [(1, 2)]);
}

#[test]
fn memory_is_bounded_by_bucket_count_under_sustained_failure() {
    let started = Instant::now();
    let observations = Observations::starting_at(started);
    for second in 0..(3 * WINDOW.as_secs()) {
        observations.record_at(QueryFailed, started + Duration::from_secs(second));
    }
    let buckets = observations.buckets.lock().unwrap();
    let retained = usize::try_from(RETAINED_BUCKETS).unwrap();
    let (_, query_failed) = buckets
        .iter()
        .find(|(reason, _)| *reason == QueryFailed)
        .unwrap();
    // One bucket per minute of the window plus the current one: bounded, and
    // not pruned early.
    assert_eq!(query_failed.len(), retained + 1);
}

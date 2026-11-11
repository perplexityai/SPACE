use super::deleted_partition_suppress_threshold;

#[test]
fn absolute_floor_dominates_small_requests() {
    // A small request can never trip suppression via the fraction:
    // a node asking about 40 ids that are all deleted is a plausible
    // devbox, not a mass-deletion event.
    assert_eq!(deleted_partition_suppress_threshold(0), 500);
    assert_eq!(deleted_partition_suppress_threshold(40), 500);
    assert_eq!(deleted_partition_suppress_threshold(1000), 500);
}

#[test]
fn suppression_is_inclusive_at_the_ceiling() {
    // The contract says "at or beyond" the ceiling is implausible:
    // exactly the ceiling (e.g. 850 of 1700, or exactly 500) must
    // suppress, not squeak through on the boundary.
    assert!(super::deleted_partition_should_suppress(850, 1700));
    assert!(super::deleted_partition_should_suppress(500, 40));
    assert!(!super::deleted_partition_should_suppress(849, 1700));
    assert!(!super::deleted_partition_should_suppress(499, 40));
}

#[test]
fn fraction_dominates_worker_scale_requests() {
    // Once requests are large enough, the half-request threshold must dominate
    // the fixed floor.
    assert_eq!(deleted_partition_suppress_threshold(1700), 850);
    assert_eq!(deleted_partition_suppress_threshold(4000), 2000);
}

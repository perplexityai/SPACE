use super::*;

#[test]
fn operation_snapshots_are_internal() {
    assert!(is_internal_snapshot_name("d-stop-current"));
    assert!(
        is_internal_snapshot_name("d-operation-019f9ce3-2370-7a33-8706-fda0d0496d33"),
        "operation snapshots must not be exposed to list, delete, or rollback"
    );
    assert!(!is_internal_snapshot_name("d-manual-20260804T180000Z"));
    // GC staging in either shape, the fixed-length one included: a leading
    // `.gc-pending-` still contains the marker.
    for staging in [
        ".gc-pending-019f9ce3-2370-7a33-8706-fda0d0496d33",
        "d-manual-20260804T180000Z.gc-pending-019f9ce3-2370-7a33-8706-fda0d0496d33",
    ] {
        assert!(is_internal_snapshot_name(staging), "{staging}");
    }
}

#[test]
fn disk_snapshot_rollback_delegates_generation_minting() {
    let request = disk_snapshot_rollback_start_request(
        "019f8000-0000-7000-8000-000000000001".to_string(),
        Some("/var/run/netns/sns-test".to_string()),
    );

    assert_eq!(request.route_identity, None);
}

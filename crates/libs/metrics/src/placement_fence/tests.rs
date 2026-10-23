use super::*;

#[test]
fn termination_contention_deferral_has_no_churning_dimensions() {
    let capture = crate::emit::test_capture::capture_counters_guard();
    termination_contention_deferral();
    let captured = capture.finish();

    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].name,
        "placement_fence.termination_contention_deferrals"
    );
    assert_eq!(captured[0].value, 1);
    assert!(captured[0].tags.is_empty());
}

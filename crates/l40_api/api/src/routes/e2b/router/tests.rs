use super::ecw_logged_path;

#[test]
fn fork_log_path_does_not_retain_caller_controlled_sandbox_segment() {
    assert_eq!(
        ecw_logged_path("/sandboxes/caller-secret/fork"),
        "/sandboxes/{sandboxID}/fork"
    );
}

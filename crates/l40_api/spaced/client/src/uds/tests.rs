use super::*;

#[test]
fn authenticated_channel_allows_only_its_prevalidated_connection() {
    let claimed = std::sync::atomic::AtomicBool::new(false);
    claim_single_connection(&claimed).expect("initial connection is available");
    let error = claim_single_connection(&claimed).expect_err("reconnect must re-attest");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
}

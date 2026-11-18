use std::path::PathBuf;

use crate::linux;

#[test]
fn guest_linux_paths_preserve_guest_socket_layout() {
    assert_eq!(linux::paths::SPACED_GUEST_SOCKET_DIR, "/run");
    assert_eq!(linux::paths::SPACED_GUEST_SOCKET_FILE, "spaced.sock");

    if cfg!(target_os = "linux") {
        assert_eq!(
            linux::paths::default_guest_uds_path("spaced.sock"),
            PathBuf::from("/run/spaced.sock")
        );
    } else {
        assert!(linux::paths::default_guest_uds_path("spaced.sock").ends_with("spaced.sock"));
    }
}

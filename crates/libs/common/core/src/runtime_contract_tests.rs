use std::path::PathBuf;

use crate::guest;

#[test]
fn template_cache_paths_preserve_space_layout() {
    assert_eq!(
        crate::types::template_build_dir(
            "seed",
            "abc",
            crate::types::TemplateCachePartition::Legacy
        ),
        "/space/cache/firecracker/templates/seed/abc"
    );
    let environment =
        crate::types::EnvironmentReference::new(crate::types::Digest::sha256([2; 32]));
    assert_eq!(
        crate::types::template_snapshot_dir(
            "seed",
            "abc",
            crate::types::TemplateCachePartition::Environment(environment),
            2,
            1024
        ),
        format!(
            "/space/cache/firecracker/templates/seed/snapshots/abc@{environment}/2vcpu-1024mib"
        )
    );
}

#[test]
fn guest_linux_paths_preserve_guest_socket_layout() {
    assert_eq!(guest::linux::paths::SPACED_GUEST_SOCKET_DIR, "/run");
    assert_eq!(guest::linux::paths::SPACED_GUEST_SOCKET_FILE, "spaced.sock");

    if cfg!(target_os = "linux") {
        assert_eq!(
            guest::linux::paths::default_guest_uds_path("spaced.sock"),
            PathBuf::from("/run/spaced.sock")
        );
    } else {
        assert!(
            guest::linux::paths::default_guest_uds_path("spaced.sock").ends_with("spaced.sock")
        );
    }
}

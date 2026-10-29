use std::path::PathBuf;

use crate::{guest, host};

#[test]
fn host_linux_paths_preserve_space_layout() {
    assert_eq!(
        host::linux::paths::SANDBOX_MANAGER_SOCKET,
        "/space/run/sandbox-manager/sandbox-manager.sock"
    );
    assert_eq!(
        host::linux::paths::sandbox_run_dir("sbx-1"),
        "/space/run/firecracker/sandboxes/sbx-1"
    );
    assert_eq!(
        host::linux::paths::sandbox_rootfs_path("sbx-1"),
        "/space/cache/firecracker/sandboxes/sbx-1/instance/rootfs.ext4"
    );
    assert_eq!(
        host::linux::paths::sandbox_metadata_path("sbx-1"),
        "/space/cache/firecracker/sandboxes/sbx-1/metadata.json"
    );
    assert_eq!(
        host::linux::paths::template_rootfs_path("seed", "abc"),
        "/space/cache/firecracker/templates/seed/abc/rootfs.ext4"
    );
}

#[test]
fn host_linux_paths_canonicalize_sandbox_uuids() {
    // SandboxSelector::parse strips dashes from UUIDs; CLI commands
    // then send the compact form to sbx-mgr over gRPC. Every per-sandbox
    // path helper must canonicalize so that the path lands at the same
    // location on disk regardless of the wire form the id arrived in.
    let hyphenated = "019e31ec-5633-7111-b27c-1b6ac5cb170a";
    let compact = "019e31ec56337111b27c1b6ac5cb170a";
    let expected_run = "/space/run/firecracker/sandboxes/019e31ec-5633-7111-b27c-1b6ac5cb170a";
    let expected_cache = "/space/cache/firecracker/sandboxes/019e31ec-5633-7111-b27c-1b6ac5cb170a";

    assert_eq!(
        host::linux::paths::sandbox_run_dir(hyphenated),
        expected_run
    );
    assert_eq!(host::linux::paths::sandbox_run_dir(compact), expected_run);

    assert_eq!(
        host::linux::paths::sandbox_cache_dir(hyphenated),
        expected_cache
    );
    assert_eq!(
        host::linux::paths::sandbox_cache_dir(compact),
        expected_cache
    );

    assert_eq!(
        host::linux::paths::sandbox_instance_dir(compact),
        format!("{expected_cache}/instance"),
    );
    assert_eq!(
        host::linux::paths::sandbox_rootfs_path(compact),
        format!("{expected_cache}/instance/rootfs.ext4"),
    );
    assert_eq!(
        host::linux::paths::sandbox_snapshots_dir(compact),
        format!("{expected_cache}/snapshots"),
    );
    assert_eq!(
        host::linux::paths::sandbox_snapshot_dir(compact, "sn"),
        format!("{expected_cache}/snapshots/sn"),
    );
    assert_eq!(
        host::linux::paths::spaced_host_socket(compact),
        format!("{expected_run}/spaced-host.sock"),
    );
    assert_eq!(
        host::linux::paths::net_gateway_control_socket(compact),
        format!("{expected_run}/netgtw-control.sock"),
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

#[test]
fn host_linux_identities_are_generated_from_node_contract() {
    assert_eq!(
        host::linux::identities::group_names::SPACE_NODECLIENTS,
        "space-nodeclients"
    );
}

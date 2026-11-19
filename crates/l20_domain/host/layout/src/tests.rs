use super::paths;

#[test]
fn svc_binary_allowlists_derive_from_the_bin_dir_constants() {
    // The allowlists and the named bin-dir constants expand from one macro;
    // this pins that a future edit to either keeps them in agreement — a
    // diverged entry would deny every credmgr RPC from that peer under
    // enforce.
    for (allowlist, names) in [
        (paths::SPACE_SBXMGR_UID_BINARIES, &["space-sbxmgr-svc"][..]),
        (paths::SPACE_VOLMGR_SVC_BINARIES, &["space-volmgr-svc"][..]),
        (
            paths::SPACE_TMPLMGR_SVC_BINARIES,
            &["space-tmplmgr-svc"][..],
        ),
    ] {
        let expected: Vec<String> = names
            .iter()
            .flat_map(|name| {
                [
                    format!("{}/{name}", paths::SPACE_HELM_BIN_DIR),
                    format!("{}/{name}", paths::SPACE_SYSTEMD_BIN_DIR),
                ]
            })
            .collect();
        assert_eq!(
            allowlist
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn systemd_apps_dir_is_a_sibling_of_the_bin_dir() {
    // The named invariant: digest targets and the symlink dir share a parent
    // (the /space volume), so it survives a coordinated rename but fires if
    // /space/apps moves under a different root.
    assert_eq!(
        std::path::Path::new(paths::SPACE_SYSTEMD_APPS_DIR).parent(),
        std::path::Path::new(paths::SPACE_SYSTEMD_BIN_DIR).parent(),
    );
    // The literals themselves are pinned separately: the digest-form matcher
    // compares against these exact strings in the supported systemd layout.
    assert_eq!(paths::SPACE_SYSTEMD_BIN_DIR, "/space/bin");
    assert_eq!(paths::SPACE_SYSTEMD_APPS_DIR, "/space/apps");
}

#[test]
fn paths_preserve_space_layout() {
    assert_eq!(
        crate::paths::SANDBOX_MANAGER_SOCKET,
        "/space/run/sandbox-manager/sandbox-manager.sock"
    );
    assert_eq!(
        crate::paths::sandbox_run_dir("sbx-1"),
        "/space/run/firecracker/sandboxes/sbx-1"
    );
    assert_eq!(
        crate::paths::sandbox_rootfs_path("sbx-1"),
        "/space/cache/firecracker/sandboxes/sbx-1/instance/rootfs.ext4"
    );
    assert_eq!(
        crate::paths::sandbox_metadata_path("sbx-1"),
        "/space/cache/firecracker/sandboxes/sbx-1/metadata.json"
    );
}

#[test]
fn paths_canonicalize_sandbox_uuids() {
    // SandboxSelector::parse strips dashes from UUIDs; CLI commands
    // then send the compact form to sbx-mgr over gRPC. Every per-sandbox
    // path helper must canonicalize so that the path lands at the same
    // location on disk regardless of the wire form the id arrived in.
    let hyphenated = "019e31ec-5633-7111-b27c-1b6ac5cb170a";
    let compact = "019e31ec56337111b27c1b6ac5cb170a";
    let expected_run = "/space/run/firecracker/sandboxes/019e31ec-5633-7111-b27c-1b6ac5cb170a";
    let expected_cache = "/space/cache/firecracker/sandboxes/019e31ec-5633-7111-b27c-1b6ac5cb170a";

    assert_eq!(crate::paths::sandbox_run_dir(hyphenated), expected_run);
    assert_eq!(crate::paths::sandbox_run_dir(compact), expected_run);

    assert_eq!(crate::paths::sandbox_cache_dir(hyphenated), expected_cache);
    assert_eq!(crate::paths::sandbox_cache_dir(compact), expected_cache);

    assert_eq!(
        crate::paths::sandbox_instance_dir(compact),
        format!("{expected_cache}/instance"),
    );
    assert_eq!(
        crate::paths::sandbox_rootfs_path(compact),
        format!("{expected_cache}/instance/rootfs.ext4"),
    );
    assert_eq!(
        crate::paths::sandbox_snapshots_dir(compact),
        format!("{expected_cache}/snapshots"),
    );
    assert_eq!(
        crate::paths::sandbox_snapshot_dir(compact, "sn"),
        format!("{expected_cache}/snapshots/sn"),
    );
    assert_eq!(
        crate::paths::sandbox_snapshot_dir_in("/root", compact, "sn"),
        format!("/root/{hyphenated}/snapshots/sn"),
    );
    assert_eq!(
        crate::paths::spaced_host_socket(compact),
        format!("{expected_run}/spaced-host.sock"),
    );
    assert_eq!(
        crate::paths::net_gateway_control_socket(compact),
        format!("{expected_run}/netgtw-control.sock"),
    );
}

#[test]
fn identities_are_generated_from_node_contract() {
    assert_eq!(
        crate::identities::group_names::SPACE_NODECLIENTS,
        "space-nodeclients"
    );
}

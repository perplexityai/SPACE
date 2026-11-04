// Exercise the public connector's real path selection, rather than a helper
// that could be passed a safe path while production still uses the alias.
// Those are fixed node paths, and on a node `/space/run` holds the live
// gateways, so the test runs on a thread whose private mount namespace has an
// empty tmpfs as its root: nothing it creates reaches the host.
#[cfg(target_os = "linux")]
#[test]
fn sandbox_connections_ignore_replaced_alias_and_reject_legacy_only_gateway() {
    if !pplx_space_test_env::root_or_skip("mount a private root over the node's fixed paths") {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            enter_private_root(root.path());
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(connect_through_the_fixed_paths());
        });
    });
}

/// Unsharing the mount namespace also unshares this thread's root directory,
/// so the chroot changes no other thread. Private propagation keeps the tmpfs
/// out of the host's namespace, and it disappears with this thread.
#[cfg(target_os = "linux")]
fn enter_private_root(root: &std::path::Path) {
    use nix::mount::{MsFlags, mount};
    use nix::sched::{CloneFlags, unshare};

    unshare(CloneFlags::CLONE_NEWNS).unwrap();
    mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    )
    .unwrap();
    mount(
        Some("tmpfs"),
        root,
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
        Some("mode=0755"),
    )
    .unwrap();
    nix::unistd::chroot(root).unwrap();
    nix::unistd::chdir("/").unwrap();
}

#[cfg(target_os = "linux")]
async fn connect_through_the_fixed_paths() {
    use pplx_space_common_core::host::linux::paths;
    use std::path::Path;
    use std::time::Duration;
    use tokio::net::UnixListener;

    std::fs::create_dir_all(paths::NET_GATEWAY_RUN_DIR).unwrap();
    let gateway_dir = tempfile::Builder::new()
        .prefix("control-endpoint-test-")
        .tempdir_in(paths::NET_GATEWAY_RUN_DIR)
        .unwrap();
    let sandbox_id = gateway_dir.path().file_name().unwrap().to_str().unwrap();
    let alias = paths::net_gateway_control_socket(sandbox_id);
    let alias_dir = Path::new(&alias).parent().unwrap();
    std::fs::create_dir_all(alias_dir.parent().unwrap()).unwrap();
    std::fs::create_dir(alias_dir).unwrap();
    let canonical = gateway_dir.path().join("control.sock");
    std::os::unix::fs::symlink(&canonical, &alias).unwrap();
    // A sandbox-UID process replaces its own directory entry after preparation.
    std::fs::remove_file(&alias).unwrap();
    let impostor = UnixListener::bind(&alias).unwrap();
    let trusted = UnixListener::bind(&canonical).unwrap();
    let policy = pplx_space_common_grpc::GrpcRetryPolicy {
        max_elapsed: Duration::from_millis(100),
        ..pplx_space_common_grpc::DEFAULT_GRPC_CONNECT_RETRY_POLICY
    };
    let (client, accepted) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            super::connect_with_retry_policy(sandbox_id, policy),
            trusted.accept(),
        )
    })
    .await
    .expect("the protected listener must receive the connection");
    drop(client.expect("connect to protected listener"));
    drop(accepted.unwrap());
    tokio::time::timeout(Duration::ZERO, impostor.accept())
        .await
        .expect_err("the substituted listener must receive no connection");

    // Old shared-UID gateways only have the compatibility path. They must be
    // cycled explicitly; availability must not select the attacker-controlled path.
    drop(trusted);
    std::fs::remove_file(&canonical).unwrap();
    let error = super::connect_with_retry_policy(sandbox_id, policy)
        .await
        .expect_err("never fall back to a legacy or substituted listener");
    assert!(error.to_string().contains("cycle-net-gateway"));
    tokio::time::timeout(Duration::ZERO, impostor.accept())
        .await
        .expect_err("the substituted listener must receive no connection");
    drop(impostor);
}
